# Should pyrmts own an IaC layer? Where the boundary goes

Status: **proposed** (2026-08-28, pyrmts session). Answers the question ctbk raised after `specs/done/d1-shard-index-temporal.md`: *"if pyrmts went this way, the natural resource set is a 'pyramid stack' … `pyrmts_ops` already exists as a package and is the obvious home."* Written after surveying what every relevant repo actually does, because the answer turns on evidence that was scattered across four of them.

**Short answer: no — not as a framework adoption, and the sequencing ctbk proposed is right for a reason other than the one given.** Step 1 (schema) is done and shipped separately (`specs/d1-schema-drift.md`). Steps 2 and 3 should not be "pyrmts adopts Pulumi"; they should be three specific repairs to what pyrmts already ships, listed at the end. The rest of this argues why.

## The evidence that changed the answer

ctbk's case rested on two incidents. Both are real. Neither is fixed by pyrmts adopting an IaC framework:

1. **`ensure_schedule` would have silently re-enabled a disabled rule.** True, and pyrmts shipped the identical bug — `pyrmts_ops.aws.upsert_schedule` hardcoded `State='ENABLED'` too. That is a defect in an upsert primitive, ~15 lines from fixed. Adopting a framework to avoid it would be treating a bug as an architecture problem.
2. **Nothing runs `schemaSql()` as a migration.** True, and explicitly *outside* IaC's scope by the convention this org already follows — `marin-gcs-usage/specs/cf-iac.md`: *"**Not** IaC'd: deployments themselves … and D1 migrations (schema stays with the app)."* Solved in `specs/d1-schema-drift.md` with no framework involved.

And the decisive fact, which neither session had in view: **ctbk already adopted Pulumi.** `ctbk/infra/` is a `pulumi-cloudflare` project with a committed local-file backend and `specs/pulumi-cf-infra.md` behind it. It declares four resources — the R2 bucket (imported, `protect=True`), the D1 database, the queue, one event notification — and has not been touched since **2026-04-12**. In the four months since, ctbk added three Lambdas, three EventBridge rules, two ECR repos, a Batch job definition, three Workers and two custom domains, every one of them imperatively, around the stack. Its `WORKERS` dict is a comment that lists 3 of the 6 workers that now exist. Phase 2 never happened; the `infra.yml` its own spec drafts verbatim was never created.

So the counterfactual is available rather than speculative. ctbk did not lack a tool. It had the tool, the backend, the spec, and the intent, and the imperative scripts still won — because they are what runs in CI on every push, and `pulumi up` was a thing a person had to remember to do from a laptop. A pyrmts-supplied IaC layer would have landed in exactly that gap.

This is also precisely the risk ctbk itself named: *"a half-adopted IaC layer is worse than scripts, because it implies coverage it doesn't have — the drift is the same but now invisible behind an abstraction that looks authoritative."* `ctbk/infra/` is that sentence, already true, in ctbk's own tree.

## Why a library is the wrong layer for this specifically

**The conventions are three-way split, and pyrmts does not get to break the tie.** OA (`~/c/oa/ops`) runs Pulumi/Python against a GCS backend with KMS secrets, flat `create_*_resources()` factories, and a `workflow_dispatch` GHA wrapper. awair — a pyrmts consumer — runs **AWS CDK**, one stack per device. ctbk runs the stalled Pulumi stack plus imperative boto3 plus wrangler. pyrmts' own written position, in `specs/engine-batch-packaging.md`, is *"Plain boto3, no CDK/Terraform."* A library that picks one makes itself un-adoptable by consumers on the others; a library that abstracts over all of them has invented a meta-IaC, which is worse than any of them.

**The resource set is mostly not pyrmts-shaped.** Inventorying ctbk's infrastructure, the resources that exist *because it is a pyramid* are: a fill Lambda (or Batch job), its schedule, a D1 database, an R2 bucket. Everything else — the queue and its event notification, the loader/compactor/poller/api workers, Analytics Engine, custom domains, the r2.dev public URL, six worker secrets — is ctbk being ctbk. awair's per-device Lambda stacks are awair being awair. The genuinely shared surface is small, and pyrmts already has code for most of it (`pyrmts_ops.aws`, `pyrmts_engine.batch`).

**Ownership boundaries would be wrong in both directions.** Half of what a pyramid needs isn't declarable by a library even in principle: ctbk deliberately keeps `ctbk.dev` out of wrangler config because the narrow-scope CI token lacks zone permissions, and its Batch job definition's real content (vCPUs, memory, role ARNs, credentials) exists only in AWS, propagated forward one revision at a time by a read-modify-write. A pyrmts stack would either not cover those — the half-adoption failure — or demand credentials a consumer's CI deliberately does not hold.

## What to do instead

Three repairs to what pyrmts already ships. Each is small, each targets an observed failure, and none requires a consumer to adopt anything.

**1. Make the upsert primitives honest.** *(Done in this pass — `pyrmts_ops.aws`.)* `upsert_schedule` gained `enabled: bool | None = None`, where the default **preserves** a rule's live state instead of forcing `ENABLED`. That is what makes a disable mean something: retiring a pyramid is disabling its tick, and under a forced state the only way to make retirement stick was to delete the calling code — leaving the rule alive in the account, invisible to the deployer, ready to return if the code did. The same pass fixed a latent outage bug: the invoke-permission `StatementId` was a **constant**, so the second rule targeting a function conflicted with the first rule's statement, the `ResourceConflictException` swallow hid it, and the rule fired into a function that rejected it. ctbk lost its `avail-v6` tick to exactly this for a day (2026-08-06) and fixed their fork; pyrmts still shipped it, as does ctbk's legacy `deploy.py`. Ids are now per-rule.

Remaining in this class, not yet done: `upsert_lambda_role` never re-checks an existing role's trust policy or attachments (create-only reconciliation); `put_targets` uses a fixed target id and never prunes extra targets; `pyrmts_engine.batch.bootstrap` is describe-or-create throughout with no delete path.

**2. Derive the desired resource set from the pyramid config, and stop there.** The real gap in `pyrmts_ops.aws` is not that it lacks a state file — it is that `deploy_pyramid_lambda` takes loose strings and **nothing reads the pyramid YAML to say what a pyramid needs**. A pure function — config in, a description of the fill function, its schedule, its env, its bucket and database out — is useful to *every* consumer regardless of tool: ctbk's Pulumi program can declare it, awair's CDK stack can declare it, `pyrmts_ops.aws` can imperatively upsert it, and a `--dry-run` can print it. That is the honest version of "pyrmts owns the pyramid stack": pyrmts owns the *description*, the consumer owns the *application*. It is also the only piece that gets harder to add later, once each consumer has hand-written its own answer.

**3. Give the consumer something that notices.** `verify`-shaped, not `apply`-shaped, matching `specs/d1-schema-drift.md`: read-only checks that a consumer can run in CI or surface from `/health`. Schema is done. The obvious next one is a schedule/function check — does a rule exist per configured pyramid, is it enabled, does its target resolve — which needs only read permissions and would have caught both the v6 permission outage and the four months of `ctbk/infra/` drift. Detection is worth more than reconciliation here and costs an order of magnitude less, because the thing that actually failed in every incident above was **nobody noticed**, not *nobody could express it*.

## If a consumer still wants full IaC (recommendation to ctbk)

Do it in ctbk, not in pyrmts, and finish the stack that already exists rather than starting one: import the live Lambdas, rules, and ECR repos into `ctbk/infra/` so the first `pulumi up` is a no-op diff (OA's stated adoption order — *"that alone gives drift detection"*), move the state off the committed local-file backend, and wire the `infra.yml` that `specs/pulumi-cf-infra.md` already drafts so it runs on push rather than from a laptop. The unrun-for-four-months problem is a CI problem, not a tooling problem, and it will recur identically with any tool that a human has to remember to invoke.

Two ctbk-specific notes from the inventory, worth fixing whether or not the stack advances: the D1 database id is a pasted literal in four `wrangler.toml`s while `infra/` already exports it, and `gbfs/lambda/deploy.py` and `deploy-image.py` are two divergent copies of one resource graph, with the fixed `StatementId` bug still live in the former.

## Non-goals

- pyrmts depending on Pulumi, CDK, Terraform, or any provider SDK. The `boto3`-behind-an-extra, lazily-imported convention stays.
- pyrmts owning consumer resources (queues, custom domains, worker secrets, Analytics Engine datasets, per-device stacks).
- Reconciliation or state tracking inside pyrmts. If a consumer wants a state file, that is what their IaC tool is for.
