# Should pyrmts own an IaC layer? Where the boundary goes

Status: **superseded by implementation** (2026-08-29). Its short answer was overturned by the user, correctly — see **Outcome** at the end for what shipped and which arguments here survived. Originally: **proposed** (2026-08-28, pyrmts session). Answers the question ctbk raised after `specs/done/d1-shard-index-temporal.md`: *"if pyrmts went this way, the natural resource set is a 'pyramid stack' … `pyrmts_ops` already exists as a package and is the obvious home."* Written after surveying what every relevant repo actually does, because the answer turns on evidence that was scattered across four of them.

**Short answer (superseded — see Outcome): no — not as a framework adoption, and the sequencing ctbk proposed is right for a reason other than the one given.** Step 1 (schema) is done and shipped separately (`specs/d1-schema-drift.md`). Steps 2 and 3 should not be "pyrmts adopts Pulumi"; they should be three specific repairs to what pyrmts already ships, listed at the end. The rest of this argues why.

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

**1. Make the upsert primitives honest.** *(Done in this pass — `pyrmts_ops.aws`.)* `upsert_schedule` gained `enabled: bool | None = None`, where the default **preserves** a rule's live state instead of forcing `ENABLED`. That is what makes a disable mean something: retiring a pyramid is disabling its tick, and under a forced state the only way to make retirement stick was to delete the calling code — leaving the rule alive in the account, invisible to the deployer, ready to return if the code did. The same pass fixed a latent outage bug: the invoke-permission `StatementId` was a **constant**, so the second rule targeting a function conflicted with the first rule's statement, the `ResourceConflictException` swallow hid it, and the rule fired into a function that rejected it. ctbk lost its `avail-v6` tick to exactly this and fixed their fork; pyrmts still shipped it. Ids are now per-rule. *(Corrected 2026-08-29: an earlier draft of this spec said the outage lasted "a day (2026-08-06)". It was ~56 minutes — ctbk's `a7becbad` added the second rule at 2026-08-05 22:12 EDT and `5451d708` fixed it at 23:08 the same evening. The 08-06 date came from reading the fix's UTC timestamp as a duration. The bug was real; the blast radius was under an hour.)*

Remaining in this class, not yet done: `upsert_lambda_role` never re-checks an existing role's trust policy or attachments (create-only reconciliation); `put_targets` uses a fixed target id and never prunes extra targets; `pyrmts_engine.batch.bootstrap` is describe-or-create throughout with no delete path.

**2. Derive the desired resource set from the pyramid config, and stop there.** The real gap in `pyrmts_ops.aws` is not that it lacks a state file — it is that `deploy_pyramid_lambda` takes loose strings and **nothing reads the pyramid YAML to say what a pyramid needs**. A pure function — config in, a description of the fill function, its schedule, its env, its bucket and database out — is useful to *every* consumer regardless of tool: ctbk's Pulumi program can declare it, awair's CDK stack can declare it, `pyrmts_ops.aws` can imperatively upsert it, and a `--dry-run` can print it. That is the honest version of "pyrmts owns the pyramid stack": pyrmts owns the *description*, the consumer owns the *application*. It is also the only piece that gets harder to add later, once each consumer has hand-written its own answer.

**3. Give the consumer something that notices.** `verify`-shaped, not `apply`-shaped, matching `specs/d1-schema-drift.md`: read-only checks that a consumer can run in CI or surface from `/health`. Schema is done. The obvious next one is a schedule/function check — does a rule exist per configured pyramid, is it enabled, does its target resolve — which needs only read permissions and would have caught both the v6 permission outage and the four months of `ctbk/infra/` drift. Detection is worth more than reconciliation here and costs an order of magnitude less, because the thing that actually failed in every incident above was **nobody noticed**, not *nobody could express it*.

## If a consumer still wants full IaC (recommendation to ctbk)

Do it in ctbk, not in pyrmts, and finish the stack that already exists rather than starting one: import the live Lambdas, rules, and ECR repos into `ctbk/infra/` so the first `pulumi up` is a no-op diff (OA's stated adoption order — *"that alone gives drift detection"*), move the state off the committed local-file backend, and wire the `infra.yml` that `specs/pulumi-cf-infra.md` already drafts so it runs on push rather than from a laptop. The unrun-for-four-months problem is a CI problem, not a tooling problem, and it will recur identically with any tool that a human has to remember to invoke.

Two ctbk-specific notes from the inventory, worth fixing whether or not the stack advances: the D1 database id is a pasted literal in the `wrangler.toml`s while `infra/` already exports it, and `gbfs/lambda/deploy.py` and `deploy-image.py` are two divergent copies of one resource graph, with the fixed `StatementId` bug still live in the former. *(Both addressed by ctbk in `fb0dbd6a`: `deploy.py` was deleted rather than fixed — all three Lambdas are `PackageType=Image`, so its `update_function_code(ZipFile=…)` had no valid target — and the id consistency check landed in `deploy.sh`, which found **five** blocks, not four: `gbfs/api/wrangler.toml` has two.)*

## Non-goals

- ~~pyrmts depending on Pulumi, CDK, Terraform, or any provider SDK.~~ *(Overtaken: `pyrmts_pulumi` depends on `pulumi`, with `pulumi-aws` / `pulumi-cloudflare` as extras. The convention it preserves is the one that mattered — providers are imported lazily inside the components, so a package needing only AWS never loads the Cloudflare provider, and `pyrmts`, `pyrmts-engine`, and `pyrmts-ops` take no IaC dependency at all.)*
- pyrmts owning consumer resources (queues, custom domains, worker secrets, Analytics Engine datasets, per-device stacks).
- Reconciliation or state tracking inside pyrmts. If a consumer wants a state file, that is what their IaC tool is for.

## Outcome (2026-08-29): the answer was wrong, and `pyrmts_pulumi` shipped

The user's clarification: *"provide Pulumi code in this repo that users of pyrmts can use, in their own Pulumi code/stacks, to save boilerplate and most easily stand up required or common infra patterns."*

That is a **component library**, and this document argued against a **framework adoption** — pyrmts owning a stack, a backend, credentials, and a tool choice that consumers inherit. Those are different propositions, and conflating them was the error: the original question said "offer some Pulumi/IaC for standing up the required/common resources", which is the library reading. Shipped as `python/pyrmts_pulumi/`: `S3ShardStore`, `R2ShardStore`, `ShardIndex`, `FillFunction`, `BatchEngine`, and a `Pyramid` that composes them. pyrmts still runs no stack, configures no provider, and holds no credentials.

**Which arguments here survived, and how they shaped the result:**

- *"The resource set is mostly not pyrmts-shaped."* Stands, and it defined the scope. The inventory above named the genuinely shared surface — fill Lambda, its schedule, D1, R2/S3 bucket (plus Batch) — and that is exactly the component set, no more. The conclusion drawn from it was wrong in direction: a small, well-bounded surface is an argument that a library is *cheap and safe*, not that it is unnecessary.
- *"Ownership boundaries would be wrong in both directions."* Stands, and became explicit non-goals rather than a reason to abstain. The package creates no Worker, no Pages app, no custom domain, and no D1 schema; `pyrmts-ops d1 apply` keeps the schema and `wrangler` keeps the app. `ShardIndex.wrangler_binding()` hands the app its binding instead of trying to own it.
- *"A half-adopted IaC layer implies coverage it doesn't have."* Stands, and is the reason the non-goals are stated in the README rather than left implicit.
- *"The conventions are three-way split."* Stands as a real limitation. awair is on CDK and cannot use these components. What it *can* use is the tool-neutral layer §2 asked for: `compute_environment_spec` / `job_definition_spec` emit AWS's own schemas, and `BatchEngine` consumes those same builders rather than restating them — so the description has one home and Pulumi is merely one applier of it.

**Two defects found by building it**, both from asking what happens with more than one consumer (`03406e0`):

1. `bootstrap` restated `vcpus: int = 8` beside a builder that said 16, shadowing it for every non-CLI caller — reproduced against the pre-fix tree. Sizing now delegates.
2. Every Batch/IAM/log name was an account-global module constant, and `submit` hard-coded `jobDefinition=PREFIX`, so two consumers in one AWS account would clobber each other. `resource_names(prefix)` is now the one mapping.

The imperative path still defaults its prefix to the shared `pyrmts-engine`, so isolation there remains opt-in via `-p`. `pyrmts_pulumi` does not inherit that: `default_prefix()` is `<project>-<stack>`, distinct per deployment by construction, and a test asserts two prefixes yield disjoint names for every resource type.

**What did not change:** §1's repairs and §3's `verify`-shaped detection still stand on their own — a component library reconciles what it declares and notices nothing about what it doesn't, so `pyrmts-ops d1 verify` remains the thing that catches drift in CI, and the schedule/function check §3 proposes is still unbuilt and still the highest-value next step.
