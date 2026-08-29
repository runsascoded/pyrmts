# pyrmts-pulumi

Pulumi [ComponentResource]s for the cloud footprint a pyrmts pyramid needs. Import them into **your own** Pulumi program — this package defines no stack, no backend, and no provider configuration of its own, so your account, region, and credentials stay entirely yours.

```python
import pulumi
from pyrmts_pulumi import Pyramid, Schedule

cfg = pulumi.Config()
pyramid = Pyramid(
    'gbfs',
    cloudflare_account_id=cfg.require('cloudflare_account_id'),
    fill_image_uri=cfg.require('fill_image'),
    schedules=[Schedule('tick', 'rate(5 minutes)', input_json='{"pyramid":"avail-v6"}')],
)
pulumi.export('pyrmts', pyramid.stack_outputs())
```

## Isolation

Every resource here lives in a namespace that is **global to the cloud account** — Batch job definitions, queues and compute environments; IAM roles; log groups; bucket and D1 database names. Two deployments collide on all of them unless something separates them.

The prefix defaults to `<pulumi-project>-<pulumi-stack>`, which is distinct for every deployment by construction, so isolation is the default rather than a flag you must remember. Pass `prefix=` to override — to adopt names that already exist in an account, or to keep a name stable across a project rename.

### Why explicit names, not Pulumi autonaming

Pulumi normally guarantees isolation for free: with no explicit `name=`, it appends a random suffix per stack (`probe-auto` → `probe-auto-885d82d`), so two stacks cannot collide. These components **opt out** of that, setting physical names from the prefix.

The reason is addressability. pyrmts's runtime resolves names rather than reading stack outputs — `pyrmts-engine batch submit -p <prefix>` derives the queue, job definition and log group from the prefix — and a random suffix is not derivable. Deterministic `<project>-<stack>` names keep both properties: unique per deployment *and* reproducible by the CLI.

The residual risk autonaming would have covered: `pulumi.get_stack()` is the bare stack name, so two stacks with the same project *and* stack name, in different orgs or backends, deploying to one cloud account, still collide. If that is your situation, pass an explicit `prefix`.

This is deliberately *stricter* than the imperative path: `pyrmts-engine batch bootstrap` defaults its prefix to the shared constant `pyrmts-engine`, so two consumers that both take its default clobber each other. Use `-p` there, or use this package.

## Handoff to the runtime

Infra declared here is addressed by name at runtime, so the components emit exactly what the other tools need:

| Output | Consumer |
|---|---|
| `prefix` | `pyrmts-engine batch submit -p <prefix>` (also `BatchEngine.submit_args()`) |
| `bucket`, `R2_ENDPOINT_URL` | engine + handler env |
| `d1_database_id` | `wrangler.toml` (`ShardIndex.wrangler_binding()`) |
| `batch_queue`, `batch_job_definition` | submissions that pin a queue explicitly |

`scoped_names()` is `pyrmts_engine.batch.resource_names`, not a copy, so a stack stood up here resolves to the same names the CLI derives. Likewise `BatchEngine` builds its job definition from `job_definition_spec` — the same function the imperative `bootstrap` calls — because AWS's `ContainerProperties` schema is what both appliers take. One description, two appliers, no drift.

## Deliberate non-goals

Two things this package does **not** create, in both cases because they already have a better owner:

- **The query worker / `/health` app.** Workers and Pages deploy from `wrangler` in the app's own repo with its own build; a Pulumi resource wrapping that would fight the app's pipeline for ownership. What the app needs from here is its *bindings*, which `ShardIndex.wrangler_binding()` and the stores emit.
- **The D1 schema.** `pyrmts-ops d1 {schema,verify,apply}` owns it. Pulumi has no clean way to express "apply these statements, in order, once", and migration numbering is consumer-owned (consumers interleave their own tables). Run `pyrmts-ops d1 verify` in CI — detection is the half that pays.

## Examples

`examples/aws/` and `examples/cloudflare/` are runnable programs — a reference to read, a template to copy, and a schema check you can run against a real account with `pulumi preview` (read-only, read-scoped credentials suffice). See `examples/README.md`.

## Installing

```bash
pip install 'pyrmts-pulumi[aws,cloudflare]'
```

Providers are extras: an AWS-only deployment needn't install `pulumi-cloudflare`, and imports are function-local so a missing provider surfaces only when you use a component that needs it.
