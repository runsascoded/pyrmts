"""AWS-side components: shard store, fill Lambda + schedules, batch engine.

Runnable as-is with `pulumi preview` (read-only) against any AWS account —
nothing here is created, and previewing is how this doubles as a check that
the provider still accepts what the components declare. It is also the
template to copy: a real deployment replaces the placeholder image URIs and
networking, and adds whatever else that project needs *around* these."""
import pulumi
from pyrmts_pulumi import BatchEngine, FillFunction, S3ShardStore, Schedule

cfg = pulumi.Config()
# Placeholders so `preview` runs with no setup; override in a real stack.
fill_image = cfg.get('fillImage') or '000000000000.dkr.ecr.us-east-1.amazonaws.com/fill:latest'
engine_image = cfg.get('engineImage') or '000000000000.dkr.ecr.us-east-1.amazonaws.com/engine:latest'
subnets = cfg.get_object('subnetIds') or ['subnet-00000000000000000']
security_groups = cfg.get_object('securityGroupIds') or ['sg-00000000000000000']

# `prefix` defaults to `<project>-<stack>`; pass one explicitly only to adopt
# existing names or to keep a name stable across a project rename.
store = S3ShardStore('store', expire_raw_after_days=30)

fill = FillFunction(
    'fill',
    image_uri=fill_image,
    env=store.engine_env(),
    schedules=[
        Schedule('tick', 'rate(5 minutes)', input_json='{"pyramid": "avail-v6"}'),
        # A retired pyramid stays declared, DISABLED — so the retirement is a
        # fact in the graph rather than the absence of code, and nothing
        # re-enables it on the next deploy.
        Schedule('v3-tick', 'rate(1 hour)', enabled=False),
    ],
)

engine = BatchEngine(
    'engine',
    image_uri=engine_image,
    subnet_ids=subnets,
    security_group_ids=security_groups,
    on_demand=True,
)

pulumi.export('bucket', store.bucket)
pulumi.export('fill_function', fill.function_name)
pulumi.export('batch_queue', engine.queue)
# What `pyrmts-engine batch submit` needs to reach the stack.
pulumi.export('submit_args', engine.submit_args())
