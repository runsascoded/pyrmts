"""Batch packaging: pure spec builders + storage-backed manifest +
`--source` factory hook. (No AWS calls — the boto3 orchestration is a thin
wrapper over these builders.)"""
from __future__ import annotations

import json
from dataclasses import astuple, replace

from pyrmts import MemStorage
from pyrmts_engine import ShardRecord, StorageJsonlShardIndex, WideShardSource, build_local
from pyrmts_engine.batch import (
    ECR_LIFECYCLE_POLICY,
    Names,
    bootstrap,
    build_command,
    compute_environment_spec,
    job_definition_spec,
    push_commands,
    resource_names,
    submit,
    submit_overrides,
)

from conftest import FROM, TO, make_pyramid, write_base_shards


def test_job_definition_spec():
    assert job_definition_spec(
        image='123.dkr.ecr.us-east-1.amazonaws.com/pyrmts-engine:abc1234',
        execution_role_arn='arn:aws:iam::123:role/pyrmts-engine-batch-execution',
        environment={'R2_ENDPOINT_URL': 'https://x.r2.cloudflarestorage.com'},
    ) == {
        'jobDefinitionName': 'pyrmts-engine',
        'type': 'container',
        'platformCapabilities': ['FARGATE'],
        'containerProperties': {
            'image': '123.dkr.ecr.us-east-1.amazonaws.com/pyrmts-engine:abc1234',
            'runtimePlatform': {
                'operatingSystemFamily': 'LINUX',
                'cpuArchitecture': 'X86_64',
            },
            'resourceRequirements': [
                {'type': 'VCPU', 'value': '16'},
                {'type': 'MEMORY', 'value': '32768'},
            ],
            'ephemeralStorage': {'sizeInGiB': 100},
            'executionRoleArn': 'arn:aws:iam::123:role/pyrmts-engine-batch-execution',
            'networkConfiguration': {'assignPublicIp': 'ENABLED'},
            'logConfiguration': {
                'logDriver': 'awslogs',
                'options': {'awslogs-group': '/pyrmts-engine/batch'},
            },
            'environment': [
                {'name': 'R2_ENDPOINT_URL', 'value': 'https://x.r2.cloudflarestorage.com'},
            ],
        },
        'retryStrategy': {'attempts': 2},
    }


def test_job_definition_spec_arm64():
    spec = job_definition_spec(
        image='img',
        arch='ARM64',
        execution_role_arn='arn',
    )
    assert spec['containerProperties']['runtimePlatform'] == {
        'operatingSystemFamily': 'LINUX',
        'cpuArchitecture': 'ARM64',
    }


def test_ecr_lifecycle_policy():
    assert ECR_LIFECYCLE_POLICY == {
        'rules': [
            {
                'rulePriority': 1,
                'description': 'expire untagged (superseded buildx manifests) after 7 days',
                'selection': {
                    'tagStatus': 'untagged',
                    'countType': 'sinceImagePushed',
                    'countUnit': 'days',
                    'countNumber': 7,
                },
                'action': {'type': 'expire'},
            },
            {
                'rulePriority': 2,
                'description': 'keep the 4 most recent tags',
                'selection': {
                    'tagStatus': 'tagged',
                    'tagPatternList': ['*'],
                    'countType': 'imageCountMoreThan',
                    'countNumber': 4,
                },
                'action': {'type': 'expire'},
            },
        ],
    }


def test_push_commands():
    ref = '123.dkr.ecr.us-east-1.amazonaws.com/pyrmts-engine:abc1234'
    assert push_commands(
        ref,
        dockerfile='python/pyrmts_engine/Dockerfile',
    ) == [
        ['docker', 'build', '-t', ref, '--provenance=false', '--sbom=false',
         '--platform', 'linux/amd64',
         '-f', 'python/pyrmts_engine/Dockerfile', '.'],
        ['docker', 'push', ref],
    ]
    assert push_commands(ref, build=False) == [['docker', 'push', ref]]
    assert push_commands(ref, platform=None, context='wt/x') == [
        ['docker', 'build', '-t', ref, '--provenance=false', '--sbom=false', 'wt/x'],
        ['docker', 'push', ref],
    ]


def test_compute_environment_spec():
    assert compute_environment_spec(subnets=['subnet-1', 'subnet-2'], security_group_ids=['sg-1']) == {
        'computeEnvironmentName': 'pyrmts-engine-spot',
        'type': 'MANAGED',
        'state': 'ENABLED',
        'computeResources': {
            'type': 'FARGATE_SPOT',
            'maxvCpus': 16,
            'subnets': ['subnet-1', 'subnet-2'],
            'securityGroupIds': ['sg-1'],
        },
    }
    assert compute_environment_spec(spot=False, subnets=['subnet-1'], security_group_ids=['sg-1']) == {
        'computeEnvironmentName': 'pyrmts-engine-od',
        'type': 'MANAGED',
        'state': 'ENABLED',
        'computeResources': {
            'type': 'FARGATE',
            'maxvCpus': 16,
            'subnets': ['subnet-1'],
            'securityGroupIds': ['sg-1'],
        },
    }


def test_build_command():
    assert build_command(
        's3://ctbk/configs/avail-v4.yaml',
        pyramid_name='avail-v4',
        range_='2026-04-01T00:00/2026-07-18T00:00',
        window='12h',
        rg_size=2048,
        sort='s2_cell,dt',
        manifest='s3://ctbk/engine-check/manifest.jsonl',
        filters=('device_id=17617',),
    ) == [
        'build',
        '-n', 'avail-v4',
        '-r', '2026-04-01T00:00/2026-07-18T00:00',
        '-w', '12h',
        '-g', '2048',
        '-s', 's2_cell,dt',
        '-m', 's3://ctbk/engine-check/manifest.jsonl',
        '-F', 'device_id=17617',
        '-v',
        's3://ctbk/configs/avail-v4.yaml',
    ]


def test_build_command_source_rung_fill_resume_allow_empty():
    assert build_command(
        'cfg.yaml',
        pyramid_name='avail-v4',
        range_='2026-04-01T00:00/2026-07-18T00:00',
        source_tier='1m',
        source_shard='2d',
        fill=True,
        resume=True,
        allow_empty=True,
        max_missing=0.5,
    ) == [
        'build',
        '-n', 'avail-v4',
        '-r', '2026-04-01T00:00/2026-07-18T00:00',
        '-t', '1m',
        '-d', '2d',
        '-f',
        '-u',
        '-e',
        '-M', '0.5',
        '-v',
        'cfg.yaml',
    ]


def test_submit_overrides():
    assert submit_overrides(
        ['build', '-n', 'x', 'cfg.yaml'],
        vcpus=16,
        memory_mib=65536,
        environment={'R2_ACCESS_KEY_ID': 'k'},
    ) == {
        'command': ['build', '-n', 'x', 'cfg.yaml'],
        'resourceRequirements': [
            {'type': 'VCPU', 'value': '16'},
            {'type': 'MEMORY', 'value': '65536'},
        ],
        'environment': [{'name': 'R2_ACCESS_KEY_ID', 'value': 'k'}],
    }
    assert submit_overrides(['build']) == {'command': ['build']}


def test_storage_jsonl_shard_index_flushes_and_closes():
    """Engine build with a storage-backed manifest: periodic re-PUTs during
    the run (flush_every=4 → 11 records ⇒ 2 mid-run + 1 close), full
    content at the end."""
    manifest_store = MemStorage()
    puts: list[int] = []
    orig_put = manifest_store.put

    def counting_put(key: str, data: bytes) -> None:
        puts.append(len(data.decode().rstrip('\n').split('\n')))
        orig_put(key, data)

    manifest_store.put = counting_put  # type: ignore[method-assign]

    pyramid = make_pyramid()
    write_base_shards(pyramid)
    result = build_local(
        pyramid, (FROM, TO), WideShardSource(pyramid, shard_dur='6h'),
        pyramid_name='test',
        shard_index=StorageJsonlShardIndex(manifest_store, 'manifests/run.jsonl', flush_every=4),
    )
    assert puts == [4, 8, 11]
    lines = manifest_store.get('manifests/run.jsonl').decode().rstrip('\n').split('\n')
    assert sorted(json.loads(l)['key'] for l in lines) == sorted(w.key for w in result.written)


def test_storage_jsonl_shard_index_default_cadence_and_reload():
    """Default flush_every=1: a PUT per record (a reclaimed container loses
    nothing). A fresh index over the same key loads prior records —
    `existing_keys()` feeds resume, and re-PUTs stay cumulative."""
    store = MemStorage()
    idx = StorageJsonlShardIndex(store, 'm.jsonl')
    rec = ShardRecord(
        pyramid='p', tier='t', shard_dur='1d',
        period_start_ms=0, period_end_ms=1, key='k1', written_at_ms=5,
    )
    idx.record_shard(rec)
    assert store.get('m.jsonl').decode() == (
        '{"pyramid": "p", "tier": "t", "shard_dur": "1d", "period_start": 0, '
        '"period_end": 1, "key": "k1", "written_at": 5}\n'
    )

    idx2 = StorageJsonlShardIndex(store, 'm.jsonl')
    assert idx2.existing_keys() == {'k1'}
    idx2.record_shard(replace(rec, key='k2'))
    lines = store.get('m.jsonl').decode().rstrip('\n').split('\n')
    assert [json.loads(l)['key'] for l in lines] == ['k1', 'k2']


# -- resource naming --------------------------------------------------------

def test_resource_names_default():
    assert resource_names() == Names(
        prefix='pyrmts-engine',
        job_definition='pyrmts-engine',
        execution_role='pyrmts-engine-batch-execution',
        log_group='/pyrmts-engine/batch',
        spot_ce='pyrmts-engine-spot',
        od_ce='pyrmts-engine-od',
        spot_queue='pyrmts-engine',
        od_queue='pyrmts-engine-od',
    )


def test_resource_names_custom_prefix_isolates_every_account_global_name():
    # Two consumers in one AWS account collide on all of these unless the
    # prefix reaches every one; no name may fall back to the default.
    a, b = resource_names(), resource_names('awair-pyrmts')
    assert b == Names(
        prefix='awair-pyrmts',
        job_definition='awair-pyrmts',
        execution_role='awair-pyrmts-batch-execution',
        log_group='/awair-pyrmts/batch',
        spot_ce='awair-pyrmts-spot',
        od_ce='awair-pyrmts-od',
        spot_queue='awair-pyrmts',
        od_queue='awair-pyrmts-od',
    )
    assert set(astuple(a)) & set(astuple(b)) == set()


def test_names_queue_selects_by_on_demand():
    names = resource_names()
    assert [names.queue(), names.queue(on_demand=False), names.queue(on_demand=True)] == [
        'pyrmts-engine', 'pyrmts-engine', 'pyrmts-engine-od',
    ]


def test_job_definition_spec_log_group_follows_the_name():
    spec = job_definition_spec(
        name='awair-pyrmts', image='img', execution_role_arn='arn:role',
    )
    assert [
        spec['jobDefinitionName'],
        spec['containerProperties']['logConfiguration']['options']['awslogs-group'],
    ] == ['awair-pyrmts', '/awair-pyrmts/batch']


def test_compute_environment_spec_prefix():
    assert [
        compute_environment_spec(
            prefix=p, spot=spot, subnets=['s'], security_group_ids=['g'],
        )['computeEnvironmentName']
        for p, spot in (('pyrmts-engine', True), ('pyrmts-engine', False),
                        ('awair-pyrmts', True), ('awair-pyrmts', False))
    ] == ['pyrmts-engine-spot', 'pyrmts-engine-od', 'awair-pyrmts-spot', 'awair-pyrmts-od']


# -- bootstrap orchestration ------------------------------------------------

class _NoSuchEntity(Exception): pass
class _NoRepo(Exception): pass


class FakeBatchClients:
    """Recorded-call stand-in for the five boto3 clients `bootstrap` uses.

    Everything already exists, so `bootstrap` takes its describe-branches and
    the only mutation is the job-definition registration we assert on."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    # -- iam
    class _Exc:
        NoSuchEntityException = _NoSuchEntity
        RepositoryNotFoundException = _NoRepo
    exceptions = _Exc

    def get_role(self, RoleName: str) -> dict:
        self.calls.append(('get_role', {'RoleName': RoleName}))
        return {'Role': {'Arn': f'arn:aws:iam::123:role/{RoleName}'}}

    # -- logs
    def describe_log_groups(self, logGroupNamePrefix: str) -> dict:
        self.calls.append(('describe_log_groups', {'logGroupNamePrefix': logGroupNamePrefix}))
        return {'logGroups': [{'logGroupName': logGroupNamePrefix}]}

    # -- ecr
    def describe_repositories(self, repositoryNames: list[str]) -> dict:
        self.calls.append(('describe_repositories', {'repositoryNames': repositoryNames}))
        return {}

    # -- ec2
    def describe_subnets(self, Filters: list) -> dict:
        return {'Subnets': [{'SubnetId': 'subnet-1'}]}

    def describe_security_groups(self, Filters: list) -> dict:
        return {'SecurityGroups': [{'GroupId': 'sg-1'}]}

    # -- batch
    def describe_compute_environments(self, computeEnvironments: list[str]) -> dict:
        self.calls.append(('describe_compute_environments', {'names': computeEnvironments}))
        return {'computeEnvironments': [{'status': 'VALID'}]}

    def describe_job_queues(self, jobQueues: list[str]) -> dict:
        self.calls.append(('describe_job_queues', {'names': jobQueues}))
        return {'jobQueues': [{'jobQueueName': jobQueues[0]}]}

    def register_job_definition(self, **kwargs) -> dict:
        self.calls.append(('register_job_definition', kwargs))
        return {}

    def submit_job(self, **kwargs) -> dict:
        self.calls.append(('submit_job', kwargs))
        return {'jobId': 'job-1'}

    def as_clients(self) -> dict:
        return {name: self for name in ('iam', 'logs', 'ecr', 'ec2', 'batch')}


def _registered(fake: FakeBatchClients) -> dict:
    return [kw for name, kw in fake.calls if name == 'register_job_definition'][0]


def _sizing(spec: dict) -> dict:
    cp = spec['containerProperties']
    return {
        **{r['type']: r['value'] for r in cp['resourceRequirements']},
        'EPHEMERAL': cp['ephemeralStorage']['sizeInGiB'],
    }


def test_bootstrap_job_definition_sizing_defaults_to_the_spec_builders():
    # Regression: `bootstrap` used to restate `vcpus: int = 8`, shadowing
    # `job_definition_spec`'s 16 for every non-CLI caller — so a job could
    # not fill the 16-vCPU compute environment it ran in.
    fake = FakeBatchClients()
    bootstrap(image='123.dkr.ecr.us-east-1.amazonaws.com/pyrmts-engine:abc', clients=fake.as_clients())
    assert _sizing(_registered(fake)) == {'VCPU': '16', 'MEMORY': '32768', 'EPHEMERAL': 100}


def test_bootstrap_sizing_overrides_are_passed_through():
    fake = FakeBatchClients()
    bootstrap(
        image='img', vcpus=4, memory_mib=8192, ephemeral_gib=50,
        clients=fake.as_clients(),
    )
    assert _sizing(_registered(fake)) == {'VCPU': '4', 'MEMORY': '8192', 'EPHEMERAL': 50}


def test_bootstrap_prefix_reaches_every_resource():
    fake = FakeBatchClients()
    bootstrap(image='img', prefix='awair-pyrmts', on_demand=True, clients=fake.as_clients())
    # Full sequence, not a prefix: every account-global name bootstrap reads
    # or writes must carry the prefix. (Each CE is described twice — once to
    # branch on existence, once by `_wait` polling for VALID.)
    assert [c for c in fake.calls if c[0] != 'register_job_definition'] == [
        ('get_role', {'RoleName': 'awair-pyrmts-batch-execution'}),
        ('describe_log_groups', {'logGroupNamePrefix': '/awair-pyrmts/batch'}),
        ('describe_repositories', {'repositoryNames': ['img']}),
        ('describe_compute_environments', {'names': ['awair-pyrmts-spot']}),
        ('describe_compute_environments', {'names': ['awair-pyrmts-spot']}),
        ('describe_job_queues', {'names': ['awair-pyrmts']}),
        ('describe_compute_environments', {'names': ['awair-pyrmts-od']}),
        ('describe_compute_environments', {'names': ['awair-pyrmts-od']}),
        ('describe_job_queues', {'names': ['awair-pyrmts-od']}),
    ]
    spec = _registered(fake)
    assert [
        spec['jobDefinitionName'],
        spec['containerProperties']['logConfiguration']['options']['awslogs-group'],
        spec['containerProperties']['executionRoleArn'],
    ] == [
        'awair-pyrmts',
        '/awair-pyrmts/batch',
        'arn:aws:iam::123:role/awair-pyrmts-batch-execution',
    ]


def test_submit_targets_the_prefixed_job_definition_and_queue():
    # `submit` used to hard-code `jobDefinition=PREFIX`, so a job def
    # bootstrapped under another prefix was unreachable.
    for kwargs, expected in (
        ({}, ('pyrmts-engine', 'pyrmts-engine')),
        ({'on_demand': True}, ('pyrmts-engine', 'pyrmts-engine-od')),
        ({'prefix': 'awair-pyrmts'}, ('awair-pyrmts', 'awair-pyrmts')),
        ({'prefix': 'awair-pyrmts', 'on_demand': True}, ('awair-pyrmts', 'awair-pyrmts-od')),
        ({'queue': 'custom'}, ('pyrmts-engine', 'custom')),
    ):
        fake = FakeBatchClients()
        submit(command=['build'], job_name='j', clients=fake.as_clients(), **kwargs)
        sent = [kw for name, kw in fake.calls if name == 'submit_job'][0]
        assert (sent['jobDefinition'], sent['jobQueue']) == expected
