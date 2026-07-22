"""Batch packaging: pure spec builders + storage-backed manifest +
`--source` factory hook. (No AWS calls — the boto3 orchestration is a thin
wrapper over these builders.)"""
from __future__ import annotations

import json
from dataclasses import replace

from pyrmts import MemStorage
from pyrmts_engine import ShardRecord, StorageJsonlShardIndex, WideShardSource, build_local
from pyrmts_engine.batch import (
    ECR_LIFECYCLE_POLICY,
    build_command,
    compute_environment_spec,
    job_definition_spec,
    push_commands,
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
                {'type': 'VCPU', 'value': '8'},
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
        ['docker', 'build', '-t', ref, '--platform', 'linux/amd64',
         '-f', 'python/pyrmts_engine/Dockerfile', '.'],
        ['docker', 'push', ref],
    ]
    assert push_commands(ref, build=False) == [['docker', 'push', ref]]
    assert push_commands(ref, platform=None, context='wt/x') == [
        ['docker', 'build', '-t', ref, 'wt/x'],
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


def test_build_command_source_rung_resume_allow_empty():
    assert build_command(
        'cfg.yaml',
        pyramid_name='avail-v4',
        range_='2026-04-01T00:00/2026-07-18T00:00',
        source_tier='1m',
        source_shard='2d',
        resume=True,
        allow_empty=True,
        max_missing=0.5,
    ) == [
        'build',
        '-n', 'avail-v4',
        '-r', '2026-04-01T00:00/2026-07-18T00:00',
        '-t', '1m',
        '-d', '2d',
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
        pyramid, (FROM, TO), WideShardSource(pyramid),
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
