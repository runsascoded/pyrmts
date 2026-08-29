"""Stores and the shard index."""
from __future__ import annotations

import pulumi
import pytest
from conftest import DECLARED, declared

from pyrmts_pulumi import Pyramid, R2ShardStore, S3ShardStore, ShardIndex

BUCKET = 'aws:s3/bucket:Bucket'
LIFECYCLE = 'aws:s3/bucketLifecycleConfiguration:BucketLifecycleConfiguration'
R2 = 'cloudflare:index/r2Bucket:R2Bucket'
D1 = 'cloudflare:index/d1Database:D1Database'


@pulumi.runtime.test
def test_s3_store_names_the_bucket_from_the_prefix():
    store = S3ShardStore('s1', prefix='alpha')
    def check(_):
        (_, inputs), = declared(BUCKET, 's1')
        assert inputs['bucket'] == 'alpha'
        assert declared(LIFECYCLE, 's1') == []
    return pulumi.Output.all(store.bucket).apply(check)


@pulumi.runtime.test
def test_raw_expiry_scopes_its_rule_to_the_raw_prefix():
    # Shards must never fall under a blanket age rule — the cover ladder and
    # `pyrmts_ops.gc` decide what is droppable, and the fill loop would
    # rebuild anything deleted while the cover still expects it.
    store = S3ShardStore('s2', prefix='beta', expire_raw_after_days=30)
    def check(_):
        (_, inputs), = declared(LIFECYCLE, 's2')
        assert inputs['rules'] == [{
            'id': 'expire-raw',
            'status': 'Enabled',
            'filter': {'prefix': 'raw/'},
            'expiration': {'days': 30},
        }]
    return pulumi.Output.all(store.bucket).apply(check)


@pulumi.runtime.test
def test_r2_store_env_derives_the_endpoint():
    store = R2ShardStore('s3', account_id='acct123', prefix='gamma')
    env = store.engine_env()
    def check(v):
        assert v == ['gamma', 'https://acct123.r2.cloudflarestorage.com']
    return pulumi.Output.all(env['PYRMTS_BUCKET'], env['R2_ENDPOINT_URL']).apply(check)


@pulumi.runtime.test
def test_shard_index_emits_a_wrangler_binding():
    # The id has one source; a worker config generated from this cannot
    # disagree with the database Pulumi created.
    idx = ShardIndex('i1', account_id='acct123', prefix='delta')
    return idx.wrangler_binding().apply(lambda b: (
        None if b == {
            'binding': 'DB', 'database_name': 'delta', 'database_id': 'i1-db-id',
        } else (_ for _ in ()).throw(AssertionError(b))
    ))


@pulumi.runtime.test
def test_pyramid_rejects_two_store_backends_or_none():
    def check(_):
        with pytest.raises(ValueError, match='exactly one of'):
            Pyramid('p1', s3_store=True, cloudflare_account_id='acct')
        with pytest.raises(ValueError, match='exactly one of'):
            Pyramid('p2')
    return pulumi.Output.from_input(0).apply(check)


@pulumi.runtime.test
def test_pyramid_rejects_s3_only_options_on_r2():
    def check(_):
        with pytest.raises(ValueError, match='S3-only'):
            Pyramid('p3', cloudflare_account_id='acct', expire_raw_after_days=7)
    return pulumi.Output.from_input(0).apply(check)


@pulumi.runtime.test
def test_pyramid_wires_store_and_index_into_the_fill_env():
    # The actual boilerplate this saves: one prefix, and the bucket/database
    # identifiers threaded into the function's environment.
    p = Pyramid(
        'p4', prefix='ctbk-gbfs', cloudflare_account_id='acct123',
        fill_image_uri='repo:tag',
    )
    def check(_):
        (_, fn), = declared('aws:lambda/function:Function', 'p4')
        assert fn['environment'] == {'variables': {
            'PYRMTS_BUCKET': 'ctbk-gbfs',
            'R2_ENDPOINT_URL': 'https://acct123.r2.cloudflarestorage.com',
            'PYRMTS_D1_DATABASE_ID': 'p4-index-db-id',
        }}
        assert [n for _, n, _ in DECLARED].count('p4-store-bucket') == 1
    # Depend on the *function's* output: the store's resolves before the
    # function is registered, so checking on it races the graph.
    return pulumi.Output.all(p.fill.arn).apply(check)


@pulumi.runtime.test
def test_pyramid_stack_outputs_cover_every_handoff():
    p = Pyramid(
        'p5', prefix='ctbk-gbfs', cloudflare_account_id='acct123',
        fill_image_uri='repo:tag', engine_image_uri='engine:tag',
        subnet_ids=['subnet-1'], security_group_ids=['sg-1'],
    )
    out = p.stack_outputs()
    return pulumi.Output.all(**out).apply(lambda v: (
        None if v == {
            'prefix': 'ctbk-gbfs',
            'bucket': 'ctbk-gbfs',
            'd1_database_id': 'p5-index-db-id',
            'd1_database_name': 'ctbk-gbfs',
            'fill_function': 'ctbk-gbfs',
            'batch_queue': 'ctbk-gbfs',
            'batch_job_definition': 'ctbk-gbfs',
        } else (_ for _ in ()).throw(AssertionError(v))
    ))
