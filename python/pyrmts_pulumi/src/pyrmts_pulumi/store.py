"""Shard storage: the bucket a pyramid writes shards (and optionally raw
ingest) into.

Two backends, because pyrmts consumers use both: S3 for AWS-native
deployments and R2 where the query side is a Cloudflare Worker (R2 has no
egress fee to a Worker, which is the whole reason ctbk uses it). Both speak
the S3 API, so `pyrmts.S3Storage` reads either — only the credentials and
endpoint differ, and both components emit the env block the engine needs."""
from __future__ import annotations

from typing import Mapping

import pulumi

from ._names import scoped_names


class S3ShardStore(pulumi.ComponentResource):
    """An S3 bucket for a pyramid's shards.

    `expire_raw_after_days` applies a lifecycle rule to the raw-ingest
    prefix only. Shards are never expired here: what a pyramid may drop is
    decided by the cover ladder and `pyrmts_ops.gc`, which knows which rungs
    are load-bearing — a blanket age rule would delete shards the cover
    still expects and the fill loop would immediately rebuild them."""

    bucket: pulumi.Output[str]
    arn: pulumi.Output[str]

    def __init__(
        self,
        name: str,
        *,
        prefix: str | None = None,
        bucket_name: str | pulumi.Output[str] | None = None,
        raw_prefix: str = 'raw/',
        expire_raw_after_days: int | None = None,
        force_destroy: bool = False,
        tags: Mapping[str, str] | None = None,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__('pyrmts:index:S3ShardStore', name, {}, opts)
        import pulumi_aws as aws

        child = pulumi.ResourceOptions(parent=self)
        names = scoped_names(prefix)
        bucket = aws.s3.Bucket(
            f'{name}-bucket',
            # Explicit when given; otherwise Pulumi autonames from the stack,
            # which is what keeps two deployments from claiming one bucket.
            bucket=bucket_name if bucket_name is not None else names.prefix,
            force_destroy=force_destroy,
            tags=dict(tags or {}),
            opts=child,
        )
        if expire_raw_after_days is not None:
            aws.s3.BucketLifecycleConfiguration(
                f'{name}-lifecycle',
                bucket=bucket.id,
                rules=[aws.s3.BucketLifecycleConfigurationRuleArgs(
                    id='expire-raw',
                    status='Enabled',
                    filter=aws.s3.BucketLifecycleConfigurationRuleFilterArgs(prefix=raw_prefix),
                    expiration=aws.s3.BucketLifecycleConfigurationRuleExpirationArgs(
                        days=expire_raw_after_days,
                    ),
                )],
                opts=child,
            )
        self.bucket = bucket.bucket
        self.arn = bucket.arn
        self.register_outputs({'bucket': self.bucket, 'arn': self.arn})

    def engine_env(self) -> dict[str, pulumi.Output[str]]:
        """Env vars pointing the engine at this bucket (S3 needs no endpoint
        override; creds come from the task/execution role)."""
        return {'PYRMTS_BUCKET': self.bucket}


class R2ShardStore(pulumi.ComponentResource):
    """A Cloudflare R2 bucket for a pyramid's shards.

    R2 has no lifecycle-rule resource in the Cloudflare provider, so raw
    expiry is not offered here (rather than silently ignored) — set it in
    the dashboard or via the R2 API if you need it."""

    bucket: pulumi.Output[str]
    account_id: pulumi.Output[str]

    def __init__(
        self,
        name: str,
        *,
        account_id: str | pulumi.Output[str],
        prefix: str | None = None,
        bucket_name: str | pulumi.Output[str] | None = None,
        location: str | None = None,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__('pyrmts:index:R2ShardStore', name, {}, opts)
        import pulumi_cloudflare as cloudflare

        names = scoped_names(prefix)
        bucket = cloudflare.R2Bucket(
            f'{name}-bucket',
            account_id=account_id,
            name=bucket_name if bucket_name is not None else names.prefix,
            location=location,
            opts=pulumi.ResourceOptions(parent=self),
        )
        self.bucket = bucket.name
        self.account_id = pulumi.Output.from_input(account_id)
        self.register_outputs({'bucket': self.bucket, 'account_id': self.account_id})

    def engine_env(self) -> dict[str, pulumi.Output[str]]:
        """Env vars pointing the engine at this bucket. The access key pair
        is deliberately absent — R2 tokens are secrets, so the consumer
        supplies them (`pulumi.Config.require_secret`) rather than having
        this component mint and store them in stack state."""
        return {
            'PYRMTS_BUCKET': self.bucket,
            'R2_ENDPOINT_URL': self.account_id.apply(
                lambda a: f'https://{a}.r2.cloudflarestorage.com',
            ),
        }
