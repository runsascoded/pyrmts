"""One pyramid's cloud footprint, composed.

The individual components are usable on their own; this exists because the
boilerplate a consumer actually rewrites is not any single resource — it is
the *wiring*: giving every resource a consistent prefix, threading the
bucket and database identifiers into the fill function's environment, and
re-exporting the handful of values the runtime CLI and the query worker need.

Deliberately not included, in both cases because the thing already has a
better owner:

- **The query worker / `/health` app.** Workers and Pages deploy from
  `wrangler` in the app's own repo, with its own build. A Pulumi resource
  wrapping that would fight the app's deploy pipeline for ownership; what
  this package owes the app is its *bindings*, which `ShardIndex` and the
  stores emit.
- **The D1 schema.** See `index.py` — `pyrmts-ops d1 apply` owns it, and
  `verify` is what belongs in CI."""
from __future__ import annotations

from typing import Mapping, Sequence

import pulumi

from ._names import scoped_names
from .engine import BatchEngine
from .fill import FillFunction, Schedule
from .index import ShardIndex
from .store import R2ShardStore, S3ShardStore


class Pyramid(pulumi.ComponentResource):
    """Store + index + fill function (+ optional batch engine) under one prefix."""

    prefix: str
    store: S3ShardStore | R2ShardStore
    index: ShardIndex | None
    fill: FillFunction | None
    engine: BatchEngine | None

    def __init__(
        self,
        name: str,
        *,
        prefix: str | None = None,
        # -- store: exactly one backend
        cloudflare_account_id: str | pulumi.Output[str] | None = None,
        s3_store: bool = False,
        bucket_name: str | pulumi.Output[str] | None = None,
        expire_raw_after_days: int | None = None,
        # -- index
        shard_index: bool = True,
        # -- fill
        fill_image_uri: str | pulumi.Output[str] | None = None,
        fill_env: Mapping[str, str | pulumi.Output[str]] | None = None,
        fill_memory_mb: int = 2048,
        schedules: Sequence[Schedule] = (),
        # -- batch engine
        engine_image_uri: str | pulumi.Output[str] | None = None,
        subnet_ids: Sequence[str] | pulumi.Output[Sequence[str]] | None = None,
        security_group_ids: Sequence[str] | pulumi.Output[Sequence[str]] | None = None,
        engine_env: Mapping[str, str | pulumi.Output[str]] | None = None,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__('pyrmts:index:Pyramid', name, {}, opts)
        child = pulumi.ResourceOptions(parent=self)
        names = scoped_names(prefix)
        self.prefix = names.prefix

        if s3_store == (cloudflare_account_id is not None):
            raise ValueError(
                'Pyramid: pass exactly one of `s3_store=True` or `cloudflare_account_id`',
            )
        if s3_store:
            self.store = S3ShardStore(
                f'{name}-store', prefix=self.prefix, bucket_name=bucket_name,
                expire_raw_after_days=expire_raw_after_days, opts=child,
            )
        else:
            if expire_raw_after_days is not None:
                raise ValueError(
                    'Pyramid: `expire_raw_after_days` is S3-only; R2 has no '
                    'lifecycle resource in the Cloudflare provider',
                )
            self.store = R2ShardStore(
                f'{name}-store', account_id=cloudflare_account_id,
                prefix=self.prefix, bucket_name=bucket_name, opts=child,
            )

        self.index = None
        if shard_index:
            if cloudflare_account_id is None:
                raise ValueError('Pyramid: `shard_index` needs `cloudflare_account_id` (D1)')
            self.index = ShardIndex(
                f'{name}-index', account_id=cloudflare_account_id,
                prefix=self.prefix, opts=child,
            )

        # Everything downstream reads the store through the same env block the
        # engine and handlers expect, so there is one spelling of these keys.
        env: dict[str, str | pulumi.Output[str]] = dict(self.store.engine_env())
        if self.index is not None:
            env['PYRMTS_D1_DATABASE_ID'] = self.index.database_id

        self.fill = None
        if fill_image_uri is not None:
            self.fill = FillFunction(
                f'{name}-fill', image_uri=fill_image_uri, prefix=self.prefix,
                env={**env, **(fill_env or {})}, memory_mb=fill_memory_mb,
                schedules=schedules, opts=child,
            )
        elif schedules:
            raise ValueError('Pyramid: `schedules` needs `fill_image_uri`')

        self.engine = None
        if engine_image_uri is not None:
            if subnet_ids is None or security_group_ids is None:
                raise ValueError(
                    'Pyramid: the batch engine needs `subnet_ids` and `security_group_ids`',
                )
            self.engine = BatchEngine(
                f'{name}-engine', image_uri=engine_image_uri, prefix=self.prefix,
                subnet_ids=subnet_ids, security_group_ids=security_group_ids,
                environment={**env, **(engine_env or {})}, opts=child,
            )

        self.register_outputs(self.stack_outputs())

    def stack_outputs(self) -> dict[str, object]:
        """The values a consumer exports and then feeds to `wrangler`, CI, and
        `pyrmts-engine batch submit` — so those never hard-code an id."""
        out: dict[str, object] = {'prefix': self.prefix, 'bucket': self.store.bucket}
        if self.index is not None:
            out['d1_database_id'] = self.index.database_id
            out['d1_database_name'] = self.index.database_name
        if self.fill is not None:
            out['fill_function'] = self.fill.function_name
        if self.engine is not None:
            out['batch_queue'] = self.engine.queue
            out['batch_job_definition'] = self.engine.job_definition
        return out
