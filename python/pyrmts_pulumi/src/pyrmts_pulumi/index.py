"""Shard index: the D1 database holding watermarks and the shard inventory.

Scope boundary, deliberate: this component creates the **database**, not its
**schema**. The tables pyrmts owns are emitted and reconciled by
`pyrmts-ops d1 {schema,verify,apply}`, which a consumer runs from its deploy
pipeline — the same place its own migrations run, and the only place that
knows the migration numbering (consumers interleave their own tables, so
pyrmts must not own the sequence).

Two reasons not to fold the schema in here. Pulumi has no first-class way to
express "apply these statements, in order, once" without a dynamic provider
that re-runs on every `up`; and a schema applied at `pulumi up` time drifts
from an app deployed separately. Detection is the useful half anyway — wire
`pyrmts-ops d1 verify` into CI and a schema gap fails the build."""
from __future__ import annotations

import pulumi

from ._names import scoped_names


class ShardIndex(pulumi.ComponentResource):
    """A Cloudflare D1 database for a pyramid's watermarks + shard inventory."""

    database_id: pulumi.Output[str]
    database_name: pulumi.Output[str]
    account_id: pulumi.Output[str]

    def __init__(
        self,
        name: str,
        *,
        account_id: str | pulumi.Output[str],
        prefix: str | None = None,
        database_name: str | pulumi.Output[str] | None = None,
        primary_location_hint: str | None = None,
        read_replication: bool | None = None,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__('pyrmts:index:ShardIndex', name, {}, opts)
        import pulumi_cloudflare as cloudflare

        names = scoped_names(prefix)
        kwargs: dict = {}
        if read_replication is not None:
            kwargs['read_replication'] = cloudflare.D1DatabaseReadReplicationArgs(
                mode='auto' if read_replication else 'disabled',
            )
        db = cloudflare.D1Database(
            f'{name}-db',
            account_id=account_id,
            name=database_name if database_name is not None else names.prefix,
            primary_location_hint=primary_location_hint,
            opts=pulumi.ResourceOptions(parent=self),
            **kwargs,
        )
        self.database_id = db.id
        self.database_name = db.name
        self.account_id = pulumi.Output.from_input(account_id)
        self.register_outputs({
            'database_id': self.database_id,
            'database_name': self.database_name,
        })

    def wrangler_binding(self, binding: str = 'DB') -> pulumi.Output[dict]:
        """The `[[d1_databases]]` block for a consumer's `wrangler.toml`.

        Emitting it from the stack is the fix for the id-pasted-into-N-files
        problem: the id has exactly one source, and a worker config generated
        from this output cannot disagree with the database Pulumi created."""
        return pulumi.Output.all(self.database_id, self.database_name).apply(
            lambda v: {'binding': binding, 'database_name': v[1], 'database_id': v[0]},
        )
