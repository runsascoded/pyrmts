"""`pyrmts-ops` CLI.

Two verify-shaped command groups, both read-only and both meant for a
consumer's CI: `d1` (schema drift) and `aws` (schedule/function drift).

`d1` (`specs/d1-schema-drift.md`). pyrmts owns the
`pyramid_shards` / `pyramid_watermarks` DDL but consumers apply it, so
without these there is nothing that emits the current schema for a
migration file, and nothing that notices a deployment still running an
older one.

Credentials come from the environment `pyrmts.d1` already uses:
`CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN`, `D1_DATABASE_ID`."""
from __future__ import annotations

import json as json_mod
import sys
from functools import partial

from click import argument, group, option

from pyrmts.d1 import apply_schema, schema_sql, verify_schema
from .aws import ExpectedSchedule, verify_schedules

err = partial(print, file=sys.stderr, flush=True)


@group()
def cli() -> None:
    """Ops helpers for pyrmts pyramids."""


@cli.group()
def d1() -> None:
    """Cloudflare D1 schema: emit, verify, apply."""


def _tables(skip_inventory: bool, shards_table: str, watermarks_table: str) -> dict:
    return dict(
        skip_inventory=skip_inventory,
        shards_table=shards_table,
        watermarks_table=watermarks_table,
    )


@d1.command('schema')
@option('-i', '--skip-inventory', is_flag=True, help="Emit only the watermarks table (matches `D1ShardIndex({ skipInventory: true })`)")
@option('-s', '--shards-table', default='pyramid_shards', help="Shards table name (default `pyramid_shards`)")
@option('-w', '--watermarks-table', default='pyramid_watermarks', help="Watermarks table name (default `pyramid_watermarks`)")
def d1_schema(skip_inventory: bool, shards_table: str, watermarks_table: str) -> None:
    """Print the DDL pyrmts expects, as a runnable SQL script.

    Every statement is `IF NOT EXISTS`, so this doubles as the migration
    body — redirect it into your next numbered migration file (pyrmts
    deliberately does not own the numbering; consumers interleave their own
    tables) and apply with `wrangler d1 migrations apply`."""
    for sql in schema_sql(**_tables(skip_inventory, shards_table, watermarks_table)):
        print(f'{sql};')


@d1.command('verify')
@option('-d', '--database-id', help="D1 database id (default `$D1_DATABASE_ID`)")
@option('-i', '--skip-inventory', is_flag=True, help="Expect only the watermarks table")
@option('-j', '--json', 'as_json', is_flag=True, help="Emit the diff as JSON on stdout")
@option('-s', '--shards-table', default='pyramid_shards', help="Shards table name (default `pyramid_shards`)")
@option('-w', '--watermarks-table', default='pyramid_watermarks', help="Watermarks table name (default `pyramid_watermarks`)")
def d1_verify(
    database_id: str | None,
    skip_inventory: bool,
    as_json: bool,
    shards_table: str,
    watermarks_table: str,
) -> None:
    """Diff a live D1 database against the expected schema (read-only).

    Exits 1 when the database is missing an object or has one with the
    wrong columns — so it works as a deploy-time or CI gate."""
    kwargs = _tables(skip_inventory, shards_table, watermarks_table)
    if database_id is not None:
        kwargs['database_id'] = database_id
    diff = verify_schema(**kwargs)
    if as_json:
        print(json_mod.dumps({
            'ok': diff.ok,
            'missing': list(diff.missing),
            'mismatched': list(diff.mismatched),
        }))
    else:
        print(diff.summary())
    if not diff.ok:
        raise SystemExit(1)


@d1.command('apply')
@option('-d', '--database-id', help="D1 database id (default `$D1_DATABASE_ID`)")
@option('-i', '--skip-inventory', is_flag=True, help="Apply only the watermarks table")
@option('-n', '--dry-run', is_flag=True, help="Print the statements that would run, without running them")
@option('-s', '--shards-table', default='pyramid_shards', help="Shards table name (default `pyramid_shards`)")
@option('-w', '--watermarks-table', default='pyramid_watermarks', help="Watermarks table name (default `pyramid_watermarks`)")
def d1_apply(
    database_id: str | None,
    skip_inventory: bool,
    dry_run: bool,
    shards_table: str,
    watermarks_table: str,
) -> None:
    """Apply the expected schema to a live D1 database.

    Idempotent (every statement is `IF NOT EXISTS`), so re-running against a
    current database is a no-op. Note a `CREATE INDEX` on a populated table
    builds in one pass and D1 bills the build as row writes."""
    kwargs = _tables(skip_inventory, shards_table, watermarks_table)
    if dry_run:
        for sql in schema_sql(**kwargs):
            err(f'would apply: {sql.splitlines()[0]}…' if '\n' in sql else f'would apply: {sql}')
        return
    if database_id is not None:
        kwargs['database_id'] = database_id
    for sql in apply_schema(**kwargs):
        err(f'applied: {sql.splitlines()[0]}…' if '\n' in sql else f'applied: {sql}')


@cli.group()
def aws() -> None:
    """AWS schedule/function drift: read-only checks."""


def _parse_binding(spec: str, disabled: set[str]) -> ExpectedSchedule:
    rule, sep, rest = spec.partition('=')
    if not sep or not rule or not rest:
        raise SystemExit(f'aws verify: expected RULE=FUNCTION[@SCHEDULE], got {spec!r}')
    function, _, schedule = rest.partition('@')
    if not function:
        raise SystemExit(f'aws verify: no function in {spec!r}')
    return ExpectedSchedule(
        rule=rule,
        function=function,
        enabled=rule not in disabled,
        schedule=schedule or None,
    )


@aws.command('verify')
@option('-d', '--disabled', 'disabled_rules', multiple=True, help="Rule expected DISABLED — a retired tick that must stay retired (repeatable)")
@option('-j', '--json', 'as_json', is_flag=True, help="Emit the diff as JSON on stdout")
@argument('bindings', nargs=-1, required=True)
def aws_verify(disabled_rules: tuple[str, ...], as_json: bool, bindings: tuple[str, ...]) -> None:
    """Check each RULE=FUNCTION[@SCHEDULE] binding against the live account.

    Read-only — needs only `events:DescribeRule`,
    `events:ListTargetsByRule`, `lambda:GetFunction`, `lambda:GetPolicy`.
    Exits 1 on any drift, so it can gate a deploy the way
    `pyrmts-ops d1 verify` does.

    \b
    Checks per binding: the rule exists; its state matches (ENABLED unless
    named in -d); its schedule expression matches, when one is given; it
    targets FUNCTION; FUNCTION exists; and FUNCTION grants that rule invoke
    permission — the last being the one a console reads as healthy.

    \b
    pyrmts-ops aws verify \\
      avail-v6-tick=ctbk-gbfs-fill@'rate(5 minutes)' \\
      avail-v3-tick=ctbk-gbfs-fill -d avail-v3-tick
    """
    disabled = set(disabled_rules)
    expected = [_parse_binding(b, disabled) for b in bindings]
    unknown = disabled - {e.rule for e in expected}
    if unknown:
        raise SystemExit(
            f"aws verify: -d names rules with no binding: {', '.join(sorted(unknown))}",
        )
    diff = verify_schedules(expected)
    if as_json:
        print(json_mod.dumps(
            {'ok': diff.ok, 'missing': list(diff.missing), 'mismatched': list(diff.mismatched)},
            indent=2,
        ))
    else:
        # stdout, matching `d1 verify` — the summary is the command's result,
        # not a log line.
        print(diff.summary())
    if not diff.ok:
        raise SystemExit(1)
