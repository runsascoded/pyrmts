"""Deployment-scoped naming.

Every AWS and Cloudflare resource a pyramid needs lives in a namespace that
is **global to the account** — Batch job definitions, queues, and compute
environments; IAM roles; CloudWatch log groups; S3 and R2 bucket names; D1
database names. Two pyrmts deployments in one account therefore collide on
every one of them unless something distinguishes them.

The imperative path (`pyrmts_engine.batch`) distinguishes them with an
explicit `prefix` that defaults to a *shared* constant, so isolation there
is opt-in and a forgotten flag silently clobbers a sibling deployment. This
package removes that failure mode: `default_prefix()` derives the prefix
from the Pulumi project and stack, which are distinct for every deployment
by construction. You have to work at it to make two stacks collide."""
from __future__ import annotations

import re

import pulumi
from pyrmts_engine.batch import Names, resource_names

__all__ = ['Names', 'default_prefix', 'resource_names', 'scoped_names']

# Batch/IAM names allow alphanumerics, hyphen, underscore; bucket and D1
# names are stricter still. Lowercase alnum + hyphen is the safe intersection.
_UNSAFE = re.compile(r'[^a-z0-9-]+')


def default_prefix() -> str:
    """`<project>-<stack>`, sanitized — unique per deployment.

    Pulumi guarantees a stack is identified by (project, stack), so this is
    distinct for every deployment without the operator having to think about
    it. Pass an explicit `prefix` to override (e.g. to adopt names that
    already exist in the account, or to keep a name stable across a project
    rename)."""
    return sanitize(f'{pulumi.get_project()}-{pulumi.get_stack()}')


def sanitize(raw: str) -> str:
    """Lowercase `raw` and reduce it to the character set every provider we
    target accepts, so one prefix can name a Batch queue, an IAM role, a
    bucket, and a D1 database."""
    out = _UNSAFE.sub('-', raw.lower()).strip('-')
    if not out:
        raise ValueError(f'prefix {raw!r} has no usable characters')
    return out


def scoped_names(prefix: str | None) -> Names:
    """`resource_names` over `prefix`, defaulting to `default_prefix()`.

    Shares `pyrmts_engine.batch.resource_names` rather than re-deriving, so
    a stack stood up by this package is addressable by the runtime CLI:
    `pyrmts-engine batch submit -p <prefix>` resolves to the same queue, job
    definition, and log group Pulumi created."""
    return resource_names(sanitize(prefix) if prefix is not None else default_prefix())
