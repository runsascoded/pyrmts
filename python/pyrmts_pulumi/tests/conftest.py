"""Pulumi mock harness.

`pulumi.runtime.set_mocks` stands in for the engine, so these tests assert on
the resource graph a component *declares* — names, inputs, parent/child
structure — with no cloud account and no `pulumi up`."""
from __future__ import annotations

# Pulumi 3.260 still calls `asyncio.get_event_loop()` at import time, which
# raises on 3.12+ when no loop is running (it became an error in 3.14).
# Installing one before importing pulumi is the whole fix.
import asyncio

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import pulumi

PROJECT = 'proj'
STACK = 'stack'


# Every resource the mocks are asked to create, in declaration order. Tests
# assert on this: the point of a ComponentResource is the graph it declares,
# and that graph is only observable here.
DECLARED: list[tuple[str, str, dict]] = []


class Mocks(pulumi.runtime.Mocks):
    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        DECLARED.append((args.typ, args.name, dict(args.inputs)))
        # Echo inputs back as state, plus the derived fields real providers
        # compute, so components that read `.arn` / `.name` resolve.
        state = dict(args.inputs)
        state.setdefault('name', args.name)
        state.setdefault('arn', f'arn:aws:mock:::{args.name}')
        if args.typ == 'aws:s3/bucketV2:BucketV2':
            state.setdefault('bucket', args.name)
        return [f'{args.name}-id', state]

    def call(self, args: pulumi.runtime.MockCallArgs):
        return {}


def declared(typ: str, under: str | None = None) -> list[tuple[str, dict]]:
    """(logical name, inputs) for declared resources of type `typ`.

    `under` filters to one component's children by logical-name prefix. Pass
    it whenever a test could see another's resources: Pulumi's runtime is
    process-global and registers asynchronously, so resources from a previous
    test can still land after this one has cleared the registry."""
    return [
        (name, inputs) for t, name, inputs in DECLARED
        if t == typ and (under is None or name.startswith(under))
    ]


def names_of(typ: str, under: str | None = None) -> list[str]:
    """The *physical* names declared for `typ` — what actually collides in an
    AWS account, as opposed to the Pulumi logical name."""
    return [i.get('name') for _, i in declared(typ, under)]


pulumi.runtime.set_mocks(Mocks(), project=PROJECT, stack=STACK, preview=False)


import pytest


@pytest.fixture(autouse=True)
def _reset_declared():
    """Each test asserts on the graph *it* declares, not the accumulation."""
    DECLARED.clear()
    yield
    DECLARED.clear()
