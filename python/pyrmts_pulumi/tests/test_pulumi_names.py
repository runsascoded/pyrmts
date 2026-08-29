"""Deployment isolation: the property the whole package exists to guarantee."""
from __future__ import annotations

import pytest
from conftest import PROJECT, STACK

from pyrmts_engine.batch import PREFIX
from pyrmts_pulumi import Names, default_prefix, sanitize, scoped_names


def test_default_prefix_is_the_project_and_stack():
    assert default_prefix() == f'{PROJECT}-{STACK}'


def test_default_prefix_is_not_the_shared_imperative_default():
    # `pyrmts_engine.batch` defaults to a constant every consumer shares, so
    # two deployments that both take the default clobber each other. This
    # package must never inherit that default.
    assert default_prefix() != PREFIX


def test_scoped_names_defaults_to_the_deployment_prefix():
    assert scoped_names(None) == Names(
        prefix='proj-stack',
        job_definition='proj-stack',
        execution_role='proj-stack-batch-execution',
        log_group='/proj-stack/batch',
        spot_ce='proj-stack-spot',
        od_ce='proj-stack-od',
        spot_queue='proj-stack',
        od_queue='proj-stack-od',
    )


def test_scoped_names_matches_the_runtime_resolver():
    # The handoff that makes a Pulumi-created stack addressable from the CLI:
    # `pyrmts-engine batch submit -p <prefix>` must resolve to the same names.
    from pyrmts_engine.batch import resource_names
    assert scoped_names('ctbk-gbfs') == resource_names('ctbk-gbfs')


@pytest.mark.parametrize('raw, want', [
    ('ctbk-gbfs', 'ctbk-gbfs'),
    ('CTBK_GBFS', 'ctbk-gbfs'),
    ('awair.prod', 'awair-prod'),
    ('--lead-and-trail--', 'lead-and-trail'),
    ('a  b', 'a-b'),
])
def test_sanitize(raw: str, want: str):
    assert sanitize(raw) == want


def test_sanitize_rejects_an_empty_result():
    with pytest.raises(ValueError, match='no usable characters'):
        sanitize('***')
