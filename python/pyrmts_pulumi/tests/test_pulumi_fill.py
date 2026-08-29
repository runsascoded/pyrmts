"""FillFunction: the Lambda, its schedules, and the invoke-permission shape."""
from __future__ import annotations

import pulumi
import pytest
from conftest import declared, names_of

from pyrmts_pulumi import FillFunction, Schedule

FN = 'aws:lambda/function:Function'
RULE = 'aws:cloudwatch/eventRule:EventRule'
TARGET = 'aws:cloudwatch/eventTarget:EventTarget'
PERM = 'aws:lambda/permission:Permission'


@pulumi.runtime.test
def test_image_function_with_no_schedules():
    fn = FillFunction('f1', image_uri='repo:tag', prefix='alpha')
    def check(_):
        (_, inputs), = declared(FN, 'f1')
        assert [inputs['name'], inputs['packageType'], inputs['imageUri']] == [
            'alpha', 'Image', 'repo:tag',
        ]
        assert declared(RULE, 'f1') == []
        assert declared(PERM, 'f1') == []
    return pulumi.Output.all(fn.arn).apply(check)


@pulumi.runtime.test
def test_each_schedule_gets_its_own_rule_target_and_permission():
    # The `StatementId` collision that took ctbk's v6 tick down is structural
    # here: two schedules mean two Permission resources, so there is no shared
    # id to collide on.
    fn = FillFunction(
        'f2', image_uri='repo:tag', prefix='beta',
        schedules=[
            Schedule('v5-tick', 'rate(5 minutes)', input_json='{"p":"v5"}'),
            Schedule('v6-tick', 'rate(5 minutes)', input_json='{"p":"v6"}'),
        ],
    )
    def check(_):
        assert sorted(names_of(RULE, 'f2')) == ['beta-v5-tick', 'beta-v6-tick']
        assert len(declared(TARGET, 'f2')) == 2
        assert len(declared(PERM, 'f2')) == 2
        # Distinct source ARNs are what scope each permission to one rule.
        assert len({str(i.get('sourceArn')) for _, i in declared(PERM, 'f2')}) == 2
    # Depend on the permissions themselves — the function's ARN resolves
    # before the schedule resources are registered.
    return pulumi.Output.all(*[p.urn for p in fn.permissions.values()]).apply(check)


@pulumi.runtime.test
def test_a_disabled_schedule_is_declared_disabled():
    # Retirement is a declared fact, not the absence of code: the rule stays
    # in the graph as DISABLED rather than being deleted and forgotten.
    fn = FillFunction(
        'f3', image_uri='repo:tag', prefix='gamma',
        schedules=[
            Schedule('v3-tick', 'rate(1 hour)', enabled=False),
            Schedule('v6-tick', 'rate(5 minutes)'),
        ],
    )
    def check(_):
        assert sorted((i['name'], i['state']) for _, i in declared(RULE, 'f3')) == [
            ('gamma-v3-tick', 'DISABLED'),
            ('gamma-v6-tick', 'ENABLED'),
        ]
    return pulumi.Output.all(*[r.urn for r in fn.rules.values()]).apply(check)


@pulumi.runtime.test
def test_zip_package_requires_handler_and_runtime():
    def check(_):
        with pytest.raises(ValueError, match='`code` requires `handler` and `runtime`'):
            FillFunction('f4', code=pulumi.FileArchive('.'), prefix='delta')
        with pytest.raises(ValueError, match='exactly one of'):
            FillFunction('f5', prefix='delta')
        with pytest.raises(ValueError, match='exactly one of'):
            FillFunction('f6', image_uri='r:t', code=pulumi.FileArchive('.'), prefix='delta')
    return pulumi.Output.from_input(0).apply(check)


@pulumi.runtime.test
def test_supplied_role_arn_forbids_policy_attachments():
    # Silently dropping `policy_arns` would leave a function without the
    # permissions its caller asked for; failing loudly is the honest option.
    def check(_):
        with pytest.raises(ValueError, match='needs a managed role'):
            FillFunction(
                'f7', image_uri='r:t', prefix='eps',
                role_arn='arn:aws:iam::1:role/x', policy_arns=['arn:aws:iam::aws:policy/Y'],
            )
    return pulumi.Output.from_input(0).apply(check)
