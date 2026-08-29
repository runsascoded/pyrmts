"""`pyrmts_ops.aws`: Lambda bundle building (no AWS calls) + the
EventBridge schedule upsert (recorded-call fakes, exact call shapes)."""
from __future__ import annotations

import io
import json
import zipfile

import pytest

from pyrmts_ops.aws import (
    ExpectedSchedule,
    build_zip,
    find_site_packages,
    upsert_schedule,
    verify_schedules,
)


def test_build_zip_entries_and_vendoring(tmp_path):
    handler = tmp_path / 'handler.py'
    handler.write_text('def lambda_handler(event, context): ...\n')
    sp = tmp_path / 'site-packages'
    pkg = sp / 'somepkg'
    (pkg / 'sub').mkdir(parents=True)
    (pkg / '__init__.py').write_text('x = 1\n')
    (pkg / 'sub' / 'mod.py').write_text('y = 2\n')
    (pkg / 'native.so').write_bytes(b'\x7fELF')          # excluded
    (pkg / '__pycache__').mkdir()
    (pkg / '__pycache__' / 'mod.cpython-312.pyc').write_bytes(b'junk')  # excluded

    blob = build_zip(
        {
            'handler.py': handler,
            'cfg.yaml': b'binCol: ts\n',
            'app/__init__.py': b'',
        },
        vendor=['somepkg'],
        site_packages=sp,
    )
    z = zipfile.ZipFile(io.BytesIO(blob))
    assert sorted(z.namelist()) == [
        'app/__init__.py',
        'cfg.yaml',
        'handler.py',
        'somepkg/__init__.py',
        'somepkg/sub/mod.py',
    ]
    assert z.read('cfg.yaml') == b'binCol: ts\n'

    with pytest.raises(FileNotFoundError) as exc:
        build_zip({}, vendor=['nope'], site_packages=sp)
    assert str(exc.value) == f'nope not found in {sp}'
    with pytest.raises(ValueError) as exc2:
        build_zip({}, vendor=['somepkg'])
    assert str(exc2.value) == 'build_zip: vendor packages need site_packages'


def test_find_site_packages_versioned_layout(tmp_path):
    sp = tmp_path / '3.13.7' / 'lib' / 'python3.13' / 'site-packages'
    sp.mkdir(parents=True)
    assert find_site_packages(tmp_path) == sp
    with pytest.raises(FileNotFoundError):
        find_site_packages(tmp_path / 'nope')


class FakeLam:
    """Recorded-call fake of the boto3 lambda client surface
    `upsert_lambda_function` touches."""

    class exceptions:
        class ResourceNotFoundException(Exception):
            pass

        class ResourceConflictException(Exception):
            pass

    def __init__(self, existing: dict[str, dict] | None = None) -> None:
        self.fns: dict[str, dict] = dict(existing or {})
        self.calls: list[tuple] = []

    def get_function(self, FunctionName):
        self.calls.append(('get_function', FunctionName))
        if FunctionName not in self.fns:
            raise self.exceptions.ResourceNotFoundException()
        return {'Configuration': self.fns[FunctionName]}

    def create_function(self, **kw):
        self.calls.append(('create_function', kw))
        self.fns[kw['FunctionName']] = {
            'PackageType': kw.get('PackageType', 'Zip'),
            'FunctionArn': f"arn:{kw['FunctionName']}",
        }

    def update_function_code(self, **kw):
        self.calls.append(('update_function_code', kw))

    def update_function_configuration(self, **kw):
        self.calls.append(('update_function_configuration', kw))

    def delete_function(self, FunctionName):
        self.calls.append(('delete_function', FunctionName))
        del self.fns[FunctionName]

    def get_waiter(self, name):
        class W:
            def wait(self, **kw):
                pass
        return W()

    def put_function_concurrency(self, **kw):
        self.calls.append(('put_function_concurrency', kw))

    def delete_function_concurrency(self, **kw):
        self.calls.append(('delete_function_concurrency', kw))


def test_upsert_image_function_create():
    from pyrmts_ops.aws import upsert_lambda_function
    lam = FakeLam()
    arn = upsert_lambda_function(
        'fn',
        role_arn='arn:role', memory_mb=5376, env={'A': '1'},
        image_uri='acct.dkr.ecr.us-east-1.amazonaws.com/pyr:rev',
        description='d', reserved=1, client=lam,
    )
    assert arn == 'arn:fn'
    # Image creates carry PackageType + ImageUri and NO Runtime/Handler/Layers.
    assert lam.calls[1] == ('create_function', {
        'FunctionName': 'fn',
        'Code': {'ImageUri': 'acct.dkr.ecr.us-east-1.amazonaws.com/pyr:rev'},
        'PackageType': 'Image',
        'Role': 'arn:role',
        'Timeout': 900,
        'MemorySize': 5376,
        'Environment': {'Variables': {'A': '1'}},
        'Description': 'd',
    })
    assert lam.calls[2] == ('put_function_concurrency', {
        'FunctionName': 'fn', 'ReservedConcurrentExecutions': 1,
    })


def test_upsert_zip_function_update():
    from pyrmts_ops.aws import upsert_lambda_function
    lam = FakeLam({'fn': {'PackageType': 'Zip', 'FunctionArn': 'arn:fn'}})
    upsert_lambda_function(
        'fn',
        role_arn='arn:role', memory_mb=1024, env={},
        zip_blob=b'zipbytes', layers=['arn:layer'], client=lam,
    )
    assert lam.calls[1] == ('update_function_code', {'FunctionName': 'fn', 'ZipFile': b'zipbytes'})
    assert lam.calls[2] == ('update_function_configuration', {
        'FunctionName': 'fn',
        'Role': 'arn:role',
        'Timeout': 900,
        'MemorySize': 1024,
        'Environment': {'Variables': {}},
        'Description': '',
        'Runtime': 'python3.12',
        'Handler': 'handler.lambda_handler',
        'Layers': ['arn:layer'],
    })


def test_upsert_package_type_flip_recreates():
    """zip → image can't be updated in place: the function is deleted and
    recreated as an image function."""
    from pyrmts_ops.aws import upsert_lambda_function
    lam = FakeLam({'fn': {'PackageType': 'Zip', 'FunctionArn': 'arn:fn'}})
    upsert_lambda_function(
        'fn',
        role_arn='arn:role', memory_mb=5376, env={},
        image_uri='img:rev', client=lam,
    )
    kinds = [c[0] for c in lam.calls]
    assert kinds == [
        'get_function',                  # existing: Zip
        'delete_function',
        'get_function',                  # gone-poll (raises → done)
        'create_function',
        'delete_function_concurrency',
        'get_function',                  # final ARN read
    ]
    create = dict(lam.calls[3][1])
    assert (create['PackageType'], create['Code']) == ('Image', {'ImageUri': 'img:rev'})


def test_upsert_requires_exactly_one_package():
    from pyrmts_ops.aws import upsert_lambda_function
    with pytest.raises(ValueError) as exc:
        upsert_lambda_function('fn', role_arn='r', memory_mb=1, env={}, client=FakeLam())
    assert str(exc.value) == 'upsert_lambda_function: exactly one of zip_blob / image_uri'


class FakeEvents:
    """Recorded-call fake of the boto3 `events` client surface
    `upsert_schedule` touches. `rules` maps rule name → its live State, so a
    test can start from "an operator disabled this"."""

    class exceptions:
        class ResourceNotFoundException(Exception):
            pass

    def __init__(self, rules: dict[str, str] | None = None) -> None:
        self.rules: dict[str, str] = dict(rules or {})
        self.calls: list[tuple] = []

    def describe_rule(self, Name):
        self.calls.append(('describe_rule', Name))
        if Name not in self.rules:
            raise self.exceptions.ResourceNotFoundException(Name)
        return {'Name': Name, 'State': self.rules[Name]}

    def put_rule(self, **kw):
        self.calls.append(('put_rule', kw))
        self.rules[kw['Name']] = kw['State']
        return {'RuleArn': f'arn:aws:events:::rule/{kw["Name"]}'}

    def put_targets(self, **kw):
        self.calls.append(('put_targets', kw))
        return {'FailedEntryCount': 0}


class FakePermLam:
    """Just the `add_permission` surface, with the real conflict semantics:
    a StatementId already used on the function raises."""

    class exceptions:
        class ResourceConflictException(Exception):
            pass

    def __init__(self) -> None:
        self.statements: set[str] = set()
        self.calls: list[tuple] = []

    def add_permission(self, **kw):
        self.calls.append(('add_permission', kw))
        sid = kw['StatementId']
        if sid in self.statements:
            raise self.exceptions.ResourceConflictException(sid)
        self.statements.add(sid)
        return {'Statement': sid}


def test_upsert_schedule_creates_a_new_rule_enabled():
    events, lam = FakeEvents(), FakePermLam()
    upsert_schedule('t-tick', 'rate(5 minutes)', 'arn:fn', 'fn', events=events, lam=lam)
    assert events.calls == [
        ('describe_rule', 't-tick'),
        ('put_rule', {
            'Name': 't-tick', 'ScheduleExpression': 'rate(5 minutes)',
            'State': 'ENABLED', 'Description': '',
        }),
        ('put_targets', {'Rule': 't-tick', 'Targets': [{'Id': 'fn', 'Arn': 'arn:fn'}]}),
    ]


def test_upsert_schedule_preserves_a_disabled_rule():
    """Retiring a pyramid means disabling its tick. Under the old forced
    `State='ENABLED'`, the next deploy silently turned it back on — so the
    only way to make a retirement stick was to delete the calling code."""
    events, lam = FakeEvents({'t-tick': 'DISABLED'}), FakePermLam()
    upsert_schedule('t-tick', 'rate(5 minutes)', 'arn:fn', 'fn', events=events, lam=lam)
    assert events.calls[1] == ('put_rule', {
        'Name': 't-tick', 'ScheduleExpression': 'rate(5 minutes)',
        'State': 'DISABLED', 'Description': '',
    })
    assert events.rules == {'t-tick': 'DISABLED'}


def test_upsert_schedule_enabled_true_re_enables_a_disabled_rule():
    events, lam = FakeEvents({'t-tick': 'DISABLED'}), FakePermLam()
    upsert_schedule(
        't-tick', 'rate(5 minutes)', 'arn:fn', 'fn',
        enabled=True, events=events, lam=lam,
    )
    # An explicit state skips the describe entirely.
    assert [c[0] for c in events.calls] == ['put_rule', 'put_targets']
    assert events.rules == {'t-tick': 'ENABLED'}


def test_upsert_schedule_enabled_false_disables_an_enabled_rule():
    events, lam = FakeEvents({'t-tick': 'ENABLED'}), FakePermLam()
    upsert_schedule(
        't-tick', 'rate(5 minutes)', 'arn:fn', 'fn',
        enabled=False, events=events, lam=lam,
    )
    assert events.rules == {'t-tick': 'DISABLED'}


def test_upsert_schedule_input_json_rides_on_the_target():
    events, lam = FakeEvents(), FakePermLam()
    upsert_schedule(
        'v6-tick', 'cron(4/5 * * * ? *)', 'arn:fn', 'fn',
        input_json='{"config": "avail-v6"}', events=events, lam=lam,
    )
    assert events.calls[2] == ('put_targets', {
        'Rule': 'v6-tick',
        'Targets': [{'Id': 'fn', 'Arn': 'arn:fn', 'Input': '{"config": "avail-v6"}'}],
    })


def test_two_rules_on_one_function_each_get_their_own_invoke_permission():
    """The `avail-v6` outage shape: with a constant StatementId the second
    rule's `add_permission` conflicts with the first rule's statement, the
    swallow hides it, and the rule fires into a function that rejects it.
    Per-rule ids mean both rules end up actually permitted."""
    events, lam = FakeEvents(), FakePermLam()
    upsert_schedule('v5-tick', 'cron(3/5 * * * ? *)', 'arn:fn', 'fn', events=events, lam=lam)
    upsert_schedule('v6-tick', 'cron(4/5 * * * ? *)', 'arn:fn', 'fn', events=events, lam=lam)
    assert [c[1]['StatementId'] for c in lam.calls] == ['invoke-v5-tick', 'invoke-v6-tick']
    assert lam.statements == {'invoke-v5-tick', 'invoke-v6-tick'}
    assert [c[1]['SourceArn'] for c in lam.calls] == [
        'arn:aws:events:::rule/v5-tick',
        'arn:aws:events:::rule/v6-tick',
    ]


def test_re_running_the_same_schedule_swallows_the_permission_conflict():
    """Idempotence still holds for a genuine repeat — that swallow is
    correct, it was only the shared id that made it hide a real failure."""
    events, lam = FakeEvents(), FakePermLam()
    for _ in range(2):
        upsert_schedule('t-tick', 'rate(5 minutes)', 'arn:fn', 'fn', events=events, lam=lam)
    assert lam.statements == {'invoke-t-tick'}
    assert len(lam.calls) == 2


# -- verify_schedules -------------------------------------------------------

class FakeVerifyEvents:
    """Read-only `events` surface: rules with state/expression, and targets."""

    class exceptions:
        class ResourceNotFoundException(Exception):
            pass

    def __init__(self, rules: dict[str, dict] | None = None) -> None:
        self.rules = dict(rules or {})
        self.calls: list[tuple] = []

    def describe_rule(self, Name):
        self.calls.append(('describe_rule', Name))
        if Name not in self.rules:
            raise self.exceptions.ResourceNotFoundException(Name)
        r = self.rules[Name]
        out = {'Name': Name, 'State': r.get('state', 'ENABLED')}
        if 'schedule' in r:
            out['ScheduleExpression'] = r['schedule']
        return out

    def list_targets_by_rule(self, Rule):
        self.calls.append(('list_targets_by_rule', Rule))
        targets = self.rules.get(Rule, {}).get('targets', [])
        return {'Targets': [
            {'Id': str(i), 'Arn': f'arn:aws:lambda:us-east-1:1:function:{fn}'}
            for i, fn in enumerate(targets)
        ]}


class FakeVerifyLam:
    """Read-only `lambda` surface: which functions exist, and which rules each
    grants invoke permission to."""

    class exceptions:
        class ResourceNotFoundException(Exception):
            pass

    def __init__(self, functions: dict[str, list[str] | None] | None = None) -> None:
        # name -> list of rules granted, or None for "no resource policy"
        self.functions = dict(functions or {})
        self.calls: list[tuple] = []

    def get_function(self, FunctionName):
        self.calls.append(('get_function', FunctionName))
        if FunctionName not in self.functions:
            raise self.exceptions.ResourceNotFoundException(FunctionName)
        return {'Configuration': {'FunctionName': FunctionName}}

    def get_policy(self, FunctionName):
        self.calls.append(('get_policy', FunctionName))
        granted = self.functions.get(FunctionName)
        if granted is None:
            raise self.exceptions.ResourceNotFoundException(FunctionName)
        return {'Policy': json.dumps({'Statement': [
            {
                'Sid': f'invoke-{r}',
                'Condition': {'ArnLike': {
                    'AWS:SourceArn': f'arn:aws:events:us-east-1:1:rule/{r}',
                }},
            }
            for r in granted
        ]})}


def _healthy():
    return (
        FakeVerifyEvents({
            'avail-v6-tick': {'targets': ['gbfs-fill'], 'schedule': 'rate(5 minutes)'},
            'avail-v3-tick': {'state': 'DISABLED', 'targets': ['gbfs-fill']},
        }),
        FakeVerifyLam({'gbfs-fill': ['avail-v6-tick', 'avail-v3-tick']}),
    )


def test_verify_schedules_clean():
    events, lam = _healthy()
    diff = verify_schedules(
        [
            ExpectedSchedule('avail-v6-tick', 'gbfs-fill', schedule='rate(5 minutes)'),
            ExpectedSchedule('avail-v3-tick', 'gbfs-fill', enabled=False),
        ],
        events=events, lam=lam,
    )
    assert (diff.missing, diff.mismatched, diff.ok, diff.summary()) == (
        (), (), True, 'schedules up to date',
    )


def test_verify_schedules_catches_the_missing_invoke_permission():
    # The v6 outage, exactly: the rule exists, is ENABLED, and its target
    # resolves — everything the console shows is healthy — but the function
    # grants no permission for it, so every tick silently fails.
    events = FakeVerifyEvents({'avail-v6-tick': {'targets': ['gbfs-fill']}})
    lam = FakeVerifyLam({'gbfs-fill': ['avail-v5-tick']})  # only the *first* rule
    diff = verify_schedules([ExpectedSchedule('avail-v6-tick', 'gbfs-fill')], events=events, lam=lam)
    assert (diff.missing, diff.mismatched) == (
        ('permission avail-v6-tick -> gbfs-fill',), (),
    )
    assert diff.summary() == 'missing: permission avail-v6-tick -> gbfs-fill'


def test_verify_schedules_catches_a_function_with_no_policy_at_all():
    events = FakeVerifyEvents({'tick': {'targets': ['fn']}})
    lam = FakeVerifyLam({'fn': None})
    diff = verify_schedules([ExpectedSchedule('tick', 'fn')], events=events, lam=lam)
    assert diff.missing == ('permission tick -> fn',)


def test_verify_schedules_catches_a_resurrected_retired_tick():
    # A forced-ENABLED upsert re-enabling a retirement is the failure
    # `531a3f8` fixed; this is what notices if it ever regresses.
    events, lam = _healthy()
    events.rules['avail-v3-tick']['state'] = 'ENABLED'
    diff = verify_schedules(
        [ExpectedSchedule('avail-v3-tick', 'gbfs-fill', enabled=False)],
        events=events, lam=lam,
    )
    assert diff.mismatched == ('rule avail-v3-tick: expected=DISABLED actual=ENABLED',)


def test_verify_schedules_reports_absent_rule_and_function():
    events = FakeVerifyEvents({})
    lam = FakeVerifyLam({})
    diff = verify_schedules(
        [ExpectedSchedule('gone-tick', 'gone-fn')], events=events, lam=lam,
    )
    assert (diff.missing, diff.mismatched) == (('rule gone-tick',), ())
    # The rule is absent, so nothing downstream of it is probed.
    assert lam.calls == []


def test_verify_schedules_distinguishes_wrong_target_from_no_target():
    events = FakeVerifyEvents({
        'wrong': {'targets': ['other-fn']},
        'none': {'targets': []},
    })
    lam = FakeVerifyLam({'fn': ['wrong', 'none'], 'other-fn': []})
    diff = verify_schedules(
        [ExpectedSchedule('wrong', 'fn'), ExpectedSchedule('none', 'fn')],
        events=events, lam=lam,
    )
    assert (diff.mismatched, diff.missing) == (
        ("target wrong: expected=fn actual=['other-fn']",),
        ('target none -> fn',),
    )


def test_verify_schedules_checks_the_schedule_expression_only_when_asserted():
    events, lam = _healthy()
    asserted = verify_schedules(
        [ExpectedSchedule('avail-v6-tick', 'gbfs-fill', schedule='rate(1 hour)')],
        events=events, lam=lam,
    )
    assert asserted.mismatched == (
        "rule avail-v6-tick: expected='rate(1 hour)' actual='rate(5 minutes)'",
    )
    unasserted = verify_schedules(
        [ExpectedSchedule('avail-v6-tick', 'gbfs-fill')], events=events, lam=lam,
    )
    assert unasserted.ok


def test_verify_schedules_probes_each_function_once():
    events, lam = _healthy()
    verify_schedules(
        [
            ExpectedSchedule('avail-v6-tick', 'gbfs-fill'),
            ExpectedSchedule('avail-v3-tick', 'gbfs-fill', enabled=False),
        ],
        events=events, lam=lam,
    )
    assert lam.calls == [('get_function', 'gbfs-fill'), ('get_policy', 'gbfs-fill')]


def test_verify_schedules_accepts_a_wildcard_source_arn():
    # `add_permission` may store the source ARN with a trailing wildcard.
    events = FakeVerifyEvents({'tick': {'targets': ['fn']}})
    lam = FakeVerifyLam({'fn': []})
    lam.functions['fn'] = ['tick']
    diff = verify_schedules([ExpectedSchedule('tick', 'fn')], events=events, lam=lam)
    assert diff.ok
