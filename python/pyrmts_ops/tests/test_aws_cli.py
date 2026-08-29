"""`pyrmts-ops aws verify` — argument parsing and exit codes."""
from __future__ import annotations

from click.testing import CliRunner

from pyrmts_ops.aws import ExpectedSchedule, ScheduleDiff
from pyrmts_ops.cli import _parse_binding, cli


def test_parse_binding_forms():
    assert [
        _parse_binding('tick=fn', set()),
        _parse_binding('tick=fn@rate(5 minutes)', set()),
        _parse_binding('old=fn', {'old'}),
    ] == [
        ExpectedSchedule('tick', 'fn', enabled=True, schedule=None),
        ExpectedSchedule('tick', 'fn', enabled=True, schedule='rate(5 minutes)'),
        ExpectedSchedule('old', 'fn', enabled=False, schedule=None),
    ]


def test_parse_binding_rejects_malformed():
    runner = CliRunner()
    for bad, want in (
        ('tick', "aws verify: expected RULE=FUNCTION[@SCHEDULE], got 'tick'"),
        ('tick=', "aws verify: expected RULE=FUNCTION[@SCHEDULE], got 'tick='"),
        ('=fn', "aws verify: expected RULE=FUNCTION[@SCHEDULE], got '=fn'"),
        ('tick=@rate(1 hour)', "aws verify: no function in 'tick=@rate(1 hour)'"),
    ):
        result = runner.invoke(cli, ['aws', 'verify', bad])
        assert (result.exit_code, str(result.exception)) == (1, want)


def test_disabled_naming_an_unbound_rule_is_an_error():
    result = CliRunner().invoke(cli, ['aws', 'verify', 'tick=fn', '-d', 'typo'])
    assert (result.exit_code, str(result.exception)) == (
        1, 'aws verify: -d names rules with no binding: typo',
    )


def _run(monkeypatch, diff: ScheduleDiff, args: list[str]):
    seen: list = []

    def fake(expected, **kwargs):
        seen.append(list(expected))
        return diff

    monkeypatch.setattr('pyrmts_ops.cli.verify_schedules', fake)
    return CliRunner().invoke(cli, ['aws', 'verify', *args]), seen


def test_clean_run_exits_zero(monkeypatch):
    result, seen = _run(monkeypatch, ScheduleDiff(), ['tick=fn'])
    assert (result.exit_code, seen) == (0, [[ExpectedSchedule('tick', 'fn')]])


def test_drift_exits_one_and_reports(monkeypatch):
    result, _ = _run(
        monkeypatch,
        ScheduleDiff(missing=('permission tick -> fn',)),
        ['tick=fn'],
    )
    assert result.exit_code == 1
    assert result.output.rstrip().split('\n') == ['missing: permission tick -> fn']


def test_json_output(monkeypatch):
    import json
    result, _ = _run(
        monkeypatch,
        ScheduleDiff(missing=('rule a',), mismatched=('rule b: expected=X actual=Y',)),
        ['a=fn', 'b=fn', '-j'],
    )
    assert (result.exit_code, json.loads(result.output)) == (1, {
        'ok': False,
        'missing': ['rule a'],
        'mismatched': ['rule b: expected=X actual=Y'],
    })
