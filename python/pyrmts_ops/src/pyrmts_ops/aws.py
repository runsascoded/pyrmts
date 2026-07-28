"""Lambda bundle + deploy skeleton (`specs/pyrmts-ops-adoption.md` phase 3
— absorbed from ctbk `gbfs/lambda/deploy.py`). Plain boto3, no CDK:
build a zip from vendored pure-python site-packages + consumer files,
then upsert the IAM role, function, and EventBridge schedule. Idempotent
— re-run to redeploy code or config. Consumer deploy scripts shrink to
config invocations of `deploy_pyramid_lambda`."""
from __future__ import annotations

import io
import json
import sys
import time
import zipfile
from functools import partial
from pathlib import Path
from typing import Mapping, Sequence

err = partial(print, file=sys.stderr, flush=True)


def find_site_packages(venv_root: Path) -> Path:
    """The venv's site-packages (versioned-venv layouts first)."""
    hits = sorted(venv_root.glob('*/lib/python3.*/site-packages'))
    if not hits:
        hits = sorted(venv_root.glob('lib/python3.*/site-packages'))
    if not hits:
        raise FileNotFoundError(f'no site-packages under {venv_root}')
    return hits[-1]


def build_zip(
    entries: Mapping[str, Path | bytes],
    *,
    vendor: Sequence[str] = (),
    site_packages: Path | None = None,
) -> bytes:
    """Deterministic-shape Lambda bundle: `entries` maps archive names to
    source paths (or literal bytes — e.g. an empty `__init__.py`);
    `vendor` names pure-python site-packages to include wholesale.
    C-extension `.so` files and `__pycache__` are excluded — packages
    with optional C accelerators (e.g. pyyaml) must fall back to their
    pure-python paths."""
    if vendor and site_packages is None:
        raise ValueError('build_zip: vendor packages need site_packages')
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
        for arcname, src in entries.items():
            if isinstance(src, bytes):
                z.writestr(arcname, src)
            else:
                z.write(src, arcname)
        for pkg in vendor:
            root = site_packages / pkg
            if not root.is_dir():
                raise FileNotFoundError(f'{pkg} not found in {site_packages}')
            for f in sorted(root.rglob('*')):
                if f.is_dir() or '__pycache__' in f.parts or f.suffix == '.so':
                    continue
                z.write(f, str(f.relative_to(site_packages)))
    return buf.getvalue()


def upsert_lambda_role(role_name: str, *, description: str = '', session=None) -> str:
    """Basic-execution Lambda role (external stores like R2/D1 need no
    AWS permissions; logs only)."""
    import boto3
    iam = (session or boto3.Session()).client('iam')
    trust = json.dumps({
        'Version': '2012-10-17',
        'Statement': [{
            'Effect': 'Allow',
            'Principal': {'Service': 'lambda.amazonaws.com'},
            'Action': 'sts:AssumeRole',
        }],
    })
    try:
        arn = iam.get_role(RoleName=role_name)['Role']['Arn']
    except iam.exceptions.NoSuchEntityException:
        arn = iam.create_role(
            RoleName=role_name, AssumeRolePolicyDocument=trust, Description=description,
        )['Role']['Arn']
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
        )
        err(f'created role {role_name}; waiting for propagation')
        time.sleep(10)
    return arn


def upsert_lambda_function(
    name: str,
    *,
    role_arn: str,
    zip_blob: bytes,
    memory_mb: int,
    env: Mapping[str, str],
    description: str = '',
    handler: str = 'handler.lambda_handler',
    runtime: str = 'python3.12',
    timeout_s: int = 900,
    layers: Sequence[str] = (),
    reserved: int | None = None,
    session=None,
) -> str:
    """Create-or-update one function (code, then config, then reserved
    concurrency — `None` clears it)."""
    import boto3
    lam = (session or boto3.Session()).client('lambda')
    cfg = dict(
        Runtime=runtime, Role=role_arn, Handler=handler,
        Timeout=timeout_s, MemorySize=memory_mb, Layers=list(layers),
        Environment={'Variables': dict(env)}, Description=description,
    )
    try:
        lam.get_function(FunctionName=name)
        lam.update_function_code(FunctionName=name, ZipFile=zip_blob)
        lam.get_waiter('function_updated').wait(FunctionName=name)
        lam.update_function_configuration(FunctionName=name, **cfg)
        err(f'updated {name}')
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=name, Code={'ZipFile': zip_blob}, **cfg)
        err(f'created {name}')
    lam.get_waiter('function_updated').wait(FunctionName=name)
    if reserved is not None:
        lam.put_function_concurrency(FunctionName=name, ReservedConcurrentExecutions=reserved)
    else:
        lam.delete_function_concurrency(FunctionName=name)
    return lam.get_function(FunctionName=name)['Configuration']['FunctionArn']


def upsert_schedule(
    rule: str,
    rate: str,
    func_arn: str,
    func_name: str,
    *,
    input_json: str | None = None,
    description: str = '',
    session=None,
) -> None:
    """EventBridge rule → function target (+ invoke permission)."""
    import boto3
    sess = session or boto3.Session()
    events = sess.client('events')
    lam = sess.client('lambda')
    rule_arn = events.put_rule(
        Name=rule, ScheduleExpression=rate, State='ENABLED', Description=description,
    )['RuleArn']
    target: dict = {'Id': 'fn', 'Arn': func_arn}
    if input_json is not None:
        target['Input'] = input_json
    events.put_targets(Rule=rule, Targets=[target])
    try:
        lam.add_permission(
            FunctionName=func_name, StatementId='events-invoke',
            Action='lambda:InvokeFunction', Principal='events.amazonaws.com',
            SourceArn=rule_arn,
        )
    except lam.exceptions.ResourceConflictException:
        pass
    err(f'schedule {rule}: {rate}' + (f' input={input_json}' if input_json else ''))


def deploy_pyramid_lambda(
    name: str,
    *,
    zip_blob: bytes,
    role_name: str,
    memory_mb: int,
    env: Mapping[str, str],
    description: str = '',
    handler: str = 'handler.lambda_handler',
    runtime: str = 'python3.12',
    timeout_s: int = 900,
    layers: Sequence[str] = (),
    reserved: int | None = None,
    schedule: str | None = None,
    schedule_rule: str | None = None,
    schedule_input: str | None = None,
    session=None,
) -> str:
    """Role + function (+ optional schedule) in one idempotent call.
    Returns the function ARN."""
    role_arn = upsert_lambda_role(role_name, description=description, session=session)
    arn = upsert_lambda_function(
        name,
        role_arn=role_arn, zip_blob=zip_blob, memory_mb=memory_mb, env=env,
        description=description, handler=handler, runtime=runtime,
        timeout_s=timeout_s, layers=layers, reserved=reserved, session=session,
    )
    if schedule is not None:
        upsert_schedule(
            schedule_rule or f'{name}-tick', schedule, arn, name,
            input_json=schedule_input, description=description, session=session,
        )
    return arn
