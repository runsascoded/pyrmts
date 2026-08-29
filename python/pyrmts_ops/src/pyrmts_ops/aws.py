"""Lambda deploy skeleton (`specs/pyrmts-ops-adoption.md` phase 3 —
absorbed from ctbk `gbfs/lambda/deploy.py`). Plain boto3, no CDK: upsert
the IAM role, function, and EventBridge schedule. Idempotent — re-run to
redeploy code or config. Consumer deploy scripts shrink to config
invocations of `deploy_pyramid_lambda`.

Two packaging paths:
- **Container image** (`image_uri=`) — the default recommendation for
  pyramid Lambdas: any consumer of `pyrmts_ops.lambda_entry`
  transitively needs polars (a `pyrmts-engine` dependency), and
  C-extension wheels can't ship in a zip bundle. Build/push the image
  with `pyrmts_engine.batch.push_image` (its `dockerfile=` param points
  at the Lambda Dockerfile). Note: AWS can't convert a function between
  package types — a zip↔image flip deletes + recreates it.
- **Zip + layers** (`zip_blob=` via `build_zip`) — for genuinely
  pure-python handlers only.
"""
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

    **C-extension boundary**: `.so` files and `__pycache__` are excluded
    — packages with optional C accelerators (e.g. pyyaml) must fall back
    to their pure-python paths, and packages that REQUIRE C extensions
    (polars, pyarrow beyond what a layer provides — i.e. anything
    importing `pyrmts_engine`) cannot ship this way at all: deploy those
    as container images (`deploy_pyramid_lambda(image_uri=…)`)."""
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


def _wait_gone(lam, name: str, tries: int = 30, delay_s: float = 2.0) -> None:
    for _ in range(tries):
        try:
            lam.get_function(FunctionName=name)
        except lam.exceptions.ResourceNotFoundException:
            return
        time.sleep(delay_s)
    raise TimeoutError(f'{name} still exists after delete_function')


def upsert_lambda_function(
    name: str,
    *,
    role_arn: str,
    memory_mb: int,
    env: Mapping[str, str],
    zip_blob: bytes | None = None,
    image_uri: str | None = None,
    description: str = '',
    handler: str = 'handler.lambda_handler',
    runtime: str = 'python3.12',
    timeout_s: int = 900,
    layers: Sequence[str] = (),
    reserved: int | None = None,
    session=None,
    client=None,
) -> str:
    """Create-or-update one function (code, then config, then reserved
    concurrency — `None` clears it). Exactly one of `zip_blob` /
    `image_uri` selects the package type; `handler`/`runtime`/`layers`
    apply to zip functions only (an image's are baked into it). A
    package-type flip (zip↔image) **deletes and recreates** the function
    — AWS cannot update across package types."""
    if (zip_blob is None) == (image_uri is None):
        raise ValueError('upsert_lambda_function: exactly one of zip_blob / image_uri')
    if client is None:
        import boto3
        client = (session or boto3.Session()).client('lambda')
    lam = client
    desired_type = 'Image' if image_uri is not None else 'Zip'
    cfg = dict(
        Role=role_arn, Timeout=timeout_s, MemorySize=memory_mb,
        Environment={'Variables': dict(env)}, Description=description,
    )
    if desired_type == 'Zip':
        cfg |= dict(Runtime=runtime, Handler=handler, Layers=list(layers))
    code = {'ImageUri': image_uri} if image_uri is not None else {'ZipFile': zip_blob}
    code_update = dict(code) if image_uri is not None else {'ZipFile': zip_blob}

    exists = True
    try:
        existing = lam.get_function(FunctionName=name)['Configuration']
        if existing.get('PackageType', 'Zip') != desired_type:
            err(f'{name}: package type {existing.get("PackageType", "Zip")} → '
                f'{desired_type} requires recreation; deleting')
            lam.delete_function(FunctionName=name)
            _wait_gone(lam, name)
            exists = False
    except lam.exceptions.ResourceNotFoundException:
        exists = False

    if exists:
        lam.update_function_code(FunctionName=name, **code_update)
        lam.get_waiter('function_updated').wait(FunctionName=name)
        lam.update_function_configuration(FunctionName=name, **cfg)
        err(f'updated {name}')
    else:
        create = dict(FunctionName=name, Code=code, **cfg)
        if desired_type == 'Image':
            create['PackageType'] = 'Image'
        lam.create_function(**create)
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
    enabled: bool | None = None,
    session=None,
    events=None,
    lam=None,
) -> None:
    """EventBridge rule → function target (+ invoke permission).

    `enabled` controls the rule's state: `True`/`False` set it explicitly,
    and the default `None` means **preserve** — a rule an operator disabled
    stays disabled across redeploys, and a new rule is created ENABLED.

    That default is deliberate, and is the one thing here that is not a
    plain upsert. `put_rule` has no "leave it alone" mode: omitting `State`
    means ENABLED, so an unconditional call silently re-enables whatever an
    operator turned off. Disabling a tick is how you retire a pyramid, so
    with a forced state the *only* way to make a retirement stick is to
    delete the calling code — at which point the rule still exists in the
    account, invisible to the deployer, and reappears if the code ever comes
    back. (ctbk hit exactly this retiring `avail-v3`, 2026-08-28, and
    documented the trap in a code comment because there was nowhere else to
    put it.) Preserving is the behavior that lets a disable mean something;
    pass `enabled=True` when a deploy really should (re-)enable a tick.

    The invoke-permission `StatementId` is per-rule (`invoke-<rule>`). A
    constant id looks idempotent and isn't: the second rule targeting the
    same function raises `ResourceConflictException` against the *first*
    rule's statement, the retry-swallow hides it, and the rule then fires
    into a function that rejects it. ctbk lost its `avail-v6` tick to this
    for a day (2026-08-06)."""
    import boto3
    sess = None
    if events is None or lam is None:
        sess = session or boto3.Session()
    events = events if events is not None else sess.client('events')
    lam = lam if lam is not None else sess.client('lambda')
    state = None if enabled is None else ('ENABLED' if enabled else 'DISABLED')
    if state is None:
        # Preserve: read the live state, defaulting to ENABLED for a rule
        # that doesn't exist yet.
        try:
            state = events.describe_rule(Name=rule).get('State', 'ENABLED')
        except events.exceptions.ResourceNotFoundException:
            state = 'ENABLED'
    rule_arn = events.put_rule(
        Name=rule, ScheduleExpression=rate, State=state, Description=description,
    )['RuleArn']
    target: dict = {'Id': 'fn', 'Arn': func_arn}
    if input_json is not None:
        target['Input'] = input_json
    events.put_targets(Rule=rule, Targets=[target])
    try:
        lam.add_permission(
            FunctionName=func_name, StatementId=f'invoke-{rule}',
            Action='lambda:InvokeFunction', Principal='events.amazonaws.com',
            SourceArn=rule_arn,
        )
    except lam.exceptions.ResourceConflictException:
        pass
    err(f'schedule {rule}: {rate} state={state}' + (f' input={input_json}' if input_json else ''))


def deploy_pyramid_lambda(
    name: str,
    *,
    role_name: str,
    memory_mb: int,
    env: Mapping[str, str],
    zip_blob: bytes | None = None,
    image_uri: str | None = None,
    description: str = '',
    handler: str = 'handler.lambda_handler',
    runtime: str = 'python3.12',
    timeout_s: int = 900,
    layers: Sequence[str] = (),
    reserved: int | None = None,
    schedule: str | None = None,
    schedule_rule: str | None = None,
    schedule_input: str | None = None,
    schedule_enabled: bool | None = None,
    session=None,
    client=None,
) -> str:
    """Role + function (+ optional schedule) in one idempotent call.
    Package via `image_uri` (recommended — see module docstring) or
    `zip_blob`. Returns the function ARN.

    `schedule_enabled` passes through to `upsert_schedule`; the default
    `None` preserves a rule's existing state rather than forcing it on."""
    role_arn = upsert_lambda_role(role_name, description=description, session=session)
    arn = upsert_lambda_function(
        name,
        role_arn=role_arn, zip_blob=zip_blob, image_uri=image_uri,
        memory_mb=memory_mb, env=env,
        description=description, handler=handler, runtime=runtime,
        timeout_s=timeout_s, layers=layers, reserved=reserved,
        session=session, client=client,
    )
    if schedule is not None:
        upsert_schedule(
            schedule_rule or f'{name}-tick', schedule, arn, name,
            input_json=schedule_input, description=description,
            enabled=schedule_enabled, session=session,
        )
    return arn
