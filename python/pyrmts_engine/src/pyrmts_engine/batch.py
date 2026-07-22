"""AWS Batch packaging: one-time idempotent `bootstrap` + per-build
`submit` (with optional log tailing). See `specs/engine-batch-packaging.md`.

Design points:

- **Fargate Spot** compute environment: no AMI/instance-role/capacity
  management; 16 vCPU / 120 GB / 200 GB ephemeral ceiling covers the
  engine's envelope. An EC2-spot CE is a follow-up if cost/wall demands.
- Plain boto3 (lazy import; install `pyrmts-engine[batch]`), no CDK/TF.
  Region/creds from the standard env/config chain.
- The IAM surface is the Fargate *execution* role only (image pull +
  logs). R2 creds ride as plain env on the job definition / submit
  overrides — no AWS data permissions involved. (If configs live on real
  AWS S3, add a job role later; R2-only consumers don't need one.)
- `bootstrap` is create-if-missing for role/logs/ECR/CE/queue; the job
  definition is re-registered each run (revisions are harmless and
  comparing specs is fussier than it's worth).

Pure spec-builder functions below are the tested surface; the boto3 calls
are thin wrappers around them."""
from __future__ import annotations

import sys
import time
from functools import partial

err = partial(print, file=sys.stderr)

PREFIX = 'pyrmts-engine'
LOG_GROUP = f'/{PREFIX}/batch'
EXECUTION_ROLE = f'{PREFIX}-batch-execution'
ECS_TRUST_POLICY = (
    '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", '
    '"Principal": {"Service": "ecs-tasks.amazonaws.com"}, '
    '"Action": "sts:AssumeRole"}]}'
)
ECS_EXECUTION_POLICY_ARN = 'arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy'


# -- pure builders ----------------------------------------------------------

def compute_environment_spec(
    *,
    name: str | None = None,
    spot: bool = True,
    max_vcpus: int = 16,
    subnets: list[str],
    security_group_ids: list[str],
) -> dict:
    return {
        'computeEnvironmentName': name or (f'{PREFIX}-spot' if spot else f'{PREFIX}-od'),
        'type': 'MANAGED',
        'state': 'ENABLED',
        'computeResources': {
            'type': 'FARGATE_SPOT' if spot else 'FARGATE',
            'maxvCpus': max_vcpus,
            'subnets': subnets,
            'securityGroupIds': security_group_ids,
        },
    }


def job_definition_spec(
    *,
    name: str = PREFIX,
    image: str,
    arch: str = 'X86_64',
    vcpus: int = 8,
    memory_mib: int = 32768,
    ephemeral_gib: int = 100,
    execution_role_arn: str,
    log_group: str = LOG_GROUP,
    environment: dict[str, str] | None = None,
) -> dict:
    return {
        'jobDefinitionName': name,
        'type': 'container',
        'platformCapabilities': ['FARGATE'],
        'containerProperties': {
            'image': image,
            'runtimePlatform': {
                'operatingSystemFamily': 'LINUX',
                'cpuArchitecture': arch,
            },
            'resourceRequirements': [
                {'type': 'VCPU', 'value': str(vcpus)},
                {'type': 'MEMORY', 'value': str(memory_mib)},
            ],
            'ephemeralStorage': {'sizeInGiB': ephemeral_gib},
            'executionRoleArn': execution_role_arn,
            'networkConfiguration': {'assignPublicIp': 'ENABLED'},
            'logConfiguration': {
                'logDriver': 'awslogs',
                'options': {'awslogs-group': log_group},
            },
            'environment': [
                {'name': k, 'value': v} for k, v in sorted((environment or {}).items())
            ],
        },
        'retryStrategy': {'attempts': 2},  # spot reclaim gets one retry
    }


def push_commands(
    image: str,
    *,
    dockerfile: str | None = None,
    context: str = '.',
    platform: str | None = 'linux/amd64',
    build: bool = True,
) -> list[list[str]]:
    """Docker argvs for `push` (login is separate — it needs the ECR token)."""
    cmds = []
    if build:
        cmd = ['docker', 'build', '-t', image]
        if platform is not None:
            cmd += ['--platform', platform]
        if dockerfile is not None:
            cmd += ['-f', dockerfile]
        cmds.append(cmd + [context])
    cmds.append(['docker', 'push', image])
    return cmds


def build_command(
    config: str,
    *,
    pyramid_name: str,
    range_: str,
    window: str | None = None,
    rg_size: int | None = None,
    sort: str | None = None,
    source: str | None = None,
    source_tier: str | None = None,
    source_shard: str | None = None,
    manifest: str | None = None,
    resume: bool = False,
    allow_empty: bool = False,
    max_missing: float | None = None,
    filters: tuple[str, ...] = (),
    workers: int | None = None,
    max_inflight: int | None = None,
    mem_budget: str | None = None,
    verbose: bool = True,
) -> list[str]:
    """Container command for the base image (whose entrypoint is
    `pyrmts-engine`)."""
    cmd = ['build', '-n', pyramid_name, '-r', range_]
    if window is not None:
        cmd += ['-w', window]
    if rg_size is not None:
        cmd += ['-g', str(rg_size)]
    if sort is not None:
        cmd += ['-s', sort]
    if source is not None:
        cmd += ['-x', source]
    if source_tier is not None:
        cmd += ['-t', source_tier]
    if source_shard is not None:
        cmd += ['-d', source_shard]
    if manifest is not None:
        cmd += ['-m', manifest]
    if resume:
        cmd += ['-u']
    if allow_empty:
        cmd += ['-e']
    if max_missing is not None:
        cmd += ['-M', str(max_missing)]
    for f in filters:
        cmd += ['-F', f]
    if workers is not None:
        cmd += ['-j', str(workers)]
    if max_inflight is not None:
        cmd += ['-K', str(max_inflight)]
    if mem_budget is not None:
        cmd += ['-b', mem_budget]
    if verbose:
        cmd += ['-v']
    cmd += [config]
    return cmd


def submit_overrides(
    command: list[str],
    *,
    vcpus: int | None = None,
    memory_mib: int | None = None,
    environment: dict[str, str] | None = None,
) -> dict:
    overrides: dict = {'command': command}
    rr = []
    if vcpus is not None:
        rr.append({'type': 'VCPU', 'value': str(vcpus)})
    if memory_mib is not None:
        rr.append({'type': 'MEMORY', 'value': str(memory_mib)})
    if rr:
        overrides['resourceRequirements'] = rr
    if environment:
        overrides['environment'] = [
            {'name': k, 'value': v} for k, v in sorted(environment.items())
        ]
    return overrides


# -- boto3 orchestration ----------------------------------------------------

def _clients():
    import boto3
    return {name: boto3.client(name) for name in ('iam', 'logs', 'ecr', 'ec2', 'batch')}


ECR_LIFECYCLE_POLICY = {
    'rules': [
        {
            'rulePriority': 1,
            'description': 'expire untagged (superseded buildx manifests) after 7 days',
            'selection': {
                'tagStatus': 'untagged',
                'countType': 'sinceImagePushed',
                'countUnit': 'days',
                'countNumber': 7,
            },
            'action': {'type': 'expire'},
        },
        {
            'rulePriority': 2,
            'description': 'keep the 4 most recent tags',
            'selection': {
                'tagStatus': 'tagged',
                'tagPatternList': ['*'],
                'countType': 'imageCountMoreThan',
                'countNumber': 4,
            },
            'action': {'type': 'expire'},
        },
    ],
}


def _ensure_repo(ecr, image: str) -> None:
    repo_name = image.split('/')[-1].split(':')[0]
    try:
        ecr.describe_repositories(repositoryNames=[repo_name])
        err(f'ecr repo {repo_name}: exists')
    except ecr.exceptions.RepositoryNotFoundException:
        ecr.create_repository(repositoryName=repo_name)
        # Self-maintaining pruning from day one (only on creation — an
        # existing repo may carry a customized policy).
        import json
        ecr.put_lifecycle_policy(
            repositoryName=repo_name,
            lifecyclePolicyText=json.dumps(ECR_LIFECYCLE_POLICY),
        )
        err(f'ecr repo {repo_name}: created (lifecycle: keep 4 tags, expire untagged >7d)')


def push_image(
    image: str,
    *,
    dockerfile: str | None = None,
    context: str = '.',
    platform: str | None = 'linux/amd64',
    build: bool = True,
) -> None:
    """ECR-login docker, then build (unless disabled) + push `image`.
    Creates the repo if missing, so push can run before `bootstrap`."""
    import base64
    from subprocess import run
    import boto3
    ecr = boto3.client('ecr')
    _ensure_repo(ecr, image)
    auth = ecr.get_authorization_token()['authorizationData'][0]
    password = base64.b64decode(auth['authorizationToken']).decode().split(':', 1)[1]
    registry = auth['proxyEndpoint']
    run(
        ['docker', 'login', '--username', 'AWS', '--password-stdin', registry],
        input=password.encode(), check=True,
    )
    for cmd in push_commands(
        image, dockerfile=dockerfile, context=context, platform=platform, build=build,
    ):
        err(f'+ {" ".join(cmd)}')
        run(cmd, check=True)


def bootstrap(
    *,
    image: str,
    arch: str = 'X86_64',
    max_vcpus: int = 16,
    vcpus: int = 8,
    memory_mib: int = 32768,
    ephemeral_gib: int = 100,
    on_demand: bool = False,
    environment: dict[str, str] | None = None,
) -> None:
    c = _clients()

    # Execution role (image pull + logs).
    try:
        role = c['iam'].get_role(RoleName=EXECUTION_ROLE)['Role']
        err(f'role {EXECUTION_ROLE}: exists')
    except c['iam'].exceptions.NoSuchEntityException:
        role = c['iam'].create_role(
            RoleName=EXECUTION_ROLE,
            AssumeRolePolicyDocument=ECS_TRUST_POLICY,
        )['Role']
        c['iam'].attach_role_policy(RoleName=EXECUTION_ROLE, PolicyArn=ECS_EXECUTION_POLICY_ARN)
        err(f'role {EXECUTION_ROLE}: created')

    # Log group.
    groups = c['logs'].describe_log_groups(logGroupNamePrefix=LOG_GROUP)['logGroups']
    if not any(g['logGroupName'] == LOG_GROUP for g in groups):
        c['logs'].create_log_group(logGroupName=LOG_GROUP)
        err(f'log group {LOG_GROUP}: created')
    else:
        err(f'log group {LOG_GROUP}: exists')

    # ECR repo (only if the image ref looks like ECR-in-this-account).
    _ensure_repo(c['ecr'], image)

    # Default-VPC networking.
    subnets = [
        s['SubnetId']
        for s in c['ec2'].describe_subnets(
            Filters=[{'Name': 'default-for-az', 'Values': ['true']}],
        )['Subnets']
    ]
    if not subnets:
        raise SystemExit('bootstrap: no default-VPC subnets found; pass a configured VPC (unsupported yet)')
    sgs = [
        g['GroupId']
        for g in c['ec2'].describe_security_groups(
            Filters=[{'Name': 'group-name', 'Values': ['default']}],
        )['SecurityGroups']
    ][:1]

    # Compute environments + queues: the spot pair always; an on-demand
    # pair (queue `<prefix>-od`) when requested — ~3.3× compute cost but
    # immune to Spot reclaims, for "final" runs until resume makes Spot
    # reclaims cheap.
    pairs = [(True, PREFIX)] + ([(False, f'{PREFIX}-od')] if on_demand else [])
    for spot, queue_name in pairs:
        spec = compute_environment_spec(
            spot=spot, max_vcpus=max_vcpus, subnets=subnets, security_group_ids=sgs,
        )
        ce_name = spec['computeEnvironmentName']
        existing = c['batch'].describe_compute_environments(
            computeEnvironments=[ce_name],
        )['computeEnvironments']
        if not existing:
            c['batch'].create_compute_environment(**spec)
            err(f'compute environment {ce_name}: created')
        else:
            err(f'compute environment {ce_name}: exists ({existing[0]["status"]})')
        _wait(lambda: c['batch'].describe_compute_environments(
            computeEnvironments=[ce_name],
        )['computeEnvironments'][0]['status'] == 'VALID', 'compute environment VALID')

        queues = c['batch'].describe_job_queues(jobQueues=[queue_name])['jobQueues']
        if not queues:
            c['batch'].create_job_queue(
                jobQueueName=queue_name,
                state='ENABLED',
                priority=1,
                computeEnvironmentOrder=[{'order': 1, 'computeEnvironment': ce_name}],
            )
            err(f'job queue {queue_name}: created')
        else:
            err(f'job queue {queue_name}: exists')

    # Job definition — always (re-)registered; revisions are harmless.
    c['batch'].register_job_definition(**job_definition_spec(
        image=image,
        arch=arch,
        vcpus=vcpus,
        memory_mib=memory_mib,
        ephemeral_gib=ephemeral_gib,
        execution_role_arn=role['Arn'],
        environment=environment,
    ))
    err(f'job definition {PREFIX}: registered')


def submit(
    *,
    command: list[str],
    job_name: str,
    queue: str = PREFIX,
    vcpus: int | None = None,
    memory_mib: int | None = None,
    environment: dict[str, str] | None = None,
    watch: bool = False,
) -> int:
    """Submit a build job; with `watch`, tail its log stream and return the
    container's exit code (also non-zero on FAILED without one)."""
    c = _clients()
    job = c['batch'].submit_job(
        jobName=job_name,
        jobQueue=queue,
        jobDefinition=PREFIX,
        containerOverrides=submit_overrides(
            command, vcpus=vcpus, memory_mib=memory_mib, environment=environment,
        ),
    )
    job_id = job['jobId']
    err(f'submitted {job_id} ({job_name})')
    if not watch:
        print(job_id)
        return 0
    return _watch(c, job_id)


def _watch(c: dict, job_id: str) -> int:
    last_status = None
    next_token = None
    stream = None
    while True:
        desc = c['batch'].describe_jobs(jobs=[job_id])['jobs'][0]
        status = desc['status']
        if status != last_status:
            err(f'  status: {status}')
            last_status = status
        stream = desc.get('container', {}).get('logStreamName') or stream
        if stream is not None:
            kwargs = {'logGroupName': LOG_GROUP, 'logStreamName': stream, 'startFromHead': True}
            if next_token is not None:
                kwargs['nextToken'] = next_token
            try:
                resp = c['logs'].get_log_events(**kwargs)
            except c['logs'].exceptions.ResourceNotFoundException:
                resp = None
            if resp is not None:
                for e in resp['events']:
                    print(e['message'])
                next_token = resp['nextForwardToken']
        if status in ('SUCCEEDED', 'FAILED'):
            exit_code = desc.get('container', {}).get('exitCode')
            reason = desc.get('statusReason')
            if reason:
                err(f'  reason: {reason}')
            return exit_code if exit_code is not None else (0 if status == 'SUCCEEDED' else 1)
        time.sleep(15)


def _wait(pred, what: str, timeout: float = 300, interval: float = 5) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return
        time.sleep(interval)
    raise TimeoutError(f'bootstrap: timed out waiting for {what}')
