"""The batch engine: the Fargate compute environment, queue, and job
definition that long-form pyramid builds run on.

Declarative twin of `pyrmts_engine.batch.bootstrap` — and, critically, built
from the *same* pure spec builders. `job_definition_spec` emits AWS's own
`ContainerProperties` shape, which `aws.batch.JobDefinition` takes verbatim
as a JSON string, so the container sizing, log wiring, and retry policy have
one definition serving both appliers. That is the property worth having:
`bootstrap` once restated `vcpus: int = 8` beside a builder that said 16 and
shadowed it for months, and no amount of care prevents that recurring while
two code paths each describe the same resource."""
from __future__ import annotations

import json
from typing import Mapping, Sequence

import pulumi
from pyrmts_engine.batch import (
    ECR_LIFECYCLE_POLICY,
    ECS_EXECUTION_POLICY_ARN,
    ECS_TRUST_POLICY,
    compute_environment_spec,
    job_definition_spec,
)

from ._names import scoped_names


class BatchEngine(pulumi.ComponentResource):
    """Execution role, log group, Fargate CE(s), queue(s), and job definition.

    `on_demand` additionally creates a non-Spot pair (`<prefix>-od`), for
    runs that must not be reclaimed mid-build; it costs ~3.3× and is off by
    default. The ECR repository is created only when `manage_repository` is
    set — a consumer whose image is built by an existing CI pipeline usually
    already has one, and adopting it here would put a repo full of images
    under this stack's delete path."""

    job_definition: pulumi.Output[str]
    queue: pulumi.Output[str]
    on_demand_queue: pulumi.Output[str] | None
    log_group: pulumi.Output[str]
    execution_role_arn: pulumi.Output[str]
    prefix: str

    def __init__(
        self,
        name: str,
        *,
        image_uri: str | pulumi.Output[str],
        subnet_ids: Sequence[str] | pulumi.Output[Sequence[str]],
        security_group_ids: Sequence[str] | pulumi.Output[Sequence[str]],
        prefix: str | None = None,
        arch: str = 'X86_64',
        max_vcpus: int | None = None,
        vcpus: int | None = None,
        memory_mib: int | None = None,
        ephemeral_gib: int | None = None,
        on_demand: bool = False,
        environment: Mapping[str, str | pulumi.Output[str]] | None = None,
        manage_repository: str | None = None,
        log_retention_days: int | None = None,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__('pyrmts:index:BatchEngine', name, {}, opts)
        import pulumi_aws as aws

        child = pulumi.ResourceOptions(parent=self)
        names = scoped_names(prefix)
        self.prefix = names.prefix
        # `None` means "the builder's default" — never a restated literal.
        ce_sizing = {} if max_vcpus is None else {'max_vcpus': max_vcpus}
        jd_sizing = {
            k: v for k, v in (
                ('vcpus', vcpus), ('memory_mib', memory_mib), ('ephemeral_gib', ephemeral_gib),
            ) if v is not None
        }

        role = aws.iam.Role(
            f'{name}-execution-role',
            name=names.execution_role,
            assume_role_policy=ECS_TRUST_POLICY,
            opts=child,
        )
        aws.iam.RolePolicyAttachment(
            f'{name}-execution-policy',
            role=role.name,
            policy_arn=ECS_EXECUTION_POLICY_ARN,
            opts=child,
        )
        log_group = aws.cloudwatch.LogGroup(
            f'{name}-logs',
            name=names.log_group,
            retention_in_days=log_retention_days,
            opts=child,
        )
        if manage_repository is not None:
            repo = aws.ecr.Repository(f'{name}-repo', name=manage_repository, opts=child)
            aws.ecr.LifecyclePolicy(
                f'{name}-repo-lifecycle',
                repository=repo.name,
                policy=json.dumps(ECR_LIFECYCLE_POLICY),
                opts=child,
            )

        queues: dict[bool, pulumi.Output[str]] = {}
        for spot in [True] + ([False] if on_demand else []):
            spec = compute_environment_spec(
                prefix=names.prefix, spot=spot,
                subnets=[], security_group_ids=[],  # supplied as Outputs below
                **ce_sizing,
            )
            resources = spec['computeResources']
            suffix = 'spot' if spot else 'od'
            ce = aws.batch.ComputeEnvironment(
                f'{name}-ce-{suffix}',
                name=spec['computeEnvironmentName'],
                type=spec['type'],
                state=spec['state'],
                compute_resources=aws.batch.ComputeEnvironmentComputeResourcesArgs(
                    type=resources['type'],
                    max_vcpus=resources['maxvCpus'],
                    subnets=subnet_ids,
                    security_group_ids=security_group_ids,
                ),
                opts=child,
            )
            queue = aws.batch.JobQueue(
                f'{name}-queue-{suffix}',
                name=names.queue(on_demand=not spot),
                state='ENABLED',
                priority=1,
                compute_environment_orders=[aws.batch.JobQueueComputeEnvironmentOrderArgs(
                    order=1, compute_environment=ce.arn,
                )],
                opts=child,
            )
            queues[spot] = queue.name

        # `containerProperties` is AWS's own schema on both sides, so the
        # imperative and declarative paths share one description.
        spec = pulumi.Output.all(
            image=pulumi.Output.from_input(image_uri),
            role_arn=role.arn,
            env=pulumi.Output.from_input(dict(environment or {})),
        ).apply(lambda a: job_definition_spec(
            name=names.job_definition,
            image=a['image'],
            arch=arch,
            execution_role_arn=a['role_arn'],
            environment=a['env'] or None,
            log_group=names.log_group,
            **jd_sizing,
        ))
        job_def = aws.batch.JobDefinition(
            f'{name}-job-def',
            name=names.job_definition,
            type=spec['type'],
            platform_capabilities=spec['platformCapabilities'],
            container_properties=spec['containerProperties'].apply(json.dumps),
            retry_strategy=spec['retryStrategy'].apply(
                lambda r: aws.batch.JobDefinitionRetryStrategyArgs(attempts=r['attempts']),
            ),
            opts=child,
        )

        self.job_definition = job_def.name
        self.queue = queues[True]
        self.on_demand_queue = queues.get(False)
        self.log_group = log_group.name
        self.execution_role_arn = role.arn
        self.register_outputs({
            'job_definition': self.job_definition,
            'queue': self.queue,
            'log_group': self.log_group,
        })

    def submit_args(self) -> list[str]:
        """The CLI flags that point `pyrmts-engine batch submit` at this
        stack — the handoff from declared infra back to the runtime."""
        return ['--prefix', self.prefix]
