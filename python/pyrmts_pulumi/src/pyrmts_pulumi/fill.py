"""The fill function: a Lambda that polls for raw data and cascades shards,
plus the EventBridge rule that ticks it.

This is the declarative twin of `pyrmts_ops.aws.deploy_pyramid_lambda`, and
two of that function's hard-won behaviors are structural here rather than
carefully coded:

- **Per-rule invoke permissions.** The imperative deployer used a constant
  `StatementId`, so a second rule targeting one function collided with the
  first rule's statement, the conflict was swallowed as "already exists",
  and the rule fired into a function that rejected it — ctbk lost a tick to
  exactly this. Here each schedule is its own `aws.lambda_.Permission`
  resource, so distinct statement ids are not a thing to remember.
- **Disabled means disabled.** `put_rule` has no "leave it alone" mode, so
  the imperative path had to read live state to avoid re-enabling a retired
  tick. A declarative rule's `state` simply *is* the desired state, and an
  operator disabling it in the console shows up as drift on the next
  preview instead of being silently reasserted."""
from __future__ import annotations

from typing import Mapping, Sequence

import pulumi

from ._names import scoped_names

# Least-privilege baseline: CloudWatch Logs only. Pyramid data lives in
# S3/R2, reached with explicit credentials in `env`, so no data policy is
# implied here — pass `policy_arns` for anything more.
LAMBDA_BASIC_EXECUTION = (
    'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
)
LAMBDA_TRUST_POLICY = {
    'Version': '2012-10-17',
    'Statement': [{
        'Effect': 'Allow',
        'Principal': {'Service': 'lambda.amazonaws.com'},
        'Action': 'sts:AssumeRole',
    }],
}


class Schedule:
    """One EventBridge tick for a fill function.

    `input_json` is the event payload; pyrmts handlers use it to select which
    pyramid/tier a tick drives, so one function can serve several schedules
    — which is precisely the shape that made the constant-statement-id bug
    reachable."""

    def __init__(
        self,
        name: str,
        expression: str,
        *,
        input_json: str | None = None,
        enabled: bool = True,
        description: str = '',
    ) -> None:
        self.name = name
        self.expression = expression
        self.input_json = input_json
        self.enabled = enabled
        self.description = description


class FillFunction(pulumi.ComponentResource):
    """A pyramid's fill Lambda, its role, and its schedules."""

    arn: pulumi.Output[str]
    function_name: pulumi.Output[str]
    role_arn: pulumi.Output[str]
    # Keyed by schedule name, so a consumer can hang extra targets off a rule
    # or reference one from another stack without re-deriving its name.
    rules: dict
    permissions: dict

    def __init__(
        self,
        name: str,
        *,
        image_uri: str | pulumi.Output[str] | None = None,
        handler: str | None = None,
        runtime: str | None = None,
        code: pulumi.Archive | None = None,
        env: Mapping[str, str | pulumi.Output[str]] | None = None,
        prefix: str | None = None,
        function_name: str | pulumi.Output[str] | None = None,
        memory_mb: int = 2048,
        timeout_s: int = 900,
        reserved_concurrency: int | None = None,
        architectures: Sequence[str] = ('x86_64',),
        policy_arns: Sequence[str] = (),
        role_arn: str | pulumi.Output[str] | None = None,
        schedules: Sequence[Schedule] = (),
        description: str = '',
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__('pyrmts:index:FillFunction', name, {}, opts)
        import pulumi_aws as aws
        import json

        if (image_uri is None) == (code is None):
            raise ValueError('FillFunction: pass exactly one of `image_uri` or `code`')
        if code is not None and (handler is None or runtime is None):
            raise ValueError('FillFunction: `code` requires `handler` and `runtime`')

        child = pulumi.ResourceOptions(parent=self)
        names = scoped_names(prefix)
        func_name = function_name if function_name is not None else names.prefix

        if role_arn is None:
            role = aws.iam.Role(
                f'{name}-role',
                name=f'{names.prefix}-lambda',
                assume_role_policy=json.dumps(LAMBDA_TRUST_POLICY),
                description=description,
                opts=child,
            )
            for i, arn in enumerate([LAMBDA_BASIC_EXECUTION, *policy_arns]):
                aws.iam.RolePolicyAttachment(
                    f'{name}-policy-{i}', role=role.name, policy_arn=arn, opts=child,
                )
            resolved_role = role.arn
        elif policy_arns:
            raise ValueError('FillFunction: `policy_arns` needs a managed role; drop `role_arn`')
        else:
            resolved_role = pulumi.Output.from_input(role_arn)

        fn = aws.lambda_.Function(
            f'{name}-fn',
            name=func_name,
            role=resolved_role,
            package_type='Image' if image_uri is not None else 'Zip',
            image_uri=image_uri,
            code=code,
            handler=handler,
            runtime=runtime,
            architectures=list(architectures),
            memory_size=memory_mb,
            timeout=timeout_s,
            reserved_concurrent_executions=reserved_concurrency,
            description=description,
            environment=aws.lambda_.FunctionEnvironmentArgs(
                variables=dict(env or {}),
            ) if env else None,
            opts=child,
        )

        self.rules = {}
        self.permissions = {}
        for sched in schedules:
            rule = aws.cloudwatch.EventRule(
                f'{name}-rule-{sched.name}',
                name=f'{names.prefix}-{sched.name}',
                schedule_expression=sched.expression,
                state='ENABLED' if sched.enabled else 'DISABLED',
                description=sched.description or description,
                opts=child,
            )
            aws.cloudwatch.EventTarget(
                f'{name}-target-{sched.name}',
                rule=rule.name,
                arn=fn.arn,
                input=sched.input_json,
                opts=child,
            )
            # One Permission per rule — the shape the imperative deployer had
            # to hand-roll a per-rule `StatementId` to reach.
            perm = aws.lambda_.Permission(
                f'{name}-invoke-{sched.name}',
                action='lambda:InvokeFunction',
                function=fn.name,
                principal='events.amazonaws.com',
                source_arn=rule.arn,
                opts=child,
            )
            self.rules[sched.name] = rule
            self.permissions[sched.name] = perm

        self.arn = fn.arn
        self.function_name = fn.name
        self.role_arn = resolved_role
        self.register_outputs({
            'arn': self.arn, 'function_name': self.function_name, 'role_arn': self.role_arn,
        })
