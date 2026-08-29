"""BatchEngine: the declared graph, and its agreement with the imperative path."""
from __future__ import annotations

import json

import pulumi
from conftest import DECLARED, declared, names_of

from pyrmts_engine.batch import job_definition_spec, resource_names
from pyrmts_pulumi import BatchEngine

CE = 'aws:batch/computeEnvironment:ComputeEnvironment'
QUEUE = 'aws:batch/jobQueue:JobQueue'
JOB_DEF = 'aws:batch/jobDefinition:JobDefinition'
ROLE = 'aws:iam/role:Role'
LOGS = 'aws:cloudwatch/logGroup:LogGroup'


def _engine(name: str, **kwargs) -> BatchEngine:
    return BatchEngine(
        name,
        image_uri='123.dkr.ecr.us-east-1.amazonaws.com/engine:abc',
        subnet_ids=['subnet-1'],
        security_group_ids=['sg-1'],
        **kwargs,
    )


@pulumi.runtime.test
def test_declares_the_full_spot_footprint():
    # Sorted, not in declaration order: Pulumi registers resources
    # concurrently, so order here is a race, not a contract.
    engine = _engine('e1', prefix='alpha')
    def check(_):
        assert sorted(t for t, n, _ in DECLARED if t.startswith('aws:') and n.startswith('e1')) == sorted([
            ROLE,
            'aws:iam/rolePolicyAttachment:RolePolicyAttachment',
            LOGS,
            CE,
            QUEUE,
            JOB_DEF,
        ])
    return pulumi.Output.all(engine.job_definition).apply(check)


@pulumi.runtime.test
def test_on_demand_adds_a_second_pair_only():
    engine = _engine('e2', prefix='beta', on_demand=True)
    def check(_):
        assert sorted(names_of(CE, 'e2')) == ['beta-od', 'beta-spot']
        assert sorted(names_of(QUEUE, 'e2')) == ['beta', 'beta-od']
    return pulumi.Output.all(engine.job_definition).apply(check)


@pulumi.runtime.test
def test_container_properties_are_the_imperative_spec_verbatim():
    # The anti-drift property: one description, two appliers. If this ever
    # fails, the declarative and imperative paths have forked.
    engine = _engine('e3', prefix='gamma', vcpus=4, memory_mib=8192)
    def check(_):
        (_, inputs), = declared(JOB_DEF, 'e3')
        expected = job_definition_spec(
            name='gamma',
            image='123.dkr.ecr.us-east-1.amazonaws.com/engine:abc',
            execution_role_arn='arn:aws:mock:::e3-execution-role',
            vcpus=4,
            memory_mib=8192,
            log_group='/gamma/batch',
        )
        assert json.loads(inputs['containerProperties']) == expected['containerProperties']
    return pulumi.Output.all(engine.job_definition).apply(check)


@pulumi.runtime.test
def test_sizing_defaults_come_from_the_builder():
    engine = _engine('e4', prefix='delta')
    def check(_):
        (_, inputs), = declared(JOB_DEF, 'e4')
        cp = json.loads(inputs['containerProperties'])
        assert {r['type']: r['value'] for r in cp['resourceRequirements']} == {
            'VCPU': '16', 'MEMORY': '32768',
        }
    return pulumi.Output.all(engine.job_definition).apply(check)


@pulumi.runtime.test
def test_every_physical_name_carries_the_prefix():
    engine = _engine('e5', prefix='epsilon', on_demand=True)
    def check(_):
        names = resource_names('epsilon')
        assert {
            'role': sorted(names_of(ROLE, 'e5')),
            'logs': sorted(names_of(LOGS, 'e5')),
            'ce': sorted(names_of(CE, 'e5')),
            'queue': sorted(names_of(QUEUE, 'e5')),
            'job_def': sorted(names_of(JOB_DEF, 'e5')),
        } == {
            'role': [names.execution_role],
            'logs': [names.log_group],
            'ce': sorted([names.spot_ce, names.od_ce]),
            'queue': sorted([names.spot_queue, names.od_queue]),
            'job_def': [names.job_definition],
        }
    return pulumi.Output.all(engine.job_definition).apply(check)


@pulumi.runtime.test
def test_two_deployments_share_no_physical_name():
    # The isolation guarantee, stated as a test: two stacks must not name the
    # same account-global resource. Compared per resource type, because the
    # job definition and the spot queue legitimately share the bare prefix —
    # Batch scopes those in separate namespaces.
    a = _engine('e6a', prefix='one', on_demand=True)
    b = _engine('e6b', prefix='two', on_demand=True)
    def check(_):
        # Partition by owning deployment rather than by declaration order.
        per_type = {
            typ: {
                owner: sorted(n for n in names_of(typ, 'e6') if owner in n)
                for owner in ('one', 'two')
            }
            for typ in (ROLE, LOGS, CE, QUEUE, JOB_DEF)
        }
        assert per_type == {
            ROLE: {'one': ['one-batch-execution'], 'two': ['two-batch-execution']},
            LOGS: {'one': ['/one/batch'], 'two': ['/two/batch']},
            CE: {'one': ['one-od', 'one-spot'], 'two': ['two-od', 'two-spot']},
            QUEUE: {'one': ['one', 'one-od'], 'two': ['two', 'two-od']},
            JOB_DEF: {'one': ['one'], 'two': ['two']},
        }
        for owners in per_type.values():
            assert set(owners['one']) & set(owners['two']) == set()
    return pulumi.Output.all(a.job_definition, b.job_definition).apply(check)


@pulumi.runtime.test
def test_submit_args_point_the_cli_at_this_stack():
    engine = _engine('e7', prefix='zeta')
    def check(_):
        assert engine.submit_args() == ['--prefix', 'zeta']
    return pulumi.Output.all(engine.job_definition).apply(check)
