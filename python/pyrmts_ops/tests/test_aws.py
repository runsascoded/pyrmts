"""Lambda bundle building (`pyrmts_ops.aws.build_zip`) — no AWS calls."""
from __future__ import annotations

import io
import zipfile

import pytest

from pyrmts_ops.aws import build_zip, find_site_packages


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
