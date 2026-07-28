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
