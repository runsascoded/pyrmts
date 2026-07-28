"""`storage_from_cfg` — R2-aware S3Storage factory (no network; asserts
the constructed client's config)."""
from __future__ import annotations

import pytest

from pyrmts import storage_from_cfg

R2_VARS = (
    'R2_ENDPOINT_URL', 'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY',
    'CLOUDFLARE_ACCOUNT_ID', 'AWS_ENDPOINT_URL',
)


def _clear_env(monkeypatch) -> None:
    for var in R2_VARS:
        monkeypatch.delenv(var, raising=False)


def test_r2_env_creds_and_derived_endpoint(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv('CLOUDFLARE_ACCOUNT_ID', 'acct123')
    monkeypatch.setenv('R2_ACCESS_KEY_ID', 'k' * 32)
    monkeypatch.setenv('R2_SECRET_ACCESS_KEY', 's' * 43)
    s = storage_from_cfg({'type': 's3', 'bucket': 'b', 'prefix': 'pyr'})
    assert (s.bucket, s.prefix) == ('b', 'pyr/')
    assert s._client.meta.endpoint_url == 'https://acct123.r2.cloudflarestorage.com'
    assert s._client.meta.region_name == 'auto'


def test_explicit_r2_endpoint_wins(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv('CLOUDFLARE_ACCOUNT_ID', 'acct123')
    monkeypatch.setenv('R2_ENDPOINT_URL', 'https://x.r2.cloudflarestorage.com')
    monkeypatch.setenv('R2_ACCESS_KEY_ID', 'k' * 32)
    monkeypatch.setenv('R2_SECRET_ACCESS_KEY', 's' * 43)
    s = storage_from_cfg({'type': 's3', 'bucket': 'b'})
    assert s._client.meta.endpoint_url == 'https://x.r2.cloudflarestorage.com'


def test_rejects_non_s3(monkeypatch):
    _clear_env(monkeypatch)
    with pytest.raises(ValueError) as exc:
        storage_from_cfg({'type': 'fs', 'bucket': 'b'})
    assert str(exc.value) == "storage_from_cfg: unsupported storage.type 'fs'; only 's3' implemented"


def test_missing_profile_creds_raises(monkeypatch):
    """With a profile requested but resolving no credentials, fail loudly
    (never fall through to AWS_* env — R2 rejects 20-char AWS keys)."""
    _clear_env(monkeypatch)
    import boto3
    monkeypatch.setattr(
        boto3, 'Session',
        lambda profile_name: type('S', (), {'get_credentials': staticmethod(lambda: None)})(),
    )
    with pytest.raises(RuntimeError) as exc:
        storage_from_cfg({'type': 's3', 'bucket': 'b'}, profile='cf')
    assert str(exc.value) == (
        'storage_from_cfg: no R2 credentials: set R2_ACCESS_KEY_ID/'
        'R2_SECRET_ACCESS_KEY in env, or configure the [cf] profile in '
        '~/.aws/credentials'
    )
