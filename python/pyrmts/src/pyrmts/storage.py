from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable


class EtagConflict(RuntimeError):
    """Conditional put failed: the object changed since it was read."""


def _md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


class MemStorage:
    def __init__(
        self,
        data: dict[str, bytes] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._data: dict[str, bytes] = dict(data) if data else {}
        self._mtimes: dict[str, datetime] = {k: self.clock() for k in self._data}

    def head(self, key: str) -> dict | None:
        b = self._data.get(key)
        if b is None: return None
        return {'size': len(b)}

    def get(self, key: str) -> bytes | None:
        return self._data.get(key)

    def get_with_etag(self, key: str) -> tuple[bytes | None, str | None]:
        b = self._data.get(key)
        if b is None: return None, None
        return b, _md5(b)

    def put(self, key: str, data: bytes) -> None:
        self._data[key] = data
        self._mtimes[key] = self.clock()

    def put_if_match(self, key: str, data: bytes, etag: str | None) -> None:
        """Conditional put: `etag` from `get_with_etag` (None = create-only,
        the If-None-Match:* case). Raises `EtagConflict` on mismatch."""
        cur = self._data.get(key)
        if etag is None:
            if cur is not None:
                raise EtagConflict(f"put_if_match: {key} already exists")
        elif cur is None or _md5(cur) != etag:
            raise EtagConflict(f"put_if_match: {key} changed since read")
        self.put(key, data)

    def delete(self, key: str) -> None:
        self._data.pop(key, None)
        self._mtimes.pop(key, None)

    def list(self, prefix: str) -> Iterable[str]:
        for k in sorted(self._data):
            if k.startswith(prefix):
                yield k

    def list_with_mtime(self, prefix: str) -> Iterable[tuple[str, datetime | None]]:
        for k in sorted(self._data):
            if k.startswith(prefix):
                yield k, self._mtimes[k]


class FsStorage:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def _path(self, key: str) -> Path:
        return self.root / key

    def head(self, key: str) -> dict | None:
        p = self._path(key)
        if not p.exists(): return None
        return {'size': p.stat().st_size}

    def get(self, key: str) -> bytes | None:
        p = self._path(key)
        if not p.exists(): return None
        return p.read_bytes()

    def put(self, key: str, data: bytes) -> None:
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def get_with_etag(self, key: str) -> tuple[bytes | None, str | None]:
        b = self.get(key)
        if b is None: return None, None
        return b, _md5(b)

    def put_if_match(self, key: str, data: bytes, etag: str | None) -> None:
        """Best-effort conditional put (compare-then-write, not atomic —
        local filesystems have no CAS primitive; fine for the
        single-writer-per-host cases FsStorage serves)."""
        cur = self.get(key)
        if etag is None:
            if cur is not None:
                raise EtagConflict(f"put_if_match: {key} already exists")
        elif cur is None or _md5(cur) != etag:
            raise EtagConflict(f"put_if_match: {key} changed since read")
        self.put(key, data)

    def delete(self, key: str) -> None:
        p = self._path(key)
        if p.exists():
            p.unlink()

    def list(self, prefix: str) -> Iterable[str]:
        prefix_path = self.root / prefix
        if prefix_path.is_dir():
            base = prefix_path
            rel_prefix = prefix
        else:
            base = prefix_path.parent
            rel_prefix = prefix
        if not base.exists(): return
        for p in sorted(base.rglob('*')):
            if not p.is_file(): continue
            rel = str(p.relative_to(self.root))
            if rel.startswith(rel_prefix):
                yield rel

    def list_with_mtime(self, prefix: str) -> Iterable[tuple[str, datetime | None]]:
        for rel in self.list(prefix):
            mtime = (self.root / rel).stat().st_mtime
            yield rel, datetime.fromtimestamp(mtime, tz=timezone.utc)


class S3Storage:
    """S3-compatible storage (works with AWS S3, Cloudflare R2, etc.).

    Lazy-imports boto3 so the dependency is opt-in (install `pyrmts[s3]`).
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = '',
        endpoint_url: str | None = None,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ) -> None:
        import boto3  # type: ignore[import-untyped]
        self.bucket = bucket
        self.prefix = prefix.rstrip('/') + '/' if prefix and not prefix.endswith('/') else prefix
        self._client = boto3.client(
            's3',
            endpoint_url=endpoint_url or os.environ.get('AWS_ENDPOINT_URL') or os.environ.get('R2_ENDPOINT_URL'),
            region_name=region_name or os.environ.get('AWS_REGION') or 'auto',
            aws_access_key_id=aws_access_key_id or os.environ.get('AWS_ACCESS_KEY_ID') or os.environ.get('R2_ACCESS_KEY_ID'),
            aws_secret_access_key=aws_secret_access_key or os.environ.get('AWS_SECRET_ACCESS_KEY') or os.environ.get('R2_SECRET_ACCESS_KEY'),
        )

    def _key(self, key: str) -> str:
        return f"{self.prefix}{key}"

    def head(self, key: str) -> dict | None:
        try:
            r = self._client.head_object(Bucket=self.bucket, Key=self._key(key))
        except self._client.exceptions.ClientError as e:
            if e.response['Error']['Code'] in ('404', 'NoSuchKey', 'NotFound'):
                return None
            raise
        return {'size': r['ContentLength'], 'etag': r.get('ETag', '').strip('"')}

    def get(self, key: str) -> bytes | None:
        try:
            r = self._client.get_object(Bucket=self.bucket, Key=self._key(key))
        except self._client.exceptions.NoSuchKey:
            return None
        except self._client.exceptions.ClientError as e:
            if e.response['Error']['Code'] in ('404', 'NoSuchKey'):
                return None
            raise
        return r['Body'].read()

    def get_with_etag(self, key: str) -> tuple[bytes | None, str | None]:
        """`(content, etag)` — etag kept verbatim (quoted, as S3/R2 return
        it) for round-tripping into `put_if_match`'s `IfMatch`."""
        try:
            r = self._client.get_object(Bucket=self.bucket, Key=self._key(key))
        except self._client.exceptions.NoSuchKey:
            return None, None
        except self._client.exceptions.ClientError as e:
            if e.response['Error']['Code'] in ('404', 'NoSuchKey'):
                return None, None
            raise
        return r['Body'].read(), r.get('ETag')

    def put(self, key: str, data: bytes) -> None:
        self._client.put_object(Bucket=self.bucket, Key=self._key(key), Body=data)

    def put_if_match(self, key: str, data: bytes, etag: str | None) -> None:
        """Conditional put (CAS): `IfMatch` the etag from `get_with_etag`,
        or `IfNoneMatch: *` when `etag` is None (create-only). Both S3 and
        R2 support conditional writes; raises `EtagConflict` on 412/409."""
        kwargs = {'IfNoneMatch': '*'} if etag is None else {'IfMatch': etag}
        try:
            self._client.put_object(
                Bucket=self.bucket, Key=self._key(key), Body=data, **kwargs,
            )
        except self._client.exceptions.ClientError as e:
            code = e.response['Error']['Code']
            if code in ('PreconditionFailed', 'ConditionalRequestConflict', '412', '409'):
                raise EtagConflict(f"put_if_match: {key}: {code}") from e
            raise

    def delete(self, key: str) -> None:
        self._client.delete_object(Bucket=self.bucket, Key=self._key(key))

    def list(self, prefix: str):
        for key, _ in self.list_with_mtime(prefix):
            yield key

    def list_with_mtime(self, prefix: str):
        paginator = self._client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._key(prefix)):
            for obj in page.get('Contents', []):
                full = obj['Key']
                key = (
                    full[len(self.prefix):]
                    if self.prefix and full.startswith(self.prefix)
                    else full
                )
                yield key, obj['LastModified']


def storage_from_cfg(storage_cfg: dict, *, profile: str | None = None) -> S3Storage:
    """`S3Storage` from a config `storage:` block, honoring R2 conventions
    (`specs/pyrmts-ops-adoption.md` phase 1). Credential chain:

    1. `R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY` env (endpoint from
       `R2_ENDPOINT_URL`, or derived from `CLOUDFLARE_ACCOUNT_ID`).
    2. `profile`, when given: creds resolved via `boto3.Session` from that
       named AWS profile (e.g. an R2-dedicated `[cf]` profile). AWS_* env
       vars are deliberately NOT consulted on this path — on hosts with
       both AWS and R2 creds installed, the default chain picks the
       20-char AWS key and R2 rejects it (`InvalidArgument: Credential
       access key has length 20, should be 32`).
    3. Neither → plain `S3Storage` (env/default AWS chain — the
       non-R2 case).

    Only `storage.type: s3` is supported."""
    typ = storage_cfg.get('type')
    if typ != 's3':
        raise ValueError(f"storage_from_cfg: unsupported storage.type {typ!r}; only 's3' implemented")
    bucket = storage_cfg['bucket']
    prefix = storage_cfg.get('prefix', '')

    endpoint_url = (
        os.environ.get('R2_ENDPOINT_URL')
        or (
            f"https://{os.environ['CLOUDFLARE_ACCOUNT_ID']}.r2.cloudflarestorage.com"
            if 'CLOUDFLARE_ACCOUNT_ID' in os.environ
            else None
        )
    )
    access_key = os.environ.get('R2_ACCESS_KEY_ID')
    secret_key = os.environ.get('R2_SECRET_ACCESS_KEY')
    if not (access_key and secret_key) and profile is not None:
        import boto3  # type: ignore[import-untyped]
        creds = boto3.Session(profile_name=profile).get_credentials()
        if creds is None:
            raise RuntimeError(
                f"storage_from_cfg: no R2 credentials: set R2_ACCESS_KEY_ID/"
                f"R2_SECRET_ACCESS_KEY in env, or configure the [{profile}] "
                f"profile in ~/.aws/credentials"
            )
        frozen = creds.get_frozen_credentials()
        access_key = frozen.access_key
        secret_key = frozen.secret_key
    if not (access_key and secret_key):
        return S3Storage(bucket=bucket, prefix=prefix, endpoint_url=endpoint_url)
    return S3Storage(
        bucket=bucket,
        prefix=prefix,
        endpoint_url=endpoint_url,
        region_name='auto' if endpoint_url else None,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
