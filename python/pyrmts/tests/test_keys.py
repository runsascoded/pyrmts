import pytest

from pyrmts import substitute_key


def test_basic():
    assert substitute_key('avail/{tier}/{period}.parquet', {'tier': 'h1', 'period': '2026-05'}) == 'avail/h1/2026-05.parquet'


def test_int_value():
    assert substitute_key('awair-{device_id}/{tier}.parquet', {'device_id': 17617, 'tier': 'd1'}) == 'awair-17617/d1.parquet'


def test_missing_value_raises():
    with pytest.raises(KeyError, match=r'\{period\}'):
        substitute_key('{tier}/{period}', {'tier': 'h1'})


def test_no_placeholders():
    assert substitute_key('static/path.parquet', {}) == 'static/path.parquet'


def test_shard_placeholder():
    assert substitute_key(
        'avail/{tier}/{shard}/{period}.parquet',
        {'tier': 'h1', 'shard': '1mo', 'period': '2026-05'},
    ) == 'avail/h1/1mo/2026-05.parquet'


def test_extra_values_ignored():
    """Extra values in the dict are harmless when the template doesn't reference them."""
    assert substitute_key(
        'avail/{tier}/{period}.parquet',
        {'tier': 'h1', 'shard': '1mo', 'period': '2026-05'},
    ) == 'avail/h1/2026-05.parquet'


# ── content-hashed keys (`specs/content-addressed-shards.md`)

import hashlib

from pyrmts import (
    MemStorage,
    TemplateResolver,
    content_hash,
    key_pattern,
    parse_key,
    put_shard,
    slot_key,
    slot_of,
    template_has_hash,
    validate_key_template,
)

MD5 = hashlib.md5(b'payload').hexdigest()
T = 'rides/{tier}/{shard}/{period}.{hash:12}.parquet'


def test_hash_token_full_and_truncated():
    v = {'tier': 'base', 'shard': '1mo', 'period': '2026-01', 'hash': MD5}
    assert substitute_key('blobs/{hash}.parquet', v) == f'blobs/{MD5}.parquet'
    assert substitute_key(T, v) == f'rides/base/1mo/2026-01.{MD5[:12]}.parquet'
    assert substitute_key('{hash:1}', v) == MD5[:1]
    assert substitute_key('{hash:32}', v) == MD5


def test_hash_token_validation():
    with pytest.raises(ValueError, match=r'\{hash:0\} must be 1\.\.32'):
        validate_key_template('x/{hash:0}')
    with pytest.raises(ValueError, match=r'\{hash:33\} must be 1\.\.32'):
        validate_key_template('x/{hash:33}')
    with pytest.raises(ValueError, match=r'`:8` is only defined for \{hash\}'):
        validate_key_template('x/{period:8}')
    with pytest.raises(ValueError, match='wants a 32-char md5 hex'):
        substitute_key('{hash:8}', {'hash': 'nope'})
    with pytest.raises(KeyError, match=r'\{hash\}'):
        substitute_key(T, {'tier': 'base', 'shard': '1mo', 'period': '2026-01'})
    assert template_has_hash(T) and not template_has_hash('rides/{tier}/{period}.parquet')


def test_slot_key_keeps_the_hash_placeholder():
    v = {'tier': 'base', 'shard': '1mo', 'period': '2026-01'}
    assert slot_key(T, v) == 'rides/base/1mo/2026-01.{hash:12}.parquet'
    assert slot_key('rides/{tier}/{period}.parquet', v) == 'rides/base/2026-01.parquet'   # hashless: the key


def test_key_pattern_parse_and_slot_of():
    key = f'rides/base/1mo/2026-01.{MD5[:12]}.parquet'
    assert key_pattern(T).pattern == r'^rides/(?P<tier>[^/]+)/(?P<shard>[^/]+)/(?P<period>[^/]+)\.(?P<hash>[0-9a-f]{12})\.parquet$'
    assert parse_key(T, key) == {'tier': 'base', 'shard': '1mo', 'period': '2026-01', 'hash': MD5[:12]}
    assert parse_key(T, 'rides/base/1mo/2026-01.parquet') is None                    # legacy key: no hash
    assert parse_key(T, f'rides/base/1mo/2026-01.{MD5[:11]}.parquet') is None         # wrong width
    assert slot_of(T, key) == 'rides/base/1mo/2026-01.{hash:12}.parquet'
    assert slot_of('blobs/{hash}.parquet', f'blobs/{MD5}.parquet') == 'blobs/{hash}.parquet'
    assert parse_key('a/{x}/{x}.parquet', 'a/1/1.parquet') == {'x': '1'}             # repeated placeholder backrefs
    assert parse_key('a/{x}/{x}.parquet', 'a/1/2.parquet') is None


def test_put_shard_hashed_is_put_if_absent_and_never_overwrites():
    s = MemStorage()
    v = {'tier': 'base', 'shard': '1mo', 'period': '2026-01'}
    w1 = put_shard(s, T, v, b'payload')
    assert (w1.key, w1.md5, w1.n_bytes, w1.put) == (f'rides/base/1mo/2026-01.{MD5[:12]}.parquet', MD5, 7, True)
    assert content_hash(b'payload') == MD5
    w2 = put_shard(s, T, v, b'payload')                                  # identical bytes: skipped
    assert (w2.key, w2.put) == (w1.key, False)
    w3 = put_shard(s, T, v, b'payload v2')                               # new version: new key, old blob intact
    assert w3.key != w1.key and w3.put
    assert sorted(s.list('rides/')) == sorted([w1.key, w3.key])
    assert s.get(w1.key) == b'payload' and s.get(w3.key) == b'payload v2'


def test_put_shard_hashless_rewrites_in_place():
    s = MemStorage()
    t = 'rides/{tier}/{period}.parquet'
    v = {'tier': 'base', 'period': '2026-01'}
    assert put_shard(s, t, v, b'a').key == put_shard(s, t, v, b'b').key == 'rides/base/2026-01.parquet'
    assert s.get('rides/base/2026-01.parquet') == b'b'


def test_template_resolver_refuses_hashed_templates():
    assert TemplateResolver('rides/{tier}/{period}.parquet').resolve('base', '1mo', 0, '2026-01', {}) == 'rides/base/2026-01.parquet'
    with pytest.raises(ValueError, match='registry-backed resolver'):
        TemplateResolver(T).resolve('base', '1mo', 0, '2026-01', {})


def test_slot_values_put_shard_slot_and_listed_slots():
    from pyrmts import listed_slots, put_shard_slot, slot_values

    s = MemStorage()
    slot = 'rides/base/1mo/2026-01.{hash:12}.parquet'
    assert slot_values(T, slot) == {'tier': 'base', 'shard': '1mo', 'period': '2026-01'}
    with pytest.raises(ValueError, match='not a slot key'):
        slot_values(T, 'rides/base/1mo/2026-01.parquet')
    w = put_shard_slot(s, T, slot, b'payload')
    assert w.key == f'rides/base/1mo/2026-01.{MD5[:12]}.parquet'
    assert listed_slots(s, T) == {slot: w.key}
    # A hashless template has no hash group, so a hashed key parses as a plain key
    # (period '2026-01.<hash>') and is its own slot — legacy and hashed pyramids
    # must not share a prefix.
    assert listed_slots(s, 'rides/{tier}/{shard}/{period}.parquet') == {w.key: w.key}
    put_shard_slot(s, T, slot, b'payload v2')                                         # a second version → ambiguous
    with pytest.raises(ValueError, match='several versions'):
        listed_slots(s, T)
