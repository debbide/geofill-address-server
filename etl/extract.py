#!/usr/bin/env python3
"""Extract a lightweight address dataset from Overture Maps (addresses theme).

Streams Overture Parquet parts from S3 (anonymous, via pyarrow httpfs),
filters to TARGET_COUNTRIES, and aggregates:
  - city_counts: rows per (country, city)  -> pick top-N cities per country
  - samples:     up to MAX_STREETS_PER_CITY distinct (street, postcode)
                 per city, keeping one real house number when present

Progress is checkpointed after every part (etl/checkpoints/state.pkl),
so a crashed/reset run resumes where it left off.

Finish with --finalize to emit data/addresses.json + addresses.db
(top CITIES_PER_COUNTRY cities per country by row count).
"""
import argparse
import json
import os
import pickle
import sqlite3
import sys
import time

import pyarrow.fs as fs
import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
CHECKPOINT_DIR = os.path.join(HERE, 'checkpoints')
CHECKPOINT_FILE = os.path.join(CHECKPOINT_DIR, 'state.pkl')

RELEASE = '2026-09-23.1'
BUCKET = 'overturemaps-us-west-2'
PREFIX = f'release/{RELEASE}/theme=addresses/type=address/'

TARGET_COUNTRIES = [
    'US', 'GB', 'CA', 'MX', 'DE', 'FR', 'IT', 'ES', 'NL', 'RU',
    'CN', 'HK', 'TW', 'JP', 'KR', 'SG', 'IN', 'AU', 'BR',
]

CITIES_PER_COUNTRY = 10
STREETS_PER_CITY = 20
MAX_SAMPLED_CITIES_PER_COUNTRY = 4000

# House-number placement per country, used by the server at serve time.
# 'prefix': "350 5th Ave" | 'suffix': "Via Roma 10" | 'jp': "1-1 Chiyoda" |
# 'tw': "No. 12, Zhongxiao Rd"
NUMBER_STYLE = {
    'US': 'prefix', 'GB': 'prefix', 'CA': 'prefix', 'AU': 'prefix',
    'FR': 'prefix', 'IN': 'prefix', 'SG': 'prefix', 'RU': 'prefix',
    'CN': 'prefix', 'KR': 'prefix', 'HK': 'prefix',
    'DE': 'suffix', 'IT': 'suffix', 'ES': 'suffix', 'NL': 'suffix',
    'BR': 'suffix', 'MX': 'suffix',
    'JP': 'jp', 'TW': 'tw',
}


def s3_filesystem():
    import urllib.request
    proxy = os.environ.get('https_proxy') or os.environ.get('HTTPS_PROXY') or ''
    kwargs = {'region': 'us-west-2', 'anonymous': True}
    if proxy:
        # pyarrow wants scheme://host:port (no userinfo); creds via env are
        # already consumed by the sandbox proxy chain in practice, so strip.
        proxy = proxy.split('@')[-1]
        if '://' not in proxy:
            proxy = 'http://' + proxy
        kwargs['proxy_options'] = proxy
    return fs.S3FileSystem(**kwargs)


def list_parts():
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    client = boto3.client('s3', config=Config(signature_version=UNSIGNED),
                          region_name='us-west-2')
    parts, token = [], None
    while True:
        kw = {'Bucket': BUCKET, 'Prefix': PREFIX}
        if token:
            kw['ContinuationToken'] = token
        resp = client.list_objects_v2(**kw)
        parts += [o['Key'] for o in resp.get('Contents', []) if 'part-' in o['Key']]
        if not resp.get('IsTruncated'):
            break
        token = resp.get('NextContinuationToken')
    return sorted(parts)


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'rb') as f:
            return pickle.load(f)
    return {'done_parts': [], 'city_counts': {}, 'samples': {}}


def save_checkpoint(state):
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    tmp = CHECKPOINT_FILE + '.tmp'
    with open(tmp, 'wb') as f:
        pickle.dump(state, f)
    os.replace(tmp, CHECKPOINT_FILE)


def extract_levels(levels):
    """address_levels: list of {'value': str}; [0]=admin1, [-1]=city."""
    if levels is None:
        return '', ''
    vals = []
    for item in levels:
        if item is None:
            continue
        v = item.get('value') if isinstance(item, dict) else item
        v = str(v).strip() if v is not None else ''
        if v:
            vals.append(v)
    if not vals:
        return '', ''
    return vals[0], vals[-1]


def process_part(s3fs, key, state, max_rows=0):
    import pyarrow.dataset as ds
    full = f'{BUCKET}/{key}'
    dataset = ds.dataset(full, filesystem=s3fs, format='parquet')
    scanner = dataset.scanner(
        columns=['country', 'street', 'number', 'postcode', 'address_levels'],
        filter=ds.field('country').isin(TARGET_COUNTRIES),
        batch_size=65536)
    counts = state['city_counts']
    samples = state['samples']
    n = 0
    for batch in scanner.to_batches():
        df = batch.to_pandas()
        for cc, street, number, postcode, levels in df[
                ['country', 'street', 'number', 'postcode', 'address_levels']
                ].itertuples(index=False, name=None):
            street = str(street or '').strip()
            postcode = str(postcode or '').strip()
            if not street or not postcode:
                continue
            if is_placeholder_street(street):
                continue
            admin1, city = extract_levels(levels)
            if not city:
                continue
            number = str(number).strip() if number is not None else ''
            ckey = (cc, city)
            counts[ckey] = counts.get(ckey, 0) + 1
            csamples = samples.setdefault(cc, {})
            if city not in csamples and \
                    len(csamples) >= MAX_SAMPLED_CITIES_PER_COUNTRY:
                continue
            streets = csamples.setdefault(city, {})
            skey = (street, postcode)
            if skey not in streets:
                streets[skey] = {'admin1': admin1, 'number': number or None}
            elif number and not streets[skey]['number']:
                streets[skey]['number'] = number
            n += 1
            if max_rows and n >= max_rows:
                return n
    return n


TARGET_SET = frozenset(TARGET_COUNTRIES)

# Placeholder street names found in open data (the upstream project had to
# reject these too). Matched case-insensitively as substrings.
_PLACEHOLDER_SUBSTRINGS = (
    'ninguno', 'ninguna', 'sin nombre', 'unknown', 'unnamed',
    'no name', 'not named', 'sem nome',
)
_PLACEHOLDER_EXACT = {'none', 'n/a', 's/n', 'sn', '-', 'null'}


def is_placeholder_street(street):
    s = street.strip().lower()
    if s in _PLACEHOLDER_EXACT:
        return True
    if s.isdigit():
        return True
    return any(p in s for p in _PLACEHOLDER_SUBSTRINGS)


def finalize(state, out_dir):
    import random
    rng = random.Random(42)
    counts = state['city_counts']
    samples = state['samples']
    records = []
    for cc in TARGET_COUNTRIES:
        cc_counts = {city: n for (c, city), n in counts.items() if c == cc}
        top_cities = sorted(cc_counts, key=lambda c: -cc_counts[c])[:CITIES_PER_COUNTRY]
        for city in top_cities:
            streets = list(samples.get(cc, {}).get(city, {}).items())
            rng.shuffle(streets)
            for (street, postcode), info in streets[:STREETS_PER_CITY]:
                records.append({
                    'country_code': cc,
                    'city': city,
                    'admin1': info['admin1'],
                    'street': street,
                    'postcode': postcode,
                    'number': info['number'],
                })
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, 'addresses.json'), 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=1)

    db_path = os.path.join(out_dir, 'addresses.db')
    if os.path.exists(db_path):
        os.remove(db_path)
    db = sqlite3.connect(db_path)
    db.executescript('''
        CREATE TABLE streets (
            country_code TEXT NOT NULL,
            city         TEXT NOT NULL,
            admin1       TEXT NOT NULL,
            street       TEXT NOT NULL,
            postcode     TEXT NOT NULL,
            number       TEXT,
            PRIMARY KEY (country_code, city, street, postcode)
        );
        CREATE INDEX idx_country_city ON streets(country_code, city);
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
    ''')
    db.executemany(
        'INSERT OR IGNORE INTO streets VALUES (:country_code, :city, :admin1, :street, :postcode, :number)',
        records)
    db.execute("INSERT INTO meta VALUES ('license', 'Overture Maps addresses - open data, (c) Overture Maps contributors')")
    db.execute("INSERT INTO meta VALUES ('release', ?)", (RELEASE,))
    db.commit()
    db.close()
    return records


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--finalize', action='store_true',
                    help='emit data/addresses.json + addresses.db from checkpoint')
    ap.add_argument('--limit-parts', type=int, default=0,
                    help='only process the first N parts (smoke test)')
    ap.add_argument('--max-rows', type=int, default=0,
                    help='stop after N matching rows per part (smoke test)')
    args = ap.parse_args()

    if args.finalize:
        state = load_checkpoint()
        records = finalize(state, os.path.join(ROOT, 'data'))
        print(f'finalized: {len(records)} records')
        return

    parts = list_parts()
    print(f'{len(parts)} parts in release {RELEASE}', flush=True)
    state = load_checkpoint()
    s3fs = s3_filesystem()
    todo = [p for p in parts if p not in state['done_parts']]
    if args.limit_parts:
        todo = todo[:args.limit_parts]
    print(f'{len(todo)} parts to process', flush=True)
    for i, key in enumerate(todo):
        t0 = time.time()
        try:
            n = process_part(s3fs, key, state, max_rows=args.max_rows)
        except Exception as e:  # noqa: BLE001 - keep going, checkpoint anyway
            print(f'[{i + 1}/{len(todo)}] {os.path.basename(key)} ERROR: {e}', flush=True)
            continue
        state['done_parts'].append(key)
        save_checkpoint(state)
        covered = len({cc for (cc, _) in state['city_counts']})
        print(f'[{i + 1}/{len(todo)}] {os.path.basename(key)}: {n} rows, '
              f'{time.time() - t0:.0f}s, countries covered: {covered}', flush=True)
    print('done. run with --finalize to emit data files.', flush=True)


if __name__ == '__main__':
    sys.exit(main())
