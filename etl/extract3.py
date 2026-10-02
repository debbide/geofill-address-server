#!/usr/bin/env python3
"""ETL v3: extract the lightweight address dataset from Overture Maps.

Fixes over v2:
  - City rule: postal_city if present; else address_levels[-1] only when
    there are >= 2 levels (a lone level is a state, not a city -- v2 turned
    US state codes into "cities"). Rows with no city are dropped.
  - City grouping is case-insensitive (Overture mixes 'HOUSTON' and
    'Los Angeles'); display name = most frequent original variant.
  - Sampling is global with a generous candidate cap (v2's per-part local
    cap starved true top cities). Counts stay exact.
  - Postcode may be missing at scan time (DE/IT/JP/TW/HK rows often have
    street+city but no postcode). Finalize rescues IT via a GeoNames
    city->postcode join; HK ships with an empty postcode (HK has none).

Checkpoint: etl/checkpoints/state3.pkl after every part.
--finalize emits data/addresses.json + data/addresses.db.
"""
import argparse
import json
import os
import pickle
import random
import re
import sqlite3
import sys
import time
from collections import Counter

import pyarrow.dataset as ds
import pyarrow.fs as fs

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
CHECKPOINT_DIR = os.path.join(HERE, 'checkpoints')
CHECKPOINT_FILE = os.path.join(CHECKPOINT_DIR, 'state3b.pkl')
GEONAMES_DIR = os.environ.get(
    'GEONAMES_DIR', os.path.join(HERE, 'geonames'))

RELEASE = '2026-09-23.1'
BUCKET = 'overturemaps-us-west-2'
PREFIX = f'release/{RELEASE}/theme=addresses/type=address/'

TARGET_COUNTRIES = [
    'US', 'GB', 'CA', 'MX', 'DE', 'FR', 'IT', 'ES', 'NL', 'RU',
    'CN', 'HK', 'TW', 'JP', 'KR', 'SG', 'IN', 'AU', 'BR',
]
# Countries whose missing postcodes get rescued via GeoNames at finalize.
GEONAMES_JOIN = {'IT', 'DE', 'FR'}
# Countries shipped with empty postcode when Overture has none.
EMPTY_POSTCODE_OK = {'HK'}

CITIES_PER_COUNTRY = 10
STREETS_PER_CITY = 20
MAX_SAMPLED_CITIES_PER_COUNTRY = 3000
STREETS_PER_SAMPLED_CITY = 40

_PLACEHOLDER_SUBSTRINGS = (
    'ninguno', 'ninguna', 'sin nombre', 'unknown', 'unnamed',
    'no name', 'not named', 'sem nome',
)
_PLACEHOLDER_EXACT = {'none', 'n/a', 's/n', 'sn', '-', 'null'}


def is_placeholder_street(street):
    s = street.strip().lower()
    if s in _PLACEHOLDER_EXACT or s.isdigit():
        return True
    return any(p in s for p in _PLACEHOLDER_SUBSTRINGS)


def s3_filesystem():
    proxy = os.environ.get('https_proxy') or os.environ.get('HTTPS_PROXY') or ''
    kwargs = {'region': 'us-west-2', 'anonymous': True}
    if proxy:
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
    return {'done_parts': [], 'counts': {}, 'variants': {}, 'samples': {}}


def save_checkpoint(state):
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    tmp = CHECKPOINT_FILE + '.tmp'
    with open(tmp, 'wb') as f:
        pickle.dump(state, f)
    os.replace(tmp, CHECKPOINT_FILE)


def level_values(levels):
    if levels is None:
        return []
    vals = []
    for item in levels:
        if item is None:
            continue
        v = item.get('value') if isinstance(item, dict) else item
        v = str(v).strip() if v is not None else ''
        if v:
            vals.append(v)
    return vals


_TWO_LETTER_CODE = re.compile(r'^[A-Z]{2,3}$')


def extract_city(postal_city, vals):
    """City from postal_city, else the last address level.

    A lone level that is a bare 2-3 letter code (US rows carry just the
    state, e.g. ['TX']) is NOT a city -> row has no usable city.
    A lone full name (FR rows carry just the city, e.g. ['Lille']) IS.
    """
    pc = str(postal_city or '').strip()
    if pc:
        return pc
    if not vals:
        return ''
    if len(vals) == 1 and _TWO_LETTER_CODE.match(vals[0]):
        return ''
    return vals[-1]


def process_part(s3fs, key, state):
    """Accumulate one part locally; merge only on success (retry-safe)."""
    dataset = ds.dataset(f'{BUCKET}/{key}', filesystem=s3fs, format='parquet')
    scanner = dataset.scanner(
        columns=['country', 'street', 'number', 'postcode', 'postal_city',
                 'address_levels'],
        filter=ds.field('country').isin(TARGET_COUNTRIES),
        batch_size=65536)
    counts = {}
    variants = {}
    samples = {}
    for batch in scanner.to_batches():
        df = batch.to_pandas()
        for cc, street, number, postcode, postal_city, levels in df[
                ['country', 'street', 'number', 'postcode', 'postal_city',
                 'address_levels']].itertuples(index=False, name=None):
            street = str(street or '').strip()
            if not street or is_placeholder_street(street):
                continue
            vals = level_values(levels)
            city_raw = extract_city(postal_city, vals)
            if not city_raw:
                continue
            admin1 = vals[0] if vals else ''
            if admin1 and admin1.upper() == city_raw.upper():
                admin1 = ''
            city_key = city_raw.upper()
            postcode = str(postcode or '').strip()
            number = str(number).strip() if number is not None else ''
            ckey = (cc, city_key)
            counts[ckey] = counts.get(ckey, 0) + 1
            variants.setdefault(ckey, Counter())[city_raw] += 1
            csamples = samples.setdefault(cc, {})
            streets = csamples.get(city_key)
            if streets is None:
                if len(csamples) < MAX_SAMPLED_CITIES_PER_COUNTRY:
                    streets = csamples[city_key] = {}
                else:
                    continue
            skey = (street, postcode)
            if len(streets) < STREETS_PER_SAMPLED_CITY and skey not in streets:
                streets[skey] = {'admin1': admin1, 'number': number or None}
            elif number and skey in streets and not streets[skey]['number']:
                streets[skey]['number'] = number
    # merge
    for k, n in counts.items():
        state['counts'][k] = state['counts'].get(k, 0) + n
    for k, ctr in variants.items():
        state['variants'].setdefault(k, Counter()).update(ctr)
    for cc, csamples in samples.items():
        target = state['samples'].setdefault(cc, {})
        for city_key, streets in csamples.items():
            tgt = target.setdefault(city_key, {})
            for skey, info in streets.items():
                if skey not in tgt:
                    tgt[skey] = info
                elif info['number'] and not tgt[skey]['number']:
                    tgt[skey]['number'] = info['number']


def load_geonames_postcodes(cc):
    """city_key -> most frequent postcode for that city (GeoNames zip file)."""
    path = os.path.join(GEONAMES_DIR, f'{cc}.txt')
    per_city = {}
    with open(path, encoding='utf-8') as f:
        for line in f:
            cols = line.rstrip('\n').split('\t')
            if len(cols) < 3:
                continue
            postcode, place = cols[1].strip(), cols[2].strip()
            if not postcode or not place:
                continue
            per_city.setdefault(place.upper(), Counter())[postcode] += 1
    return {city: ctr.most_common(1)[0][0] for city, ctr in per_city.items()}


def finalize(state, out_dir):
    rng = random.Random(42)
    counts = state['counts']
    variants = state['variants']
    samples = state['samples']
    geonames = {cc: load_geonames_postcodes(cc) for cc in GEONAMES_JOIN
                if os.path.exists(os.path.join(GEONAMES_DIR, f'{cc}.txt'))}
    records = []
    report = {}
    for cc in TARGET_COUNTRIES:
        cc_counts = {city: n for (c, city), n in counts.items() if c == cc}
        top_cities = sorted(cc_counts, key=lambda c: -cc_counts[c])[:CITIES_PER_COUNTRY]
        kept = 0
        for city_key in top_cities:
            display = variants[(cc, city_key)].most_common(1)[0][0]
            streets = list(samples.get(cc, {}).get(city_key, {}).items())
            rng.shuffle(streets)
            for (street, postcode), info in streets[:STREETS_PER_CITY]:
                pc = postcode
                if not pc:
                    if cc in EMPTY_POSTCODE_OK:
                        pc = ''
                    elif cc in geonames:
                        pc = geonames[cc].get(city_key, '')
                    if not pc and cc not in EMPTY_POSTCODE_OK:
                        continue
                records.append({
                    'country_code': cc, 'city': display,
                    'admin1': info['admin1'], 'street': street,
                    'postcode': pc, 'number': info['number'],
                })
                kept += 1
        report[cc] = (len(top_cities), kept)
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
        'INSERT OR IGNORE INTO streets VALUES '
        '(:country_code, :city, :admin1, :street, :postcode, :number)',
        records)
    db.execute("INSERT INTO meta VALUES ('license', "
               "'Overture Maps addresses - open data, (c) Overture Maps contributors')")
    db.execute("INSERT INTO meta VALUES ('release', ?)", (RELEASE,))
    db.commit()
    db.close()
    for cc in TARGET_COUNTRIES:
        cities, kept = report[cc]
        if cities or kept:
            print(f'{cc}: {cities} cities, {kept} records')
    return records


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--finalize', action='store_true')
    args = ap.parse_args()

    state = load_checkpoint()
    if args.finalize:
        records = finalize(state, os.path.join(ROOT, 'data'))
        print(f'finalized: {len(records)} records')
        return

    parts = list_parts()
    print(f'{len(parts)} parts in release {RELEASE}', flush=True)
    s3fs = s3_filesystem()
    todo = [p for p in parts if p not in state['done_parts']]
    print(f'{len(todo)} parts to process', flush=True)
    for i, key in enumerate(todo):
        t0 = time.time()
        ok = False
        for attempt in range(1, 4):
            try:
                process_part(s3fs, key, state)
                ok = True
                break
            except Exception as e:  # noqa: BLE001
                if attempt < 3:
                    wait = 15 * attempt
                    print(f'[{i + 1}/{len(todo)}] {os.path.basename(key)} '
                          f'attempt {attempt} failed: {type(e).__name__}; '
                          f'retry in {wait}s', flush=True)
                    time.sleep(wait)
                else:
                    print(f'[{i + 1}/{len(todo)}] {os.path.basename(key)} '
                          f'ERROR after 3 attempts: {e}', flush=True)
        if not ok:
            continue
        state['done_parts'].append(key)
        save_checkpoint(state)
        print(f'[{i + 1}/{len(todo)}] {os.path.basename(key)}: '
              f'{time.time() - t0:.0f}s', flush=True)
    print('done. run with --finalize to emit data files.', flush=True)


if __name__ == '__main__':
    sys.exit(main())
