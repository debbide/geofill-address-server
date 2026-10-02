#!/usr/bin/env python3
"""Targeted top-up sampling.

The main scan's first-come sampling can starve late-geography top cities
(e.g. FR TOULOUSE has exact counts but zero sampled streets). Counts are
exact, so here we compute each country's top-10 cities, find those with no
samples, and re-scan only the parts known to contain those countries.
"""
import os
import pickle
import sys
import time

import pyarrow.dataset as ds

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import extract3 as ex  # noqa: E402

# Geography of the 64 parts (first-batch countries observed during ETL
# development; parts are geographically clustered, so this is reliable).
PART_COUNTRIES = {
    0: ['MX'], 1: ['MX'], 2: ['MX'], 3: ['MX'], 6: ['US'], 7: ['US'],
    8: [], 9: ['BR'], 10: ['BR'], 11: ['BR'], 12: ['BR'], 13: ['BR'],
    14: ['BR'], 15: [], 16: ['US'], 17: ['US'], 18: ['US'], 19: ['US'],
    20: ['US'], 21: ['US'], 22: ['US'], 23: ['US'], 24: ['US'], 25: ['US'],
    26: ['US'], 27: ['US'], 28: ['US'], 29: ['CA', 'US'], 30: ['US'],
    31: ['US'], 32: ['BR'], 33: ['BR'], 34: ['BR'], 35: ['BR'], 36: ['BR'],
    37: ['BR'], 38: [], 39: [], 40: ['ES'], 41: ['ES', 'FR'], 42: ['ES', 'FR'],
    43: ['FR'], 44: ['FR'], 45: ['FR'], 46: [], 47: ['NL'], 48: ['IT'],
    49: ['IT'], 50: ['IT'], 51: ['IT'], 52: [], 53: [], 54: ['DE'],
    55: ['DE'], 56: ['AU'], 57: ['AU', 'TW'], 58: [], 59: [], 60: ['AU'],
    61: ['AU'], 62: ['AU', 'JP'], 63: ['JP'],
}

STREETS_NEEDED = ex.STREETS_PER_SAMPLED_CITY


def main():
    state = ex.load_checkpoint()
    counts, samples = state['counts'], state['samples']
    targets = {}  # (cc, city_key) -> display variants counter key
    for cc in ex.TARGET_COUNTRIES:
        top = sorted(((city, n) for (c, city), n in counts.items() if c == cc),
                     key=lambda x: -x[1])[:ex.CITIES_PER_COUNTRY]
        for city, _n in top:
            if not samples.get(cc, {}).get(city):
                targets.setdefault(cc, set()).add(city)
    if not targets:
        print('nothing to top up')
        return
    print('top-up targets:', {cc: sorted(v) for cc, v in targets.items()},
          flush=True)

    parts = ex.list_parts()
    s3fs = ex.s3_filesystem()
    wanted_countries = set(targets)
    todo = [k for i, k in enumerate(parts)
            if wanted_countries & set(PART_COUNTRIES.get(i, []))]
    print(f'{len(todo)} parts to scan', flush=True)

    filled = set()
    for i, key in enumerate(todo):
        t0 = time.time()
        dataset = ds.dataset(f'{ex.BUCKET}/{key}', filesystem=s3fs,
                             format='parquet')
        scanner = dataset.scanner(
            columns=['country', 'street', 'number', 'postcode', 'postal_city',
                     'address_levels'],
            filter=ds.field('country').isin(list(wanted_countries)),
            batch_size=65536)
        for batch in scanner.to_batches():
            df = batch.to_pandas()
            for cc, street, number, postcode, postal_city, levels in df[
                    ['country', 'street', 'number', 'postcode', 'postal_city',
                     'address_levels']].itertuples(index=False, name=None):
                cities = targets.get(cc)
                if not cities:
                    continue
                street = str(street or '').strip()
                if not street or ex.is_placeholder_street(street):
                    continue
                vals = ex.level_values(levels)
                city_raw = ex.extract_city(postal_city, vals)
                if not city_raw:
                    continue
                city_key = city_raw.upper()
                if city_key not in cities:
                    continue
                admin1 = vals[0] if vals else ''
                if admin1 and admin1.upper() == city_raw.upper():
                    admin1 = ''
                postcode = str(postcode or '').strip()
                number = str(number).strip() if number is not None else ''
                streets = samples.setdefault(cc, {}).setdefault(city_key, {})
                skey = (street, postcode)
                if len(streets) < STREETS_NEEDED and skey not in streets:
                    streets[skey] = {'admin1': admin1,
                                     'number': number or None}
                if len(streets) >= STREETS_NEEDED:
                    filled.add((cc, city_key))
        print(f'[{i + 1}/{len(todo)}] {os.path.basename(key)}: '
              f'{time.time() - t0:.0f}s, filled: {len(filled)}', flush=True)
        if len(filled) >= sum(len(v) for v in targets.values()):
            print('all targets filled, stopping early', flush=True)
            break
    ex.save_checkpoint(state)
    print('saved. re-run extract3.py --finalize.', flush=True)


if __name__ == '__main__':
    main()
