# -*- coding: utf-8 -*-
import re
from unidecode import unidecode
from itertools import product, chain, cycle
from Levenshtein import distance

RX_SPACE = re.compile(r'  +')

def clean_name(name):
    if name == None:
        return None
    return RX_SPACE.sub(' ', unidecode(name.strip()).upper())

def canon_name(x, y, levenshtein=0, levenshtein_last=None):
    if x == None or y == None:
        return None
    x, y = clean_name(x).split(' '), clean_name(y).split(' ')
    if len(x) == 0 or len(y) == 0:
        return None
    if levenshtein_last == None:
        levenshtein_last = levenshtein
    if distance(x[-1], y[-1]) > levenshtein_last:
        return None
    if distance(x[0], y[0]) > levenshtein:
        return None
    if len(y) > len(x):
        t = y
        y = x
        x = t
    if len(x) <= 2:
        return ' '.join(x)
    sub = x[1:-1]
    for middle_name in y[1:-1]:
        ms = list(map(lambda x: distance(x, middle_name) <= levenshtein, sub))
        if True not in ms:
            return None
        else:
            sub = sub[ms.index(True)+1:]
    return ' '.join(x)
    
def same_name(*args, **kwargs):
    return canon_name(*args, **kwargs) != None

def canon_maps(*args, allow_ambiguous=False, max_levenshtein=1, 
               max_levenshtein_last=None):
    if max_levenshtein_last == None:
        max_levenshtein_last = max_levenshtein
    lev, lev_last = 0, 0
    sets = [{clean_name(y) for y in x} for x in args]
    ambiguous = set()
    maps = [dict() for x in args]
    for lev, lev_last in product(range(max_levenshtein+1), \
                                 range(max_levenshtein_last+1)):
        for i in range(len(sets)):
            for nm in sets[i]:
                get_p = lambda x: (x, canon_name(nm if i < j else x, \
                                            x  if i < j else nm, \
                                            levenshtein=lev, \
                                            levenshtein_last=lev_last))
                for j in range(len(sets)):
                    if j == i:
                        continue
                    cands = list(filter(lambda p: p[1] != None, map(get_p, sets[j])))
                    if len(cands) == 1 and cands[0][0] != nm:
                        x, c = cands[0]
                        assert c == nm or c == x
                        if c == nm:
                            if x not in maps[j] or len(c) > len(maps[j][x]):
                                maps[j][x] = c
                        else:
                            if nm not in maps[i] or len(c) > len(maps[i][nm]):
                                maps[i][nm] = c
                    if not allow_ambiguous and len(cands) > 1:
                        all_names = chain([nm], map(lambda p: p[0], cands))
                        ambiguous.add(sorted(all_names, key=len)[0])
    for nm, m in filter(lambda p: p[0] in p[1], product(ambiguous, maps)):
        del m[nm]
    return maps

def fix_csv_names(datasets, columns, read_only=[], **kwargs):
    if len(datasets) !=  len(columns):
        raise ValueError(f'fix_csv_names requires len(datasets) == len(columns)')
    sets = []
    for ds, col in zip(datasets, columns):
        with ds.open_csv() as reader:
            sets.append({row[col] for row in reader})
    c_maps = canon_maps(*sets, **kwargs)
    del sets
    in_datasets = datasets
    if read_only != []:
        datasets = list(datasets)
        columns = list(columns)
    for i in sorted(read_only, reverse=True):
        del datasets[i]
        del columns[i]
        del c_maps[i]
    for ds, col, c_map in zip(datasets, columns, c_maps):
        with ds.replace_csv() as writer, ds.open_csv() as reader:
            for row in reader:
                n = row[col]
                row[col] = c_map[n] if n in c_map else n
                writer.writerow(row)
    return in_datasets
