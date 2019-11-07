# -*- coding: utf-8 -*-
from ppgcc_metrics import datasets
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
                get_p = lambda x: (x, canon_name(nm, x, levenshtein=lev, \
                                              levenshtein_last=lev_last))
                for j in range(i+1, len(sets)):
                    cands = list(filter(lambda p: p[1] != None, map(get_p, sets[j])))
                    if (allow_ambiguous or len(cands) == 1) and cands[0][0] != nm:
                        x, c = cands[0]
                        assert c == nm or c == x
                        if c == nm:
                            if x not in maps[j] or len(c) > len(maps[j][x]):
                                maps[j][x] = c
                        else:
                            if nm not in maps[i] or len(c) > len(maps[i][nm]):
                                maps[i][nm] = c
                    if not allow_ambiguous and len(cands) > 1:
                        for l in map(lambda p: sorted(p, key=len, reverse=True), \
                                     zip(cycle([nm]), cands)):
                            ambiguous.add(l[0])
    for nm, m in filter(lambda p: p[0] in p[1], product(ambiguous, maps)):
        del m[nm]
    return maps
