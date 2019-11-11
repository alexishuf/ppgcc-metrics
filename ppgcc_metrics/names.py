# -*- coding: utf-8 -*-
import re
from unidecode import unidecode
from itertools import product, chain, cycle
from Levenshtein import distance

RX_SPACE = re.compile(r'  +')
RX_DOT = re.compile(r'\.\s*$')
RX_LAST_FIRST = re.compile(r'\s*(\S+)\s+(.*)')

def clean_name(name):
    if name == None:
        return None
    return RX_SPACE.sub(' ', unidecode(name.strip()).upper())

def safe_distance(a, b):
    a = RX_DOT.sub('', a)
    b = RX_DOT.sub('', b)
    if a == b:
        return 0
    if len(a) == 0 or len(b) == 0:
        return max(len(a), len(b)) + 1
    if a[0] == b[0] and len(a) == 1 and len(b) > 3:
        return 0
    if a[0] == b[0] and len(b) == 1 and len(a) > 3:
        return 0
    if len(a) < 3 or len(b) < 3:
        return max(len(a), len(b)) + 1
    return distance(a, b)

def _subsplit(string, outer_sep, inner_sep, keep_inner='LEFT'):
    result = []
    for outer in string.split(outer_sep):
        parts = outer.split(inner_sep)
        if keep_inner.upper() == 'LEFT':
            parts = [x+inner_sep for x in parts]
            parts[-1] = parts[-1][:-len(inner_sep)]
        elif keep_inner.upper() == 'RIGHT':
            parts = [inner_sep+x for x in parts]
            parts[0] = parts[0][len(inner_sep):]
        result += list(filter(len, parts))
    return result

RX_SUPER_COMPACT = re.compile(r'^\s*([A-Z][A-Z]+)\s+(.*[a-z].*)$')
def _parse_super_compact(name):
    m = RX_SUPER_COMPACT.search(name)
    if m != None:
        return '. '.join(m.group(1)) + '. ' + m.group(2)
    return name

def canon_name(x, y, levenshtein=0, levenshtein_last=None,
               super_compact=False, large_last=7):
    '''Returns the canononical name between x and y
    Named arguments:
      - levenshtein: Maximum levenshtein distance for non-last names
      - levenshtein_last: Maximum levenshtein distance for last names
      - super_compact: Read JP Doe as J. P. Doe
      - large_last: Only apply levenshtein_last if last name is larger than this
    '''
    if x == None or y == None:
        return None
    if super_compact:
        x, y = _parse_super_compact(x), _parse_super_compact(y)
    x = _subsplit(clean_name(x), ' ', '.', keep_inner='LEFT')
    y = _subsplit(clean_name(y), ' ', '.', keep_inner='LEFT')
    if len(x) == 0 or len(y) == 0:
        return None
    if levenshtein_last == None:
        levenshtein_last = levenshtein
    if max(len(x[-1]), len(y[-1])) >= large_last:
        levenshtein_last += 1
    if safe_distance(x[-1], y[-1]) > levenshtein_last:
        return None
    if safe_distance(x[0], y[0]) > levenshtein:
        return None
    if len(RX_DOT.sub('', x[0])) == 1 and len(y[0]) > 1:
        x[0] = y[0]
    if len(y) > len(x):
        t = y
        y = x
        x = t
    if len(x) <= 2:
        return ' '.join(x)
    sub = x[1:-1]
    for middle_name in y[1:-1]:
        ms = list(map(lambda x: safe_distance(x, middle_name) <= levenshtein, sub))
        if True not in ms:
            return None
        else:
            sub = sub[ms.index(True)+1:]
    sub = y[1:-1]
    for i in range(1, len(x)-1):
        ms = list(map(lambda u: safe_distance(u, x[i]) <= levenshtein, sub))
        if True in ms:
            cand_y = sub[ms.index(True)]
            if len(RX_DOT.sub('', x[i])) == 1 and len(cand_y) > 1:
                x[i] = cand_y
            sub = sub[ms.index(True)+1:]
    return ' '.join(x)
    
def same_name(*args, **kwargs):
    return canon_name(*args, **kwargs) != None

def is_author(name, author_list, position=None, sep=';', order=',', **kwargs):
    '''Return True if name is in the given author_list
    
    Arguments:
      - name: the name to look for, in FIRST_FIRST order (see order below)
      - author_list: a string containing a list of names
      - position: None (default), int or 'FIRST'.
                  Only return True if name is the position-th author
                  If None, any position in the list suffices
                  FIRST has the same meaning as 0
      - sep: separator of the author list
      - order: Name order in the author list. In all cases, all names except 
               the last may be abbreviated and, if abbreviated, optionally 
               separated by spaces. Valid values:
               - ',' (default): LastName, FirstName SecondName
               - 'LAST_FIRST': LastName FirstName SecondName
               - 'FIRST_FIRST': FirstName SecondName LastName
      - **kwargs: Furhter named arguments are forwarded to same_name()
    '''
    if name == None or author_list == None:
        return False
    if order.upper() not in [',', 'LAST_FIRST', 'FIRST_FIRST']:
        raise ValueError(f'Unexpected order: {order}')
    if sep == order:
        raise ValueError(f'sep==order ({sep}=={order}) is ambiguous')
    if position != None:
        position = str(position).strip()
        if not re.match(r'(?i)FIRST|\d+$', position):
            raise ValueError(f'Bad position: {position}')
        position = 0 if position == 'FIRST' else int(position)
    cands = []
    for x in author_list.split(sep):
        if order in ['LAST_FIRST', 'FIRST_FIRST']:
            if order == 'LAST_FIRST':
                x = RX_LAST_FIRST.sub(r'\2 \1', x.strip())
            cands.append(x)
        else:
            parts = x.strip().split(order)
            if len(parts) == 1:
                cands.append(parts[0])
            elif len(parts) == 2:
                cands.append(parts[1] + ' ' + parts[0])
    if position != None:
        if position >= len(cands):
            return False
        cands = [cands[position]]
    return any(map(lambda c: same_name(name, c, **kwargs), cands))        

def is_in(name, iterable, allow_ambiguous=True, **kwargs):
    matcher = lambda x: same_name(name, x, **kwargs)
    matches = len(list(filter(bool, map(matcher, iterable))))
    return matches == 1 or (allow_ambiguous and matches >= 1)

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
