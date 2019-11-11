# -*- coding: utf-8 -*-
import os.path
import re
import csv
from datetime import datetime
from itertools import chain, product
from ppgcc_metrics import names, datasets

def h_index(citations):
    d = dict()
    for number in citations:
        for i in range(1, number+1):
            d[i] = 1 + (d[i] if i in d else 0)
    h = 0
    for k, v in d.items():
        if k > h and v >= k:
            h = k
    return h
                
class Bibliometrics(datasets.Dataset):
    FIELDS = ['group', 'pub_year', 'base_year', 'source',
              'h', 'h5', 'documents', 'citations']

    def __init__(self, docentes, linhas, filename='bibliometrics-year.csv', \
                 scopus=None, scholar=None, base_year=None, **kwargs):
        super().__init__(filename, None, **kwargs)
        self.docentes_ds = docentes
        self.linhas_ds = linhas
        self.linhas = None
        self.scopus = scopus
        self.scholar = scholar
        self.base_year = base_year if base_year != None else datetime.now().year

    def _get_fieldname(self, fieldnames, *args):
        for name in args:
            is_field = lambda x: x.strip().lower()==name.strip().lower()
            f = next(chain(filter(is_field, fieldnames), [None]))
            if f != None:
                return f
        raise ValueError(f'Could not find field for {name} in {fieldnames}')

    def _write_metrics(self, group, source, rows, base, fields, dict_sink):

        year_f = self._get_fieldname(fields, 'year')
        cited_f = self._get_fieldname(fields, 'cited by', 'citations')
        h = h_index([r[cited_f] for r in rows])
        h5_years = range(base-5, base)
        h5 = h_index([r[cited_f] for r in rows if r[year_f] in h5_years])
        years = {r[year_f] for r in rows if r[year_f]}
        # print(f'write_metrics({group}, {source}, years={min(years)}:{max(years)+1} h={h}, h5={h5}')
        for year in range(min(years), max(years)+1):
            sub = [x for x in rows if x[year_f] == year]
            dict_sink({
                'group': group, 'pub_year': year, 'base_year': base,
                'source': source, 'documents' : len(sub),
                'h'  : sum([1 for r in sub if r[cited_f] >= h ]),
                'h5' : sum([1 for r in sub if r[cited_f] >= h5 and \
                                              r[year_f] in h5_years]),
                'citations': sum([r[cited_f] for r in sub])
            })

    def _get_linhas(self):
        if not self.linhas:
            with self.linhas_ds.open_csv() as linhas_reader, \
                 self.docentes_ds.open_csv() as docs_reader:
                docs = [r['docente'] for r in docs_reader \
                        if r['status'].upper().strip()=='PERMANENTE']
                self.linhas = [r for r in linhas_reader \
                               if names.is_in(r['docente'], docs)]
        return self.linhas

    def fetch_for(self, src_name, source_ds, base_year, dict_sink):
        if source_ds == None:
            return
        with source_ds.open_csv() as reader:
            fields = reader.fieldnames
            year_f = self._get_fieldname(fields, 'year')
            a_f = self._get_fieldname(fields, 'authors')
            cited_f = self._get_fieldname(fields, 'cited by', 'citations')
            rows = [x for x in reader]
            for r in rows:
                r[cited_f] = datasets.tolerant_int(r[cited_f], empty=0)
                r[year_f] = datasets.tolerant_int(r[year_f])
            self._write_metrics('all', src_name, rows, base_year,
                                fields, dict_sink)
            linhas = self._get_linhas()
            for group in {r['linha'].strip().lower() for r in linhas}:
                nms = [r['docente'] for r in linhas \
                       if r['linha'].strip().lower() == group]
                fmt = source_ds.AUTHORS_FMT
                sub = [r for r in rows if any\
                       (map(lambda d: names.is_author(d, r[a_f], **fmt), nms))]
                self._write_metrics(group, src_name, sub, \
                                    base_year, fields, dict_sink)
        
    def download(self, force=False, **kwargs):
        filepath = self._get_filepath(directory=kwargs.get('directory'))
        if not force and os.path.isfile(filepath):
            return filepath
        with open(filepath, 'w', newline='', encoding=self.encoding) as out_f:
            writer = csv.DictWriter(out_f, fieldnames=self.FIELDS)
            writer.writeheader()
            scholar = kwargs.get('scholar', self.scholar)
            scopus = kwargs.get('scopus', self.scopus)
            base = kwargs.get('base_year', self.base_year)
            self.fetch_for('scholar', scholar, base, writer.writerow)
            self.fetch_for('scopus', scopus, base, writer.writerow)
        return filepath

class BibliometricsAggregate(datasets.Dataset):
    FIELDS = ['group', 'base_year', 'source', 'h', 'h5',
              'documents', 'citations', 'impact']
    _NUMERIC_FIELDS = ['pub_year', 'h', 'h5', 'documents', 'citations']
    
    def __init__(self, bibliometrics, filename='bibliometrics.csv', **kwargs):
        super().__init__(filename, None, **kwargs)
        self.bib = bibliometrics

    def download(self, force=False, **kwargs):
        filepath = self._get_filepath(directory=kwargs.get('directory'))
        if not force and os.path.isfile(filepath):
            return filepath
        with self.bib.open_csv() as reader, \
             open(filepath, 'w', newline='', encoding=self.encoding) as out_f:
            out = csv.DictWriter(out_f, fieldnames=self.FIELDS)
            out.writeheader()
            data = [x for x in reader]
            for r, c in product(data, self._NUMERIC_FIELDS):
                r[c] = datasets.tolerant_int(r[c], empty=0)
            for g, s in {(r['group'], r['source']) for r in data}:
                sub = [r for r in data if r['group']==g and r['source']==s]
                row = {'group': g, 'source': s,
                       'base_year': self.bib.base_year,
                       'h' : sum([r['h' ] for r in sub]),
                       'h5': sum([r['h5'] for r in sub]),
                       'documents': sum([r['documents'] for r in sub]),
                       'citations': sum([r['citations'] for r in sub]),
                }
                impact_years = range(self.bib.base_year-2, self.bib.base_year)
                impact_sub = [r for r in sub if r['pub_year'] in impact_years]
                row['impact'] = sum([r['citations'] for r in impact_sub]) \
                              / sum([r['documents'] for r in impact_sub])
                out.writerow(row)
        return filepath

BIBLIOMETRICS = Bibliometrics(datasets.DOCENTES, datasets.LINHAS,
                              scopus=datasets.SCOPUS_WORKS_CSV,
                              scholar=datasets.SCHOLAR_WORKS_CSV)
BIBLIOMETRICS_AGGREGATE = BibliometricsAggregate(BIBLIOMETRICS)
