# -*- coding: utf-8 -*-
import os
import os.path
import re
import csv
import json
from unidecode import unidecode
from datetime import datetime, date
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
            if len(sub):
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
    
class AugmentedDiscentes(datasets.Dataset):
    EXTRA_FIELDS = ['DT_MATRICULA_ISO', 'DT_SITUACAO_ISO',
                    'N_CONF', 'N_PER',
                    'PTS_CONF', 'PTS_PER', 'PTS_CONF_IR', 'PTS_PER_IR',
                    'ST_REQ_PUB']
    SICLAP_WEIGHT = {
        'A1': 1,
        'A2': 0.85,
        'B1': 0.70,
        'B2': 0.55,
        'B3': 0.40,
        'B4': 0.25,
        'B5': 0.10,
    }
    IR_WEIGHT = SICLAP_WEIGHT['B1']
    __PUB_TYPE_STR = {'CONF': 'eve', 'PER': 'per'}
    __EVT_TIPO_GRAU = {'DFM': 'MESTRADO', 'DFD': 'DOUTORADO'}
    
    def __init__(self, sucupira, cpc, secretaria, calendar, calendar_csv, \
                 filename='discentes-augmented.csv', **kwargs):
        kwargs['csv_delim'] = kwargs.get('csv_delim', ';')
        super().__init__(filename, None, **kwargs)
        self.sucupira = sucupira
        self.cpc = cpc
        self.cpc_data = None
        self.doi_getter = None
        self.secretaria = secretaria
        self.calendar = calendar
        self.calendar_data = None
        self.calendar_csv = calendar_csv

    def weight(self, cpc_row):
        return self.SICLAP_WEIGHT.get(cpc_row['SICLAP'].upper().strip(), 0)

    def has_pub_type(self, cpc_row, pub_type):
        if pub_type == None:
            return True
        return unidecode(cpc_row['Tipo'].strip()).lower()[:3] \
            == self.__PUB_TYPE_STR.get(pub_type)

    def is_in_master_defense(self, name, phd_enroll_year, cpc_entry):
        if not self.calendar_data and self.calendar:
            with self.calendar.open() as fp:
                self.calendar_data = json.load(fp)
        if not self.calendar_data:
            return False
        if not self.doi_getter:
            self.doi_getter = self.cpc.doi_getter()
        for event in self.calendar_data['items']:
            e_dict = datasets.PPGCC_CALENDAR_CSV.parse_event(event)
            if e_dict == None or e_dict['tipo'] != 'DFM' or \
               not names.same_name(e_dict['discente'], name):
                continue
            doi = self.doi_getter(cpc_entry)
            if doi:
                print(f'######## returning {doi in event["description"]}')
                return doi in event['description']
            if phd_enroll_year == date.fromisoformat(e_dict['data_ymd']).year:
                return True # assume it is a masters' paper
        return False
        
    def get_works(self, dis, pub_type, min_weight=None,
                  bump_year=False, position=None):
        if pub_type != None:
            pub_type = pub_type.strip().upper()
            if pub_type not in self.__PUB_TYPE_STR:
                raise ValueError(f'Bad publication type {pub_type}')
        if not self.cpc_data:
            with self.cpc.open_csv() as reader:
                self.cpc_data = [x for x in reader]
        fmt = self.cpc.AUTHORS_FMT
        nm = dis['NM_DISCENTE']
        start = datetime.strptime(dis['DT_MATRICULA_ISO'], '%Y-%m-%d').year
        if bump_year:
            start += 1
        return [r for r in self.cpc_data if \
                names.is_author(nm, r['autores'], position=position, **fmt) and \
                self.has_pub_type(r, pub_type) and \
                (min_weight==None or self.weight(r) >= min_weight)]

    def has_req_pub(self, dis):
        '''Art. 23 do regulamento do PPGCC'''
        name = dis['NM_DISCENTE']
        grau = dis['DS_GRAU_ACADEMICO_DISCENTE'].strip().upper()
        b3, b2 = self.SICLAP_WEIGHT['B3'], self.SICLAP_WEIGHT['B2']
        b1     = self.SICLAP_WEIGHT['B1']
        if grau == 'MESTRADO':
            return bool(len(self.get_works(dis, None, min_weight=b3, position=0)))
        elif grau == 'DOUTORADO':
            enroll = datetime.strptime(dis['DT_MATRICULA_ISO'], '%Y-%m-%d').year
            works  = self.get_works(dis, 'CONF', min_weight=b3, position=0)
            works += self.get_works(dis, 'PER',  min_weight=b3, position=0)
            works  = [x for x in works \
                      if not self.is_in_master_defense(name, enroll, x)]
            ok_2016 = bool(len(works) >= 2 and \
                      any(filter(lambda r: self.weight(r) >= b2, works)))
            if enroll < 2016 or not ok_2016:
                return ok_2016
            return any(filter(lambda r: self.has_pub_type(r, 'PER') and \
                                   self.weight(r) >= b1, works))
        return True # no rule not applies

    def download(self, force=False, **kwargs):
        filepath = self._get_filepath(**kwargs)
        if not force and os.path.isfile(filepath):
            return filepath
        ir_w = self.SICLAP_WEIGHT['B1']
        fieldnames = []
        students = []
        with self.sucupira.open_csv() as suc_reader, \
             self.secretaria.open_csv() as sec_reader:
            sec_fields = list(filter(lambda x: x not in suc_reader.fieldnames, \
                                      sec_reader.fieldnames))
            fieldnames = suc_reader.fieldnames + sec_fields + self.EXTRA_FIELDS
            students = [dict(x) for x in suc_reader]
            for row in sec_reader:
                nm = row['NM_DISCENTE']
                grau = row['DS_GRAU_ACADEMICO_DISCENTE']
                has = lambda d: names.same_name(d['NM_DISCENTE'], nm) and \
                           d['DS_GRAU_ACADEMICO_DISCENTE'].strip()==grau
                cands = list(filter(has, students))
                if len(cands) > 1:
                    has = lambda d: d['NM_DISCENTE'].strip()==nm and \
                               d['DS_GRAU_ACADEMICO_DISCENTE'].strip()==grau
                    cands = list(filter(has, students))
                if len(cands):
                    for f in sec_fields:
                        cands[0][f] = row[f]
                    f = 'NM_ORIENTADOR_PRINCIPAL'
                    if not cands[0][f] or cands[0][f].upper().strip()=='NA':
                        cands[0][f] = row[f]
                else:
                    students.append(dict(row))
        with self.calendar_csv.open_csv() as cal_reader:
            for event in cal_reader:
                name = event['discente']
                grau = self.__EVT_TIPO_GRAU.get(event['tipo'])
                if grau:
                    cands = [r for r in students \
                             if r['DS_GRAU_ACADEMICO_DISCENTE']==grau and \
                             names.same_name(r['NM_DISCENTE'], name)]
                    if len(cands) > 1:
                        cands = [r for r in cands \
                                 if r['NM_DISCENTE']==name]
                    if len(cands) == 1 and cands[0]['NM_SITUACAO_DISCENTE'] == 'MATRICULADO':
                        cands[0]['NM_SITUACAO_DISCENTE'] = 'TITULADO'
                        end = datasets.iso2suc_date(event['data_ymd'])
                        cands[0]['DT_SITUACAO_DISCENTE'] = end
                        end = datasets.suc_date2date(end)
                        begin = datasets.suc_date2date(cands[0]['DT_MATRICULA_DISCENTE'])
                        months = round((end - begin).days / 30)
                        cands[0]['QT_MES_TITULACAO'] = months
        with open(filepath+'.tmp', 'w', newline='', encoding=self.encoding) as out_f:
            writer = csv.DictWriter(out_f, fieldnames=fieldnames)
            writer.writeheader()
            for d in students:
                d['DT_MATRICULA_ISO'] = datasets.suc_date2iso(d['DT_MATRICULA_DISCENTE'])
                d['DT_SITUACAO_ISO'] = datasets.suc_date2iso(d['DT_SITUACAO_DISCENTE'])
                d['N_CONF'] = sum([1 for x in self.get_works(d, 'CONF')])
                d['N_PER' ] = sum([1 for x in self.get_works(d, 'PER' )])
                d['PTS_CONF'] = sum([self.weight(x) for x in \
                                     self.get_works(d, 'CONF')])
                d['PTS_PER' ] = sum([self.weight(x) for x in \
                                     self.get_works(d, 'PER' )])
                d['PTS_CONF_IR'] = sum([self.weight(x) for x in \
                                        self.get_works(d, 'CONF', \
                                                       min_weight=ir_w)])
                d['PTS_PER_IR' ] = sum([self.weight(x) for x in \
                                        self.get_works(d, 'PER', \
                                                       min_weight=ir_w)])
                d['ST_REQ_PUB'] = self.has_req_pub(d)
                writer.writerow(d)
        os.replace(filepath+'.tmp', filepath)
        return filepath

class MultiProgramDocentes(datasets.Dataset):
    def __init__(self, filename, program_code, year2dataset, **kwargs):
        super().__init__(filename, None, **kwargs)
        self.program_code = program_code
        self.year2dataset = year2dataset

    def download(self, force=False, **kwargs):
        filepath = self._get_filepath(**kwargs)
        if not force and os.path.isfile(filepath):
            return filepath
        fields = []
        for year in sorted(self.year2dataset.keys(), reverse=True):
            with self.year2dataset[year].open_csv() as reader:
                fields += list(filter(lambda x: x not in fields, reader.fieldnames))
        with open(filepath, 'w', encoding='utf-8', newline='') as out_f:
            writer = csv.DictWriter(out_f, fieldnames=fields)
            writer.writeheader()
            for year in sorted(self.year2dataset.keys(), reverse=True):
                ds = self.year2dataset[year]
                ids = set()
                with ds.open_csv() as reader:
                    for row in reader:
                        if self.program_code in row['CD_PROGRAMA_IES']:
                            ids.add(row['ID_PESSOA'].strip())
                with ds.open_csv() as reader:
                    for row in reader:
                        if row['ID_PESSOA'].strip() in ids and \
                           self.program_code not in row['CD_PROGRAMA_IES']:
                            writer.writerow(row)
        return filepath
        


BIBLIOMETRICS = Bibliometrics(datasets.DOCENTES, datasets.LINHAS,
                              scopus=datasets.SCOPUS_WORKS_CSV,
                              scholar=datasets.SCHOLAR_WORKS_CSV)

BIBLIOMETRICS_AGGREGATE = BibliometricsAggregate(BIBLIOMETRICS)

AUG_DISCENTES = AugmentedDiscentes(datasets.SUC_DISCENTES_PPGCC,
                                   datasets.CPC_CSV,
                                   datasets.SECRETARIA_DISCENTES,
                                   datasets.PPGCC_CALENDAR,
                                   datasets.PPGCC_CALENDAR_CSV)
MULTIPROG_DOC = MultiProgramDocentes('multiprog-doc.csv', '41001010025', \
                                     datasets.SUC_DOCENTES)
