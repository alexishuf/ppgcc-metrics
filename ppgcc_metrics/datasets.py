# -*- coding: utf-8 -*-
import requests
import requests_html
import os.path
import os
import errno
import lzma
import re
import csv
import json
import googleapiclient.discovery
import pyperclip
from datetime import datetime
from random import randint
from itertools import chain, product
from ppgcc_metrics import names
from time import sleep
from contextlib import contextmanager
from google.oauth2 import service_account
from unidecode import unidecode

SERVICE_ACCOUNT_FILE = 'service-account-key.json'

class Dataset:
    def __init__(self, name, url, directory='data',
                 csv_delim=',', encoding='utf-8'):
        self.filename = name
        self.url = url
        self.directory = directory
        self.csv_delim = csv_delim
        self.encoding = encoding

    def __str__(self):
        return self.filename

    def _get_filepath(self, directory=None, create_dir=True, **kwargs):
        directory = self.directory if directory == None else directory
        if not os.path.isdir(directory):
            os.makedirs(directory)
        return os.path.join(directory, self.filename)
        
    def download(self, directory=None, force=False, **kwargs):
        filepath = self._get_filepath(directory=directory)
        if force or not os.path.isfile(filepath):
            with open(filepath+'.tmp', 'b') as out:
                r = requests.get(self.url, stream=True)
                for chunk in r.iter_content(chunk_size=2048):
                    out.write(chunk)
            os.replace(filepath+'.tmp', filepath)
        return filepath

    def open(self, **kwargs):
        newline = kwargs['newline'] if 'newline' in kwargs else None
        encoding = kwargs['encoding'] if 'encoding' in kwargs else self.encoding
        return open(self.download(**kwargs), 'r',
                    newline=newline, encoding=encoding)

    @contextmanager
    def open_csv(self, **kwargs):
        if 'newline' in kwargs:
            del kwargs['newline']
        f = self.open(newline='', **kwargs)
        r = csv.DictReader(f, delimiter=self.csv_delim)
        try:
            yield r
        finally:
            f.close()

    def _open(self, filepath, mode, **kwargs):
        return open(filepath, mode, **kwargs)

    @contextmanager
    def replace_csv(self, **kwargs):
        if 'newline' in kwargs:
            del kwargs['newline']
        fields = []
        with self.open_csv(**kwargs) as r:
            fields = r.fieldnames
        directory = kwargs.get('directory')
        directory = self.directory if directory == None else directory
        filepath = os.path.join(directory, self.filename+'.tmp')
        f = self._open(filepath, 'w', newline='')
        w = csv.DictWriter(f, fieldnames=fields, delimiter=self.csv_delim)
        try:
            w.writeheader()
            yield w
        finally:
            f.close()
            os.replace(filepath, self._get_filepath(**kwargs))
        

class InputDataset(Dataset):
    def __init__(self, filename, **kwargs):
        super().__init__(filename, None, **kwargs)

    def download(self, directory=None, **kwargs):
        filepath = self._get_filepath(directory=directory)
        if not os.path.isfile(filepath):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filepath)
        return filepath
        
    
class SucupiraDataset(Dataset):
    def __init__(self, name, url, **kwargs):
        super().__init__(name, url, csv_delim=';', **kwargs)

    def download(self, directory=None, force=False, **kwargs):
        filepath = self._get_filepath(directory=directory)
        if not force and os.path.isfile(filepath):
            return filepath
        print(f'Downloading {self.url}')
        r = requests.get(self.url, stream=True)
        r.encoding = 'iso-8859-1'
        with lzma.open(filepath+'.tmp', 'wt',
                       encoding='utf-8', newline='\r\n') as xz:
            na_rx = re.compile(';\s*N[AÃ]O +SE +APLICA\s*;')
            for line in r.iter_lines(decode_unicode=True):
                line = na_rx.sub(';NA;', line)
                xz.write(line + '\n')
        os.replace(filepath+'.tmp', filepath)
        print(f'Downloaded {self.url} into {filepath}')
        return filepath

    def open(self, **kwargs):
        return self._open(self.download(**kwargs), 'r', **kwargs)

    def _open(self, filepath, mode, **kwargs):
        newline = kwargs['newline'] if 'newline' in kwargs else None
        return lzma.open(filepath, mode+'t',
                         newline=newline, encoding='utf-8')

    
class SucupiraProgram(Dataset):
    FIELD_UPGRADES = {
        'NM_ORIENTADOR' : 'NM_ORIENTADOR_PRINCIPAL',
        'TP_RACA_DISCENTE': 'NM_RACA_COR',
        'IN_DEFICIENCIA': 'IN_NECESSIDADE_PESSOAL'
    }
    ID = 'ID_PESSOA'
    GRAU = 'DS_GRAU_ACADEMICO_DISCENTE'
    
    def __init__(self, filename, program_code, year2dataset, **kwargs):
        super().__init__(filename, None, csv_delim=';', **kwargs)
        self.program_code = str(program_code)
        self.year2dataset = year2dataset

    def upgrade_fields(self, d):
        r = dict()
        for k, v in d.items():
            k = self.FIELD_UPGRADES[k] if k in self.FIELD_UPGRADES else k
            r[k] = v
        return r

    def download(self, **kwargs):
        filepath = self._get_filepath(**kwargs)
        if not os.path.isfile(filepath):
            with open(filepath, 'w', encoding='utf-8', newline='') as out:
                writer = None
                fields = []
                l = list(self.year2dataset.keys())
                l.sort()
                for y in l:
                    with self.year2dataset[y].open(**kwargs) as f:
                        l_fields = map(lambda x: x.strip(), f.readline().split(';'))
                        is_novel = lambda n: n not in fields and \
                                        n not in self.FIELD_UPGRADES
                        fields += list(filter(is_novel, l_fields))
                writer = csv.DictWriter(out, fieldnames=fields, delimiter=';')
                writer.writeheader()
                for k in sorted(l, reverse=True):
                    v = self.year2dataset[k]
                    print(f'Filtering for program {self.program_code} in {v}')
                    with v.open_csv(**kwargs) as in_csv:
                        for row in in_csv:
                            matcher = lambda x: isinstance(row[x], str) and \
                                self.program_code in row[x]
                            if any(map(matcher, row.keys())):
                                writer.writerow(self.upgrade_fields(row))
            if self.GRAU in fields and self.ID in fields:
                with open(filepath, 'r', encoding='utf-8', newline='') as in_f, \
                     open(filepath+'.tmp', 'w', encoding='utf-8', newline='') \
                         as out_f:
                     reader = csv.DictReader(in_f, delimiter=';')
                     raw = [x for x in reader]
                     writer = csv.DictWriter(out_f, fieldnames=fields,
                                             delimiter=';')
                     writer.writeheader()
                     for i in range(len(raw)):
                         id_pessoa, grau = raw[i][self.ID], raw[i][self.GRAU]
                         if not any(map(lambda x: x[self.ID]==id_pessoa and \
                                        x[self.GRAU]==grau, raw[:i])):
                             writer.writerow(raw[i])
                os.replace(filepath+'.tmp', filepath)
        return filepath        

    
class GoogleCalendar(Dataset):
    def __init__(self, filename, calendarId,
                 key_file = SERVICE_ACCOUNT_FILE, **kwargs):
        super().__init__(filename, None, **kwargs)
        self.calendarId = calendarId
        self.key_file = key_file

    def download(self, directory=None, force=False, **kwargs):
        filepath = self._get_filepath(directory=directory)
        if not force and os.path.isfile(filepath):
            return filepath
        creds = service_account.Credentials.from_service_account_file(
            self.key_file,
            scopes=['https://www.googleapis.com/auth/calendar.readonly'])
        service = googleapiclient.discovery.build('calendar', 'v3', credentials=creds)
        all_r, next_token = None, None
        while True:
            r = service.events().list(calendarId=self.calendarId,
                                      pageToken=next_token).execute()
            if all_r == None:
                all_r = r
            else:
                all_r['items'] += r['items']
            next_token = r.get('nextPageToken')
            if not next_token:
                break
        with open(filepath, 'w', encoding='utf-8', newline='\n') as out:
            json.dump(all_r, out, indent=2)
        return filepath

    
class GoogleCalendarCSV(Dataset):
    FIELDS = ['tipo', 'discente', 'orientador', 'coorientador', 'data_ymd']
    RX_TYPE = re.compile(r'(?i)^\s*(Defesa|(?:Exame\s+(?:de)?\s+)?Qualifica\S+o|Semin\S+rio(?:\s*(?:de\s*)?andamento\s*)?|SAD|EQD|EQM)\s*(?:de)?\s*(Mestrado|Doutorado|)\s*(?:\((?:SAD|EQM|EQD)\))?\s*(?:de|-|:)?\s*(.*)')
    RX_EATEN_NEWLINE = re.compile('(T\S+TULO|LOCAL|DATA(.*HORA)|(CO-?)ORIENTADORA?):?\s*$')
    RX_ORIENTADOR = re.compile(      r'(?i)ORIENTADORA?:\s*(?:prof.?\.?)?\s*(?:dr.?\.)?\s*((?:\w| \w\.| )+)')
    RX_COORIENTADOR = re.compile(r'(?i)CO-?ORIENTADORA?:\s*(?:prof.?\.?)?\s*(?:dr.?\.)?\s*((?:\w| \w\.| )+)')
    RX_DATA = re.compile(r'^([0-9]+-[0-9]+-[0-9]+)')
    RX_SPACE = re.compile(r'  +')
    
    def __init__(self, filename, calendarDataset, **kwargs):
        super().__init__(filename, None, **kwargs)
        self.calendar = calendarDataset

    def __cleanup_name(self, s):
        s = self.RX_EATEN_NEWLINE.sub('', unidecode(s.strip()))
        return self.RX_SPACE.sub(' ', s.upper())

    def __add_to_dict(self, d, key, rx, string):
        m = rx.search(string)
        if m != None:
            d[key] = self.__cleanup_name(m.group(1))
        
    def parse_event(self, event):
        m = self.RX_TYPE.search(event['summary'])
        if m == None:
            return None
        tipo = m.group(1).strip().lower()
        if tipo == 'defesa':
            tipo = 'DF'
        elif tipo in ['eqm', 'eqd', 'sad']:
            tipo = tipo.upper()
        elif any(map(lambda x: x in tipo, ['exame', 'qualifica'])):
            tipo = 'EQ'
        else:
            tipo = 'SA'
        if len(tipo) < 3:
            if len(m.group(2)) == 0:
                return None
            tipo += m.group(2)[0].upper()
        d = {'tipo': tipo,
             'discente': self.__cleanup_name(m.group(3))}
        if 'description' in event:
            descr = event['description']
            self.__add_to_dict(d, 'orientador', self.RX_ORIENTADOR, descr)
            self.__add_to_dict(d, 'coorientador', self.RX_COORIENTADOR, descr)
        if 'start' in event and 'dateTime' in event['start']:
            m = self.RX_DATA.search(event['start']['dateTime'])
            if m != None:
                d['data_ymd'] = m.group(1)
        return d

    def download(self, directory=None, force=False, **kwargs):
        filepath = self._get_filepath(directory=directory, **kwargs)
        if not force and os.path.isfile(filepath):
            return filepath
        with self.calendar.open(directory=directory, force=force,
                                **kwargs) as json_f:
            o = json.load(json_f)
            with open(filepath, 'w', newline='') as csv_f:
                writer = csv.DictWriter(csv_f, fieldnames=self.FIELDS)
                writer.writeheader()
                for entry in o['items']:
                    d = self.parse_event(entry)
                    if d != None:
                        writer.writerow(d)
        return filepath

def _tolerant_int(e, **kwargs):
    try:
        matcher = re.search(r'(-?[0-9]+)', e)
        s = '' if matcher is None else matcher.group(1)
        if 'empty' in kwargs and s == '':
            return kwargs['empty']
        return int(s)
    except ValueError:
        pass

def _get_html_int(html, selector, **kwargs):
    if html == None:
        return None
    e = html.find(selector, first=('idx' not in kwargs))
    if 'idx' in kwargs:
        idx = kwargs['idx']
        e = e[idx] if len(e) > idx else None
    return _tolerant_int(e.text) if e != None else None

def _get_html_ints(html, selector, **kwargs):
    if html == None:
        return None
    return [_tolerant_int(x.text, **kwargs) for x in html.find(selector)]

    
class Scholar(Dataset):
    __URL_BASE = 'https://scholar.google.com.br/citations?user='
    MAIN_FIELDS = ['docente', 'scholar_id', 'documents', 'citations', \
                   'docs-citing', 'docs-citing-5', 'h-index', 'h5-index']
    WORKS_FIELDS = ['year', 'citations', 'authors', 'title', 'venue']
    AUTHORS_FMT = {'sep' : ';', 'order' : 'FIRST_FIRST', 'super_compact': True}
    
    def __init__(self, docentes_dataset, basename='scholar',
                 delay_bounds_secs=(7, 27), short_fraction=4,
                 **kwargs):
        super().__init__(basename+'.csv', None, **kwargs)
        self.basename = basename
        self.docentes_dataset = docentes_dataset
        self.delay_bounds_secs = delay_bounds_secs
        self.short_fraction = short_fraction
        self.session = requests_html.HTMLSession()
        self.delay_pending = False

    def __str__(self):
        return f'Scholar({self.docentes_dataset}, dir={self.directory})'

    def feed_works_sink(self, tbody_html, works_sink):
        for tds in [tr.find('td') for tr in tbody_html.find('tr.gsc_a_tr')]:
            entry = {}
            entry['title']     = tds[0].find('a', first=True).text
            entry['authors']   = tds[0].find('div')[0].text.replace(',', ';')
            entry['venue']     = tds[0].find('div')[1].text
            entry['citations'] = _tolerant_int(tds[1].text, empty=0)
            entry['year']      = _tolerant_int(tds[2].text, empty=0)
            works_sink(entry)

    def scrap_works(self, html, url, works_sink):
        TRS_SELECTOR = 'tr td.gsc_a_c a.gsc_a_ac'
        SELECTOR = '#gsc_a_t tbody ' + TRS_SELECTOR
        values = []
        window = [0,20]
        values += _get_html_ints(html, SELECTOR, empty=0)
        self.feed_works_sink(html.find('#gsc_a_t tbody', first=True), works_sink)
        while len(values) >= window[1]:
            window = [window[1], window[1]+60]
            json = self.session.post(url + f'&cstart={window[0]}&pagesize'
                                     + f'={window[1]}', data='json=1')
            json = json.json()
            payload = json['B'].strip()
            if payload != '':
                trs = requests_html.HTML(html=payload)
                values += _get_html_ints(trs, TRS_SELECTOR, empty=0)
                self.feed_works_sink(trs, works_sink)
        return {'count': len(values), 'citation-counts': values}
        
    def fetch(self, scholar_id, works_sink):
        if self.delay_pending:
            p = self.delay_bounds_secs
            secs = randint(p[0], p[1])
            print(f'{self}: Sleeping {secs} seconds')
            sleep(secs)
        self.delay_pending = True
        url = self.__URL_BASE + scholar_id
        print(f'{self}: Fetching {url}')
        html = self.session.get(url).html
        rows = html.find('#gsc_rsb_st tbody tr',)[:2]
        if len(rows) < 2:
            raise RuntimeError(f'{url} has {len(rows)} rows in metric tables!' +
                               ' Scholar was redesigned or realised i\'m ' +
                               'not human')
        works = self.scrap_works(html, url, works_sink)
        citations = sum(works['citation-counts'])
        print(f'Fetched {works["count"]} documents and {citations} citations for {scholar_id}')
        return {
            'scholar_id': scholar_id,
            'documents': works['count'],
            'citations': citations,
            'docs-citing': _get_html_int(rows[0], 'td.gsc_rsb_std'),
            'docs-citing-5': _get_html_int(rows[0], 'td.gsc_rsb_std', idx=1),
            'h-index': _get_html_int(rows[1], 'td.gsc_rsb_std'),
            'h5-index': _get_html_int(rows[1], 'td.gsc_rsb_std', idx=1)
        }

    def download(self, directory=None, force=False, **kwargs):
        directory = self.directory if directory == None else directory
        filepath = self._get_filepath(directory=directory, **kwargs)
        if not force and os.path.isfile(filepath):
            return filepath
        workspath = os.path.join(directory, self.basename+'-works.csv')
        with self.docentes_dataset.open_csv() as reader, \
             open(filepath, 'w', newline='', encoding='utf-8') as main_f, \
             open(workspath, 'w', newline='', encoding='utf-8') as works_f:
            m_writer = csv.DictWriter(main_f, fieldnames=self.MAIN_FIELDS)
            m_writer.writeheader()
            w_writer = csv.DictWriter(works_f, fieldnames=self.WORKS_FIELDS)
            w_writer.writeheader()
            for row in reader:
                sch_id = row['scholar_id']
                if sch_id != None and sch_id.strip() != '':
                    d = self.fetch(sch_id, lambda x: w_writer.writerow(x))
                    d['docente'] = row['docente']
                    m_writer.writerow(d)


class ScholarFile(Dataset):
    def __init__(self, scholar, suffix, **kwargs):
        filename = scholar.basename + suffix + '.csv'
        super().__init__(filename, None, **kwargs)
        self.scholar = scholar
        if suffix == '-works':
            self.FIELDS = Scholar.WORKS_FIELDS
            self.AUTHORS_FMT = Scholar.AUTHORS_FMT

    def download(self, **kwargs):
        self.scholar.download(**kwargs)
        return self._get_filepath(**kwargs)

class ScopusQuery(Dataset):
    def __init__(self, docentes, filename='scopus.qry', **kwargs):
        super().__init__(filename, None, **kwargs)
        self.docentes = docentes

    def download(self, force=True, **kwargs):
        filepath = self._get_filepath(**kwargs)
        if not force and os.path.isfile(filepath):
            return filepath
        with self.docentes.open_csv(**kwargs) as docs, \
             open(filepath, 'w', encoding='utf-8') as out:
            ids = filter(bool, map(str.strip, map(lambda x: x['scopus_id'], docs)))
            out.write(' OR '.join(map(lambda i: f'AU-ID({i})', ids)))
        return filepath

class ScopusWorks(InputDataset):
    AUTHORS_FMT = {'sep' : ',', 'order' : 'LAST_FIRST'}
        
    def __init__(self, qry, filename='scopus-works.csv', **kwargs):
        if 'encoding' not in kwargs:
            kwargs['encoding'] = 'utf-8-sig' #skip BOM
        super().__init__(filename, **kwargs)
        self.qry = qry

    def download(self, force=False, **kwargs):
        filepath = self._get_filepath(**kwargs)
        if not force and os.path.isfile(filepath):
            return filepath
        qry_filepath = self.qry.download(**kwargs)
        with open(qry_filepath, 'r') as qry_f:
           pyperclip.copy(qry_f.read())
        msg = f'Submit the query string at {qry_filepath} to Scopus and ' + \
              f'export the CSV to {filepath}. The query string has been ' + \
              f'placed on your clipboard. In Scopus interface, select the ' + \
              f'whole Citation Information and Bibliographical ' + \
              f'Information columns.'
        print(msg)
        raise FileNotFoundError(errno.ENOENT,
                                f'File {filepath} not found! ' + msg, filepath)

class CPCWorks(Dataset):
    AUTHORS_FMT = {'sep' : ';', 'order' : ','}
    ID = '1vhjisGxmd17uwEqjhegcyo-yYnUnNGZVPhBPPeJbqZQ'
    RX_FIRST_SENTENCE = re.compile(r'(?i)(.*?\w\w+)\.')
    RX_ABBBREVS = re.compile(r'(?i)^((?:\s*\w\w+)?(?:\s+\w[. ])*)\s*')
    RX_HTML = re.compile(r'&\w+;')
    
    def __init__(self, filename='cpc.csv', sheetId=None, \
                 key_file=SERVICE_ACCOUNT_FILE, **kwargs):
        super().__init__(filename, None, **kwargs)
        sheetId = sheetId if sheetId != None else self.ID
        self.sheetId = sheetId
        self.credentials = service_account.Credentials.from_service_account_file(
            key_file, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        self.service = googleapiclient.discovery.build('sheets', 'v4', \
                                                       credentials=self.credentials)
        self.sheets = self.service.spreadsheets()

    def get_authors(self, text):
        pieces = text.split(' . ')
        if len(pieces) > 1:
            return pieces[0]
        text = self.RX_HTML.sub('', text)
        pieces = text.split(';')
        bad_pieces = list(map(lambda x: len(x) > 50, pieces))
        if True in bad_pieces:
            pieces = pieces[:bad_pieces.index(True)+1]
            pieces[-1] = pieces[-1][:50]
            m = self.RX_FIRST_SENTENCE.search(pieces[-1])
            if m != None:
                pieces[-1] = m.group(1)
        pieces, extra = pieces[:-1], pieces[-1]
        e_pieces = extra.split(',')[:2]
        if len(e_pieces) < 2:
            return ';'.join(pieces)
        m = self.RX_ABBBREVS.search(e_pieces[1])
        if m == None:
            return ';'.join(pieces)
        e_pieces[1] = m.group(1)
        return '; '.join(pieces + [', '.join(e_pieces)])
        
    def download(self, force=True, **kwargs):
        filepath = self._get_filepath(**kwargs)
        if not force and os.path.isfile(filepath):
            return filepath
        range_address = 'Artigos!A1:S20000'
        ranges = self.sheets.values()\
                             .batchGet(spreadsheetId=self.sheetId, \
                                       majorDimension='ROWS', \
                                       ranges=range_address) \
                             .execute().get('valueRanges', [])
        if len(ranges) < 1:
            raise ValueError(f'Got no ranges from sheet {self.sheetId}. ' + \
                             f'Asked for {range_address}')
        rows = ranges[0].get('values')
        artigo_idx = rows[0].index('Artigo')
        header = rows[0][:artigo_idx] + ['autores'] + rows[0][artigo_idx:]
        header = [x.replace('\n', ' ').strip() for x in header]
        rows = rows[1:]
        with open(filepath, mode='w', newline='', encoding=self.encoding) as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for row in rows:
                c_row = [x.replace('\n', ' ').strip() for x in row]
                c_row = c_row[:artigo_idx ] + [self.get_authors(c_row[artigo_idx])] \
                      + c_row[ artigo_idx:]
                writer.writerow(c_row)


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
                
class Bibliometrics(Dataset):
    FIELDS = ['group', 'pub_year', 'base_year', 'source',
              'h', 'h5', 'documents', 'citations']

    def __init__(self, linhas, filename='bibliometrics-year.csv', \
                 scopus=None, scholar=None, base_year=None, **kwargs):
        super().__init__(filename, None, **kwargs)
        self.linhas = linhas
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
                r[cited_f] = _tolerant_int(r[cited_f], empty=0)
                r[year_f] = _tolerant_int(r[year_f])
            self._write_metrics('all', src_name, rows, base_year,
                                fields, dict_sink)
            with self.linhas.open_csv() as linhas_reader:
                linhas = [r for r in linhas_reader]
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

class BibliometricsAggregate(Dataset):
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
                r[c] = _tolerant_int(r[c], empty=0)
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
        
   
SUC_DISCENTES = {
    2018: SucupiraDataset('suc-dis-2018.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/b7003093-4fab-4b88-b0fa-b7d8df0bcb77/resource/37fde9f4-bb94-4806-85d4-5d744f7f76ef/download/br-capes-colsucup-discentes-2018-2019-10-01.csv'),
    2017: SucupiraDataset('suc-dis-2017.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/b7003093-4fab-4b88-b0fa-b7d8df0bcb77/resource/2207af02-21f6-466e-a690-46f26a2804d6/download/ddi-br-capes-colsucup-discentes-2017-2018-07-01.csv'),
    2016: SucupiraDataset('suc-dis-2016.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/dc2568b7-20b0-4d92-980d-dcf2485b5517/resource/cfbcb060-d6af-4c34-baa7-16ef259273f7/download/br-capes-colsucup-discentes-2013a2016-2017-12-02_2016.csv'),
    2015: SucupiraDataset('suc-dis-2015.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/dc2568b7-20b0-4d92-980d-dcf2485b5517/resource/08e7765f-cd76-4c7b-a29a-46e216dd79cf/download/br-capes-colsucup-discentes-2013a2016-2017-12-02_2015.csv'),
    2014: SucupiraDataset('suc-dis-2014.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/dc2568b7-20b0-4d92-980d-dcf2485b5517/resource/3aa223ba-9c60-421a-91af-48ed843a9a98/download/br-capes-colsucup-discentes-2013a2016-2017-12-02_2014.csv'),
    2013: SucupiraDataset('suc-dis-2013.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/dc2568b7-20b0-4d92-980d-dcf2485b5517/resource/89bcb419-5a11-46a1-804e-e9df8e4e6097/download/br-capes-colsucup-discentes-2013a2016-2017-12-02_2013.csv'),
}

SUC_DISCENTES_PPGCC = SucupiraProgram('suc-dis-ppgcc.csv', '41001010025', SUC_DISCENTES)
    
PPGCC_CALENDAR = GoogleCalendar('calendar.json', 'ppgccnuvem@gmail.com')
PPGCC_CALENDAR_CSV = GoogleCalendarCSV('calendar.csv', PPGCC_CALENDAR)

DOCENTES = InputDataset('docentes.csv')
LINHAS = InputDataset('linhas.csv')
SCHOLAR_CSV = Scholar(DOCENTES)
SCHOLAR_WORKS_CSV = ScholarFile(SCHOLAR_CSV, suffix='-works')

SCOPUS_QUERY = ScopusQuery(DOCENTES)
SCOPUS_WORKS_CSV = ScopusWorks(SCOPUS_QUERY)

BIBLIOMETRICS = Bibliometrics(LINHAS,
                              scopus=SCOPUS_WORKS_CSV,
                              scholar=SCHOLAR_WORKS_CSV)
BIBLIOMETRICS_AGGREGATE = BibliometricsAggregate(BIBLIOMETRICS)

def fix_all_names():
    names.fix_csv_names([DOCENTES, PPGCC_CALENDAR_CSV],
                        ['docente', 'orientador'],
                        read_only=[0], allow_ambiguous=True)
    names.fix_csv_names([DOCENTES, PPGCC_CALENDAR_CSV],
                        ['docente', 'coorientador'],
                        read_only=[0], allow_ambiguous=True)
    names.fix_csv_names([DOCENTES, SUC_DISCENTES_PPGCC],
                        ['docente', 'NM_ORIENTADOR_PRINCIPAL'],
                        read_only=[0], allow_ambiguous=True)
    names.fix_csv_names([SUC_DISCENTES_PPGCC, PPGCC_CALENDAR_CSV],
                        ['NM_DISCENTE', 'discente'],
                        read_only=[0], allow_ambiguous=True)
    
