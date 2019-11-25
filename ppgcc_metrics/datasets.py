# -*- coding: utf-8 -*-
import requests
import requests_html
import os.path
import os
import errno
import lzma
import gzip
import re
import csv
import json
import textract
import googleapiclient.discovery
import pyperclip
from datetime import datetime, date
from random import randint
from itertools import chain, product
from ppgcc_metrics import names
from time import sleep
from contextlib import contextmanager
from google.oauth2 import service_account
from unidecode import unidecode

SERVICE_ACCOUNT_FILE = 'service-account-key.json'

_SIMPLIFY_SUBTITLE_RX = re.compile(r'(?i)\s*:([^:]*)$')
_SIMPLIFY_TITLE_RX = re.compile(r'(?i)[:.,;-]|(^| )(of|for|from|to|in(to)?|an?|the)( |$)')
_DEDUP_SPACES_RX = re.compile(r'  +')

def simplify_title(title):
    title = _SIMPLIFY_SUBTITLE_RX.sub('' , title)
    title = _SIMPLIFY_TITLE_RX   .sub(' ', title)
    return  _DEDUP_SPACES_RX     .sub(' ', title)

class Dataset:
    def __init__(self, name, url, directory='data', non_trivial=False,
                 csv_delim=',', encoding='utf-8'):
        self.filename = name
        self.url = url
        self.directory = directory
        self.csv_delim = csv_delim
        self.encoding = encoding
        self.non_trivial = non_trivial

    def __str__(self):
        return self.filename

    def _get_filepath(self, directory=None, create_dir=True, **kwargs):
        directory = self.directory if directory == None else directory
        if not os.path.isdir(directory):
            os.makedirs(directory)
        return os.path.join(directory, self.filename)

    def is_ready(self, **kwargs):
        directory = kwargs.get('directory', self.directory)
        return os.path.isfile(os.path.join(directory, self.filename))
        
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
            na_rx = re.compile('\s*N[AÃ]O +SE +APLICA\s*;')
            for line in r.iter_lines(decode_unicode=True):
                line = na_rx.sub('NA;', line)
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

RX_SUC_DATE = re.compile(r'^(?i)\s*(\d?\d)[. -]?([a-z]+)[- .]?(\d+)(\D|$)')
SUC_MONTHS_PT = ['', 'JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN',
                     'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ']
SUC_MONTHS_EN = ['', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                     'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
def suc_date2date(suc_date):
    try:
        m = RX_SUC_DATE.search(suc_date)
        if m != None:
            year = int(m.group(3) if len(m.group(3)) == 4 else '20'+m.group(3))
            month = m.group(2).upper().strip()[:3]
            if month in SUC_MONTHS_PT:
                month = SUC_MONTHS_PT.index(month)
            elif month in SUC_MONTHS_EN:
                month = SUC_MONTHS_EN.index(month)
            else:
                month = datetime.strptime(month, '%b').month
            return date(year, month, int(m.group(1)))
    except:
        pass
    return suc_date
def suc_date2iso(suc_date):
    d = suc_date2date(suc_date)
    return d.strftime('%Y-%m-%d') if isinstance(d, date) else d

def iso2suc_date(iso):
    return date2suc_date(datetime.fromisoformat(iso).date())

def date2suc_date(d):
    if d == None:
        return None
    try:
        base = f'{d.day:02}{SUC_MONTHS_EN[d.month]}{d.year}'
        if isinstance(d, date) and not isinstance(d, datetime):
            return base + ':00:00:00'
        return base + d.strftime(':%H:%M:%S')
    except:
        return None
    
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
    FIELDS = ['tipo', 'discente', 'orientador', 'coorientador', 'data_ymd', '']
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

def tolerant_int(e, **kwargs):
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
    return tolerant_int(e.text) if e != None else None

def _get_html_ints(html, selector, **kwargs):
    if html == None:
        return None
    return [tolerant_int(x.text, **kwargs) for x in html.find(selector)]

    
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

    def feed_works_sink(self, tbody_html, works_sink):
        for tds in [tr.find('td') for tr in tbody_html.find('tr.gsc_a_tr')]:
            entry = {}
            entry['title']     = tds[0].find('a', first=True).text
            entry['authors']   = tds[0].find('div')[0].text.replace(',', ';')
            entry['venue']     = tds[0].find('div')[1].text
            entry['citations'] = tolerant_int(tds[1].text, empty=0)
            entry['year']      = tolerant_int(tds[2].text, empty=0)
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
        with open(workspath, 'r', newline='', encoding='utf-8') as in_f, \
             open(workspath+'.tmp', 'w', newline='', encoding='utf-8') as out_f:
            reader = csv.DictReader(in_f)
            writer = csv.DictWriter(out_f, fieldnames=reader.fieldnames)
            writer.writeheader()
            visited = []
            for r in reader:
                k = (r['authors'], simplify_title(r['title']))
                is_same = lambda p: p[1]==k[1] or \
                               names.same_authors(p[0], k[0], **self.AUTHORS_FMT)
                if not any(map(is_same, visited)):
                    visited.append(k)
                    writer.writerow(r)
        os.replace(workspath+'.tmp', workspath)
        return filepath


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
        return filepath

    def doi_getter(self):
        try:
            with self.open_csv() as reader:
                h = next(filter(lambda x: DOI in x.upper(), reader.fieldnames))
                rx = re.compile(r'(10\.\d\d\d\d/.*)$')
                def matcher(d):
                    if h not in d or not d[h]:
                        return None
                    m = rx.search(d[h])
                    return m.group(1) if m else None
                return matcher
        except:
            return lambda x: None

class SecretariaDiscentes(Dataset):
    ID = '1GUhX1Ql3Ky0BzOuIo9CPdKKQg4TIpuW7VYc7MKyl15c'
    GRAUS = {'DO': 'DOUTORADO', 'ME': 'MESTRADO'}
    FIELDS = ['NM_DISCENTE', 'DS_GRAU_ACADEMICO_DISCENTE',
              'DT_MATRICULA_DISCENTE', 'DT_SITUACAO_DISCENTE',
              'NM_ORIENTADOR_PRINCIPAL', 'NM_COORIENTADOR',
              'PRORROG_1', 'PRORROG_2', 'SEM_TRANCAMENTOS',
              'DT_TERMINO', 'DT_TERMINO_ISO',
              'ST_PROF_LING_1', 'ST_PROF_LING_2',
              'ST_SAD', 'ST_QUALIFICACAO', 'NUM_SEMINARIOS']
    RX_COADV = re.compile(r'(?i)\s*Coorientadora?:?\s*([^\)\]]*)\)?\]?')
    
    def __init__(self, docentes, filename='secretaria.csv', **kwargs):
        super().__init__(filename, None, **kwargs)
        self.sheetId = kwargs.get('sheetId', self.ID)
        self.key_file = kwargs.get('key_file', SERVICE_ACCOUNT_FILE)
        self.docentes_ds = docentes
        self.docentes = None

    def canon_advidsor(self, name):
        if not self.docentes:
            with self.docentes_ds.open_csv() as reader:
                self.docentes = {r['docente'] for r in reader}
        canon_f = lambda x: names.canon_name(name, x)
        return next(chain(filter(None, map(canon_f, self.docentes)), [name]))

    def parse_name(self, text):
        lines = list(filter(len, text.split('\n')))
        if not len(lines):
            return None
        name = names.clean_name(lines[0])
        for l in lines[1:]:
            m = self.RX_COADV.search(l)
            if m:
                return (name, names.clean_name(m.group(1)))
        return (name, None)
    
    def download(self, force=False, **kwargs):
        filepath = self._get_filepath(kwargs.get('directory'))
        if not force and os.path.isfile(filepath):
            return filepath
        
        creds = service_account.Credentials.from_service_account_file(
            self.key_file, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        service = googleapiclient.discovery.build('sheets', 'v4',
                                                  credentials=creds)
        sheets = service.spreadsheets()
        range_address = 'Controle!A1:P500'
        ranges = sheets.values().batchGet(spreadsheetId=self.sheetId,
                                            majorDimension='ROWS',
                                            ranges=range_address)\
                                  .execute().get('valueRanges', [])
        if len(ranges) < 1:
            raise ValueError(f'Got no ranges from sheet {self.sheetId}. ' + \
                             f'Asked for {range_address}')
        data = ranges[0].get('values')
        with open(filepath, 'w', newline='', encoding=self.encoding) as out_f:
            writer = csv.DictWriter(out_f, fieldnames=self.FIELDS)
            writer.writeheader()
            for adv_row in filter(None, map(lambda x: x[0] if len(x[1])==1 else None,
                                            zip(range(len(data)), data))):
                advisor = self.canon_advidsor(data[adv_row][0])
                i = adv_row + 2
                while i < len(data) and len(data[i]) > 3 and len(data[i][3]):
                    grau = self.GRAUS.get(data[i][4].strip().upper())
                    name, coadvisor = self.parse_name(data[i][3])
                    matr = datetime.strptime(data[i][5].strip(), '%d/%m/%Y')
                    term = datetime.strptime(data[i][9].strip(), '%d/%m/%Y')
                    writer.writerow({
                        self.FIELDS[ 0]: name,
                        self.FIELDS[ 1]: grau,
                        self.FIELDS[ 2]: date2suc_date(matr),
                        self.FIELDS[ 3]: date2suc_date(date.today()),
                        self.FIELDS[ 4]: advisor,
                        self.FIELDS[ 5]: coadvisor,
                        self.FIELDS[ 6]: data[i][6],  #prorrog_1
                        self.FIELDS[ 7]: data[i][7],  #prorrog_2
                        self.FIELDS[ 8]: data[i][8],  #tranc
                        self.FIELDS[ 9]: date2suc_date(term),
                        self.FIELDS[10]: term.isoformat(),
                        self.FIELDS[11]: data[i][10], #prof ing
                        self.FIELDS[12]: data[i][11], #prof 2
                        self.FIELDS[13]: data[i][12], #sad
                        self.FIELDS[14]: data[i][13], #qualify
                        self.FIELDS[15]: data[i][14], #seminarios
                    })
                    i += 1
        return filepath


class CompressedCSV(InputDataset):
    def __init__(self, filename, message='', **kwargs):
        super().__init__(filename, **kwargs)
        self.message = message

    def download(self, **kwargs):
        filepath = self._get_filepath(**kwargs)
        if os.path.isfile(filepath+'.gz'):
            return filepath+'.gz'
        if os.path.isfile(filepath+'.xz'):
            return filepath+'.xz'
        if self.message:
            print(self.message)
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filepath)

    def open(self, **kwargs):
        return self._open(self.download(**kwargs), 'r', **kwargs)

    def _open(self, filepath, mode, **kwargs):
        mod = {'.gz': gzip, '.xz': lzma}.get(filepath[-3:])
        if not mod:
            raise ValueError(f'Unknown extension {filepath[-3:]}')
        return mod.open(filepath, mode+'t',
                        newline=kwargs.get('newline'), encoding='utf-8')


class DiscentesCAPGCNPJ(Dataset):
    # RX_PDF = re.compile(r'([0-9]{11})\s*([^\n]+)\n[^0-9]*\d,\d\d\s*(\d\d/\d\d/\d\d\d\d)\s*')
    RX_PDF = re.compile(r'([0-9]{11})\s*([^\n]+)\n')
    FIELDS = ['cpf', 'discente', 'cnpj', 'data_entrada_sociedade']
    
    def __init__(self, filename, pdfs_dir, socios, **kwargs):
        super().__init__(filename, None, **kwargs)
        self.non_trivial = not socios.is_ready()
        self.pdfs_dir = pdfs_dir
        self.socios = socios
        
    def clean_cpf(self, cpf):
        if not isinstance(cpf, str) or len(cpf) < 11:
            cpf = f'{int(str(cpf)):011d}'
        return cpf
        
    def _merge(self, row_d, cpf, nome):
        cpf_socio = row_d['cnpj_cpf_do_socio']
        if not cpf_socio or len(cpf_socio.strip()) == 0:
            return None
        cpf, cpf_socio = cpf.strip(), cpf_socio.strip()
        if len(cpf) != len(cpf_socio):
            return None
        for i in range(len(cpf)):
            if cpf_socio[i] != '*' and cpf[i] != cpf_socio[i]:
                return None
        if not names.same_name(nome, row_d['nome_socio']):
            return None
        return {'cpf': cpf, 'discente': nome, 'cnpj': row_d['cnpj'],
                'data_entrada_sociedade': row_d['data_entrada_sociedade']}

    def _match_student(self, row_d, student_pairs):
        for cpf, nome in student_pairs:
            m = self._merge(row_d, cpf, nome)
            if m:
                return m
        
    def __create(self, filepath, **kwargs):
        directory = kwargs.get('directory', self.directory)
        pdfs_dir = os.path.join(directory, self.pdfs_dir)
        students = []
        for pdf in os.listdir(pdfs_dir):
            pdfpath = os.path.join(pdfs_dir, pdf)
            s = textract.process(pdfpath)
            if not s:
                continue
            s = s.decode('utf-8')
            students += self.RX_PDF.findall(s)
        students = [(self.clean_cpf(c), names.clean_name(n)) \
                    for c,n in students]
        print(f'Looking for {len(students)} students in ~26 million CNPJs')
        print('This will take many HOURS. Will print every 100 thousand rows')
        with open(filepath, 'w', newline='', encoding=self.encoding) as out_f, \
             self.socios.open_csv() as socios:
            writer = csv.DictWriter(out_f, fieldnames=self.FIELDS)
            writer.writeheader()
            wrote = 0
            read = 0
            for r in socios:
                merged = self._match_student(r, students)
                if merged:
                    writer.writerow(merged)
                    print(f'CNPJ {merged["cnpj"]} for {merged["discente"]}')
                read += 1
                if read % 100000 == 0:
                    print(f'read {read} lines')
        return wrote
        
    def download(self, **kwargs):
        filepath = self._get_filepath(**kwargs)
        if not os.path.isfile(filepath):
            self.__create(filepath, **kwargs)
        return filepath

class DiscentesCAPGCNPJDetails(Dataset):
    def __init__(self, filename, capg_cnpj, empresas, **kwargs):
        super().__init__(filename, None, **kwargs)
        self.capg_cnpj = capg_cnpj
        self.empresas = empresas
        self.non_trivial = not capg_cnpj.is_ready() or not empresas.is_ready()

    def download(self, force=False, **kwargs):
        filepath = self._get_filepath(**kwargs)
        if not force and os.path.isfile(filepath):
            return filepath
        fields, in_dicts = [], dict()
        with self.capg_cnpj.open_csv() as reader:
            fields = reader.fieldnames
            for x in reader:
                in_dicts[x['cnpj']] = x
        with self.empresas.open_csv() as empresas, \
             open(filepath, 'w', encoding=self.encoding, newline='') as out_f:
            fields += list(filter(lambda x: x not in fields, empresas.fieldnames))
            writer = csv.DictWriter(out_f, fieldnames=fields)
            writer.writeheader()
            read = 0
            for x in empresas:
                if x['cnpj'] in in_dicts:
                    writer.writerow({**in_dicts[x['cnpj']], **x})
                read += 1
                if read % 1000000 == 0:
                    print(f'Processed {read/1000000}M rows in empresas.csv.gz')
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

SUC_DOCENTES = {
    2018: SucupiraDataset('suc-doc-2018.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/57f86b23-e751-4834-8537-e9d33bd608b6/resource/08d2a6bd-ac0f-4c25-ab89-8457288e15a6/download/br-capes-colsucup-docente-2018-2019-10-01.csv'),
    2017: SucupiraDataset('suc-doc-2017.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/57f86b23-e751-4834-8537-e9d33bd608b6/resource/d918d02e-7180-4c7c-be73-980f9a8c09b5/download/br-capes-colsucup-docente-2017-2018-08-10.csv'),
}
    
PPGCC_CALENDAR = GoogleCalendar('calendar.json', 'ppgccnuvem@gmail.com')
PPGCC_CALENDAR_CSV = GoogleCalendarCSV('calendar.csv', PPGCC_CALENDAR)

DOCENTES = InputDataset('docentes.csv')
LINHAS = InputDataset('linhas.csv')
SCHOLAR_CSV = Scholar(DOCENTES)
SCHOLAR_WORKS_CSV = ScholarFile(SCHOLAR_CSV, suffix='-works')

SCOPUS_QUERY = ScopusQuery(DOCENTES)
SCOPUS_WORKS_CSV = ScopusWorks(SCOPUS_QUERY)

CPC_CSV = CPCWorks()
SECRETARIA_DISCENTES = SecretariaDiscentes(DOCENTES)

SOCIOS_BRASIL = CompressedCSV('socio.csv', message='' +\
                              'Download data using https://github.com/' +\
                              'turicas/socios-brasil. Pass --no_censorship ' +\
                              'to ./run.sh in order to get CPFs',
                              non_trivial = True)
EMPRESAS_BRASIL = CompressedCSV('empresa.csv', message='' +\
                                'Download data using https://github.com/' +\
                                'turicas/socios-brasil.',
                                non_trivial = True)
CAPG_CNPJ = DiscentesCAPGCNPJ('capg-cnpj.csv', 'capg_pdfs', SOCIOS_BRASIL)
CAPG_CNPJ_DETAILS = DiscentesCAPGCNPJDetails('capg-cnpj-details.csv', CAPG_CNPJ, EMPRESAS_BRASIL)

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
    names.fix_csv_names([SUC_DISCENTES_PPGCC, CAPG_CNPJ],
                        ['NM_DISCENTE', 'discente'],
                        read_only=[0], allow_ambiguous=True)
    names.fix_csv_names([SUC_DISCENTES_PPGCC, CAPG_CNPJ_DETAILS],
                        ['NM_DISCENTE', 'discente'],
                        read_only=[0], allow_ambiguous=True)
    
