# -*- coding: utf-8 -*-
import requests
import os.path
import os
import errno
import lzma
import re
import csv
import json
import googleapiclient.discovery
from contextlib import contextmanager
from google.oauth2 import service_account
from unidecode import unidecode

SERVICE_ACCOUNT_FILE = 'service-account-key.json'

class Dataset:
    def __init__(self, name, url, directory='data'):
        self.filename = name
        self.url = url
        self.directory = directory
        self.csv_delim = ','
        self.encoding = None

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

class InputDataset(Dataset):
    def __init__(self, filename, directory='data'):
        super().__init__(filename, None, directory=directory)
        self.encoding='utf-8'

    def download(self, directory=None, **kwargs):
        filepath = self._get_filepath(directory=directory)
        if not os.path.isfile(filepath):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filepath)
        return filepath
        
    
class SucupiraDataset(Dataset):
    def __init__(self, name, url, directory='data'):
        super().__init__(name, url, directory)
        self.csv_delim = ';'
        self.encoding = 'utf-8'

    def download(self, directory=None, force=False, **kwargs):
        filepath = self._get_filepath(directory=directory)
        if not force and os.path.isfile(filepath):
            return filepath
        print(f'Downloading {self.url}')
        r = requests.get(self.url, stream=True)
        r.encoding = 'iso-8859-1'
        with lzma.open(filepath+'.tmp', 'wt',
                       encoding='utf-8', newline='\r\n') as xz:
            for line in r.iter_lines(decode_unicode=True):
                xz.write(line)
        os.replace(filepath+'.tmp', filepath)
        print(f'Downloaded {self.url} into {filepath}')
        return filepath

    def open(self, **kwargs):
        newline = kwargs['newline'] if 'newline' in kwargs else None
        return lzma.open(self.download(**kwargs), 'rt',
                         newline=newline, encoding='utf-8')

    
class SucupiraProgram(Dataset):
    FIELD_UPGRADES = {'NM_ORIENTADOR' : 'NM_ORIENTADOR_PRINCIPAL'}
    
    def __init__(self, filename, program_code, year2dataset, directory='data'):
        super().__init__(filename, None, directory)
        self.program_code = program_code
        self.year2dataset = year2dataset
        self.csv_delim = ';'
        self.encoding = 'utf-8'

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
                l = list(self.year2dataset.keys())
                l.sort()
                with self.year2dataset[l[-1]].open(**kwargs) as f:
                    fields = list(map(lambda x: x.strip(), f.readline().split(';')))
                    writer = csv.DictWriter(out, fieldnames=fields, delimiter=';')
                    writer.writeheader()
                for k, v in self.year2dataset.items():
                    with v.open_csv(**kwargs) as in_csv:
                        for row in in_csv:
                            matcher = lambda x: self.program_code in row[x]
                            if any(map(matcher, row.keys())):
                                writer.writerow(self.upgrade_fields(row))
        return filepath        

    
class GoogleCalendar(Dataset):
    def __init__(self, filename, calendarId,
                 key_file = SERVICE_ACCOUNT_FILE, **kwargs):
        super().__init__(filename, None, **kwargs)
        self.calendarId = calendarId
        self.key_file = key_file
        self.encoding = 'utf-8'

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
    RX_TYPE = re.compile(r'(?i)^\s*(Defesa|(?:Exame\s+(?:de)?\s+)?Qualifica\S+o|Semin\S+rio(?:\s*(?:de\s*)?andamento\s*)?|SAD|EQD|EQM)\s*(?:de)?\s*(Mestrado|Doutorado|)\s*(?:\((?:SAD|EQM|EQD)\))?\s*[-:]?\s*(.*)')
    RX_EATEN_NEWLINE = re.compile('(T\S+TULO|LOCAL|DATA(.*HORA)|(CO-?)ORIENTADORA?):?\s*$')
    RX_ORIENTADOR = re.compile(      r'(?i)ORIENTADORA?:\s*(?:prof.?\.?)?\s*(?:dr.?\.)?\s*((?:\w| )+)')
    RX_COORIENTADOR = re.compile(r'(?i)CO-?ORIENTADORA?:\s*(?:prof.?\.?)?\s*(?:dr.?\.)?\s*((?:\w| )+)')
    RX_DATA = re.compile(r'^([0-9]+-[0-9]+-[0-9]+)')
    RX_SPACE = re.compile(r'  +')
    
    def __init__(self, filename, calendarDataset, **kwargs):
        super().__init__(filename, None, **kwargs)
        self.calendar = calendarDataset
        self.csv_delim = ','
        self.encoding = 'utf-8'

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
        
        
    
    
SUC_DISCENTES = {
    2018: SucupiraDataset('suc-dis-2018.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/b7003093-4fab-4b88-b0fa-b7d8df0bcb77/resource/37fde9f4-bb94-4806-85d4-5d744f7f76ef/download/br-capes-colsucup-discentes-2018-2019-10-01.csv'),
    2017: SucupiraDataset('suc-dis-2017.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/b7003093-4fab-4b88-b0fa-b7d8df0bcb77/resource/2207af02-21f6-466e-a690-46f26a2804d6/download/ddi-br-capes-colsucup-discentes-2017-2018-07-01.csv'),
    2016: SucupiraDataset('suc-dis-2016.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/dc2568b7-20b0-4d92-980d-dcf2485b5517/resource/cfbcb060-d6af-4c34-baa7-16ef259273f7/download/br-capes-colsucup-discentes-2013a2016-2017-12-02_2016.csv'),
    2015: SucupiraDataset('suc-dis-2015.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/dc2568b7-20b0-4d92-980d-dcf2485b5517/resource/08e7765f-cd76-4c7b-a29a-46e216dd79cf/download/br-capes-colsucup-discentes-2013a2016-2017-12-02_2015.csv'),
    2014: SucupiraDataset('suc-dis-2014.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/dc2568b7-20b0-4d92-980d-dcf2485b5517/resource/3aa223ba-9c60-421a-91af-48ed843a9a98/download/br-capes-colsucup-discentes-2013a2016-2017-12-02_2014.csv'),
    2013: SucupiraDataset('suc-dis-2013.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/dc2568b7-20b0-4d92-980d-dcf2485b5517/resource/89bcb419-5a11-46a1-804e-e9df8e4e6097/download/br-capes-colsucup-discentes-2013a2016-2017-12-02_2013.csv'),
}
    
PPGCC_CALENDAR = GoogleCalendar('calendar.json', 'ppgccnuvem@gmail.com')
PPGCC_CALENDAR_CSV = GoogleCalendarCSV('calendar.csv', PPGCC_CALENDAR)
