# -*- coding: utf-8 -*-
import requests
import os.path
import os
import lzma
import re
import csv

class Dataset:
    def __init__(self, name, provider_name, url, directory='.'):
        self.filename = name
        self.provider_name = provider_name
        self.url = url
        self.directory = directory

    def download(self, directory=None, force=False, **kwargs):
        directory = self.directory if directory == None else directory
        filepath = os.path.join(directory, self.filename)
        if not os.path.isdir(directory):
            os.makedirs(directory)
        if force or not os.path.isfile(filepath):
            with open(filepath, 'b') as out:
                r = requests.get(self.url, stream=True)
                for chunk in r.iter_content(chunk_size=2048):
                    out.write(chunk)
        return filepath

    def open(self, **kwargs):
        newline = kwargs['newline'] if 'newline' in kwargs else None
        encoding = kwargs['encoding'] if 'encoding' in kwargs else None
        return open(self.download(**kwargs), 'r',
                    newline=newline, encoding=encoding)

    
class SucupiraDataset(Dataset):
    def __init__(self, name, url, directory='.'):
        super().__init__(name, 'sucupira', url, directory)

    def download(self, directory=None, force=False, **kwargs):
        directory = self.directory if directory == None else directory
        filepath = os.path.join(directory, self.filename)
        if not os.path.isdir(directory):
            os.makedirs(directory)
        if not force and os.path.isfile(filepath):
            return filepath
        assert False
        print(f'Downloading {self.url}')
        r = requests.get(self.url, stream=True)
        r.encoding = 'iso-8859-1'
        with lzma.open(filepath, 'wt', encoding='utf-8', newline='\r\n') as xz:
            for line in r.iter_lines(decode_unicode=True):
                xz.write(line)
        print(f'Downloaded {self.url} into {filepath}')
        return filepath

    def open(self, **kwargs):
        newline = kwargs['newline'] if 'newline' in kwargs else None
        return lzma.open(self.download(**kwargs), 'rt',
                         newline=newline, encoding='utf-8')

    
class SucupiraProgram(Dataset):
    FIELD_UPGRADES = {'NM_ORIENTADOR' : 'NM_ORIENTADOR_PRINCIPAL'}
    
    def __init__(self, filename, program_code, year2dataset, directory='.'):
        super().__init__(filename, None, None, directory)
        self.program_code = program_code
        self.year2dataset = year2dataset

    def upgrade_fields(self, d):
        r = dict()
        for k, v in d.items():
            k = self.FIELD_UPGRADES[k] if k in self.FIELD_UPGRADES else k
            r[k] = v
        return r

    def download(self, **kwargs):
        directory = kwargs['directory'] if 'directory' in kwargs \
                    else self.directory
        filepath = os.path.join(directory, self.filename)
        if not os.path.isdir(directory):
            os.makedirs(directory)
        if not os.path.isfile(filepath):
            with open(filepath, 'w', encoding='utf-8', newline='') as out:
                writer = None
                l = list(self.year2dataset.keys())
                l.sort()
                with self.year2dataset[l[-1]].open(newline='', **kwargs) as f:
                    fields = list(map(lambda x: x.strip(), f.readline().split(';')))
                    writer = csv.DictWriter(out, fieldnames=fields)
                    writer.writeheader()
                for k, v in self.year2dataset.items():
                    with v.open(newline='', **kwargs) as f:
                        in_csv = csv.DictReader(f, delimiter=';')
                        for row in in_csv:
                            matcher = lambda x: self.program_code in row[x]
                            if any(map(matcher, row.keys())):
                                writer.writerow(self.upgrade_fields(row))
        return filepath


SUC_DISCENTES = {
    2018: SucupiraDataset('suc-dis-2018.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/b7003093-4fab-4b88-b0fa-b7d8df0bcb77/resource/37fde9f4-bb94-4806-85d4-5d744f7f76ef/download/br-capes-colsucup-discentes-2018-2019-10-01.csv'),
    2017: SucupiraDataset('suc-dis-2017.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/b7003093-4fab-4b88-b0fa-b7d8df0bcb77/resource/2207af02-21f6-466e-a690-46f26a2804d6/download/ddi-br-capes-colsucup-discentes-2017-2018-07-01.csv'),
    2016: SucupiraDataset('suc-dis-2016.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/dc2568b7-20b0-4d92-980d-dcf2485b5517/resource/cfbcb060-d6af-4c34-baa7-16ef259273f7/download/br-capes-colsucup-discentes-2013a2016-2017-12-02_2016.csv'),
    2015: SucupiraDataset('suc-dis-2015.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/dc2568b7-20b0-4d92-980d-dcf2485b5517/resource/08e7765f-cd76-4c7b-a29a-46e216dd79cf/download/br-capes-colsucup-discentes-2013a2016-2017-12-02_2015.csv'),
    2014: SucupiraDataset('suc-dis-2014.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/dc2568b7-20b0-4d92-980d-dcf2485b5517/resource/3aa223ba-9c60-421a-91af-48ed843a9a98/download/br-capes-colsucup-discentes-2013a2016-2017-12-02_2014.csv'),
    2013: SucupiraDataset('suc-dis-2013.csv.xz', 'https://dadosabertos.capes.gov.br/dataset/dc2568b7-20b0-4d92-980d-dcf2485b5517/resource/89bcb419-5a11-46a1-804e-e9df8e4e6097/download/br-capes-colsucup-discentes-2013a2016-2017-12-02_2013.csv'),
}
    
