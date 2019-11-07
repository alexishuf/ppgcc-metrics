# -*- coding: utf-8 -*-
from .context import datasets
import unittest
import csv
import lzma
import json
import tempfile
from os.path import join, isfile, abspath, dirname
from pkg_resources import resource_string, resource_stream, resource_listdir


class SucupiraTests(unittest.TestCase):
    def testAvoidDownload(self):
        with tempfile.TemporaryDirectory() as d:
            with lzma.open(join(d, datasets.SUC_DISCENTES[2013].filename),
                           'wt', encoding='utf-8') as f:
                f.write('a;b;c\n' +
                        '1;41001010025P2;joão\n')
            with datasets.SUC_DISCENTES[2013].open_csv(directory=d) as reader:
                self.assertEqual(reader.fieldnames, ['a', 'b', 'c'])
                data = [x for x in reader]
                self.assertEqual(len(data), 1)
                self.assertEqual(data[0]['a'], '1')
                self.assertEqual(data[0]['c'], 'joão')
                
    def testFilterProgram(self):
        with tempfile.TemporaryDirectory() as d:
            y2ds = {2013: datasets.SUC_DISCENTES[2013],
                    2018: datasets.SUC_DISCENTES[2018]}
            with lzma.open(join(d, y2ds[2013].filename), 'wt',
                           encoding='utf-8') as f:
                f.write('a;b;NM_ORIENTADOR\n' +
                        '1;41001010025P2;joão\n' +
                        '2;41001010024P2;alice\n')
            with lzma.open(join(d, y2ds[2018].filename), 'wt') as f:
                f.write('a;c;b;NM_ORIENTADOR_PRINCIPAL\n' +
                        '3;x;41001010025P2;fritz\n' +
                        '4;y;41001010024P2;hans\n')
            prgm = datasets.SucupiraProgram('ppgcc.csv', '41001010025', y2ds)
            with prgm.open_csv(directory=d) as reader:
                self.assertEqual(reader.fieldnames,
                                 ['a', 'b', 'c', 'NM_ORIENTADOR_PRINCIPAL'])
                fieldcount = len(reader.fieldnames)
                data = [x for x in reader]
                self.assertEqual(len(data), 2)
                self.assertEqual([x['a'] for x in data], ['1', '3'])
                self.assertEqual(data[0]['c'], '')
                self.assertEqual(data[0]['NM_ORIENTADOR_PRINCIPAL'], 'joão')


    def testReplaceCSV(self):
        with tempfile.TemporaryDirectory() as d:
            with lzma.open(join(d, 'f.csv'), 'wt', newline='\r\n', \
                           encoding='utf-8') as f:
                f.write('a;b\n1;2\n')
            ds = datasets.SucupiraDataset('f.csv', None, directory=d)
            with ds.open_csv() as r:
                self.assertEqual([dict(d) for d in r], [{'a':'1', 'b':'2'}])
            with ds.replace_csv() as w:
                w.writerow({'a':3, 'b':4})
            with ds.open_csv() as r:
                self.assertEqual([dict(d) for d in r], [{'a':'3', 'b':'4'}])


class GoogleCalendarTests(unittest.TestCase):
    def setUp(self):
        self.key_file = datasets.SERVICE_ACCOUNT_FILE
        if not isfile(self.key_file):
            self.key_file = abspath(join(dirname(__file__), self.key_file))
        if not isfile(self.key_file):
            self.key_file = None
        self.calendar_json = resource_string('tests.resources', 'calendar.json')

    def testFetchEvents(self):
        if not self.key_file:
            self.skipTest('No Google API key file.')
        with tempfile.TemporaryDirectory() as d:
            cal = datasets.GoogleCalendar('calendar.json',
                                          calendarId='ppgccnuvem@gmail.com',
                                          key_file=self.key_file)
            with open(cal.download(), 'r') as f:
                o = json.load(f)
                self.assertTrue('items' in o,
                                msg='there is no "items" key in results JSON')
                self.assertTrue(len(o['items']) > 0, f'No events in calendar!')


class GoogleCalendarCSVTests(unittest.TestCase):
    def setUp(self):
        self.calendar_json = resource_stream('tests.resources', 'calendar.json')
        self.tmp = tempfile.TemporaryDirectory()
        self.calendar = datasets.GoogleCalendar('calendar.json',
                                                'ppgccnuvem@gmail.com',
                                                directory=self.tmp.name)
        with open(join(self.tmp.name, 'calendar.json'), 'w') as f:
            f.write(self.calendar_json.read().decode())
        self.cal_csv = datasets.GoogleCalendarCSV('calendar.csv',
                                                  self.calendar,
                                                  directory=self.tmp.name)
        
    def tearDown(self):
        self.tmp.cleanup()
        self.calendar_json.close()

    def testSummaryMismatch(self):
        d = self.cal_csv.parse_event({'summary': 'asd', 'description': '',
                                      'start': {'dateTime':'2019-01-02'}})
        self.assertEqual(d, None)

    def testSummaryMatchNoInfo(self):
        get_d = lambda x: self.cal_csv.parse_event({'summary' : x})
        ex = {'tipo': 'DFM', 'discente': 'FULANO DA SILVA'}
        self.assertEqual(get_d('Defesa de Mestrado - Fulano da Silva'), ex)
        self.assertEqual(get_d('Defesa de Mestrado - Fulano da Silvã'), ex)
        self.assertEqual(get_d('Defesa de Mestrado Fulano da Silva'), ex)
        self.assertEqual(get_d('Defesa de Mestrado: Fulano  da Silva'), ex)
        self.assertEqual(get_d('Defesa de Mestrado: Fulano da Silva   '), ex)
        self.assertEqual(get_d('Defesa de Mestrado: Fúlano  da Silvã   '), ex)

    def testParseExamType(self):
        get = lambda x: self.cal_csv.parse_event({'summary': x + ' Fulano'})['tipo']
        self.assertEqual(get('Defesa de Mestrado'), 'DFM')
        self.assertEqual(get('Defesa Mestrado'), 'DFM')
        self.assertEqual(get('Defesa de doutorado'), 'DFD')
        self.assertEqual(get('Defesa doutorado'), 'DFD')
        self.assertEqual(get('Exame de Qualificação de Mestrado'), 'EQM')
        self.assertEqual(get('Exame de qualificação de Mestrado'), 'EQM')
        self.assertEqual(get('Exame de Qualificação Mestrado'), 'EQM')
        self.assertEqual(get('Qualificação de Mestrado'), 'EQM')
        self.assertEqual(get('Qualificação Mestrado'), 'EQM')
        self.assertEqual(get('Exame de Qualificação de Doutorado'), 'EQD')
        self.assertEqual(get('Exame de qualificação de doutorado'), 'EQD')
        self.assertEqual(get('Exame de Qualificação Doutorado'), 'EQD')
        self.assertEqual(get('Qualificação de Doutorado'), 'EQD')
        self.assertEqual(get('Qualificação doutorado'), 'EQD')
        self.assertEqual(get('Seminário de Andamento de Doutorado'), 'SAD')
        self.assertEqual(get('Seminário de Andamento Doutorado'), 'SAD')
        self.assertEqual(get('Seminário Andamento de Doutorado'), 'SAD')
        
        self.assertEqual(get('Exame de Qualificação de Mestrado (EQM)'), 'EQM')
        self.assertEqual(get('Qualificação de Mestrado (EQM)'), 'EQM')
        self.assertEqual(get('Exame de Qualificação de Doutorado (EQD)'), 'EQD')
        self.assertEqual(get('Exame de Qualificação Doutorado (EQD)'), 'EQD')
        self.assertEqual(get('Seminário de Andamento de Doutorado (SAD)'), 'SAD')
        self.assertEqual(get('Seminário Andamento Doutorado (SAD)'), 'SAD')
        
        self.assertEqual(get('SAD'), 'SAD')
        self.assertEqual(get('EQD'), 'EQD')
        self.assertEqual(get('EQM'), 'EQM')
        self.assertEqual(get('SAD -'), 'SAD')
        self.assertEqual(get('EQD -'), 'EQD')
        self.assertEqual(get('EQM -'), 'EQM')

    def testParseDate(self):
        get_d = lambda x: self.cal_csv.parse_event(
            {'summary' : 'Defesa de Doutorado - Fulano',
             'start': {'dateTime': x}})
        ex_bad = {'tipo': 'DFD', 'discente': 'FULANO'}
        ex = dict(ex_bad)
        ex['data_ymd'] = '2019-03-26'
        self.assertEqual(get_d('2019-03-26T17:00:00-03:00'), ex)
        self.assertEqual(get_d('2019-03-26T17:00:00-0300'), ex)
        self.assertEqual(get_d('2019-03-26T17:00:00'), ex)
        self.assertEqual(get_d('2019-03-26T17:00'), ex)
        self.assertEqual(get_d('2019-03-26'), ex)
        self.assertEqual(get_d('2019/03/26'), ex_bad)

    def testOrientador(self):
        get = lambda x: self.cal_csv.parse_event(
            {'summary' : 'Seminário de andamento de Doutorado (SAD) - Fulano',
             'description': x})['orientador']
        ex = 'CICLANO SAURO'
        self.assertEqual(get('asd\nORIENTADOR: Ciclano Sauro'), ex)
        self.assertEqual(get('asd\nORIENTADOR: Ciclano Sauro.'), ex)
        self.assertEqual(get('asd\nORIENTADOR: Ciclano Sauro\n'), ex)
        self.assertEqual(get('ORIENTADOR: Ciclano Sauro'), ex)
        self.assertEqual(get('ORIENTADOR:Ciclano Sauro'), ex)
        self.assertEqual(get('ORIENTADOR: \t Ciclano Sauro'), ex)
        self.assertEqual(get('asd\nORIENTADOR: Ciclano Sauro\n' +
                             'COORIENTADORA: Beltrana.\n'), ex)
        self.assertEqual(get('asd\nORIENTADOR: Ciclano Sauro' +
                             'COORIENTADORA: Beltrana.\n'), ex)
        self.assertEqual(get('asd\nORIENTADOR: Ciclano Sauro.' +
                             'COORIENTADORA: Beltrana.\n'), ex)

    def testParseJson(self):
        with self.cal_csv.open_csv() as reader:
            data = [x for x in reader]
            self.assertEqual(len(data), 3)
            self.assertEqual(data[0]['tipo'], 'DFM')
            self.assertEqual(data[0]['discente'], 'LUCAS VIANA KNOCHENHUAER')
            self.assertEqual(data[0]['orientador'], 'CARINA FRIEDRICH DORNELES')
            self.assertEqual(data[0]['coorientador'], '')
            self.assertEqual(data[0]['data_ymd'], '2019-02-27')
            
            self.assertEqual(data[1]['tipo'], 'SAD')
            self.assertEqual(data[1]['discente'], 'FELIPE SCHNEIDER COSTA')
            self.assertEqual(data[1]['orientador'], 'MARIO ANTONIO RIBEIRO DANTAS')
            self.assertEqual(data[1]['coorientador'], 'SILVIA MODESTO NASSAR')
            self.assertEqual(data[1]['data_ymd'], '2018-11-07')
            
            self.assertEqual(data[2]['tipo'], 'EQM')
            self.assertEqual(data[2]['discente'], 'LAIS BORIN')
            self.assertEqual(data[2]['orientador'], 'MARCIO CASTRO')
            self.assertEqual(data[2]['coorientador'], 'PATRICIA DELLA MEA PLENTZ')
            self.assertEqual(data[2]['data_ymd'], '2018-06-05')
        

class InputDatasetTests(unittest.TestCase):
    def testRequireExistence(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(FileNotFoundError) as cm:
                ds = datasets.InputDataset('input.csv')
                with ds.open_csv() as reader:
                    print(reader.fieldnames)
                
    def testOpensExisting(self):
        with tempfile.TemporaryDirectory() as d:
            with open(join(d, 'input.csv'), 'w', newline='\r\n') as f:
                f.write('a,b\n')
                f.write('1,2\n')
            ds = datasets.InputDataset('input.csv')
            with ds.open_csv(directory=d) as reader:
                self.assertEqual(reader.fieldnames, ['a', 'b'])
                data = [x for x in reader]
                self.assertEqual(len(data), 1)
                self.assertEqual(data[0]['b'], '2')

    def testReplaceCSV(self):
        with tempfile.TemporaryDirectory() as d:
            with open(join(d, 'f.csv'), 'w', newline='\r\n') as f:
                f.write('a,b\n1,2\n')
            ds = datasets.InputDataset('f.csv')
            with ds.replace_csv(directory=d) as w:
                w.writerow({'a' : 3, 'b': 4})
            with ds.open_csv(directory=d) as r:
                self.assertEqual([dict(d) for d in r], [{'a': '3', 'b': '4'}])
            

            
if __name__ == '__main__':
    unittest.main()
