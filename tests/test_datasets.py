# -*- coding: utf-8 -*-
from .context import datasets
import unittest
import csv
import lzma
import tempfile
from os.path import join


class SucupiraTests(unittest.TestCase):
    def testAvoidDownload(self):
        with tempfile.TemporaryDirectory() as d:
            with lzma.open(join(d, datasets.SUC_DISCENTES[2013].filename),
                           'wt', encoding='utf-8') as f:
                f.write('a;b;c\n' +
                        '1;41001010025P2;joão\n')
            with datasets.SUC_DISCENTES[2013].open(directory=d) as f:
                reader = csv.DictReader(f, delimiter=';')
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
            with prgm.open(directory=d) as f:
                reader = csv.DictReader(f)
                self.assertEqual(reader.fieldnames,
                                 ['a', 'c', 'b', 'NM_ORIENTADOR_PRINCIPAL'])
                fieldcount = len(reader.fieldnames)
                data = [x for x in reader]
                self.assertEqual(len(data), 2)
                self.assertEqual([x['a'] for x in data], ['1', '3'])
                self.assertEqual(data[0]['c'], '')
                self.assertEqual(data[0]['NM_ORIENTADOR_PRINCIPAL'], 'joão')



if __name__ == '__main__':
    unittest.main()
