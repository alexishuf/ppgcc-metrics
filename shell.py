# -*- coding: utf-8 -*-
from ppgcc_metrics import datasets as ds
from ppgcc_metrics import derived as de
import itertools

ALL_DATASETS = list(itertools.chain(
    filter(lambda x: isinstance(getattr(ds, x), ds.Dataset), dir(ds)),
    filter(lambda x: isinstance(getattr(de, x), ds.Dataset), dir(de))
))

def get_all(**kwargs):
    for m in [ds, de]:
        for d in filter(lambda x: isinstance(x, ds.Dataset), \
                        map(lambda x: getattr(m, x), dir(m))):
            print(f'Downloading data for {d}...')
            d.download(**kwargs)
    print(f'Download & processing completed for all datasets')

if __name__ == '__main__':
    print('\n--=[ ppgcc-metrics interactive shell ]=--\n' +
            '    (actually, just an IPython shell)\n'
          '\n' +
          'Use get_all() to ensure all datasets are available. Use the ' +
          'force=True parameter to force re-download and/or re-processing.\n' +
          'Available datasets:')
    has_pending = False
    for m_name in ['ds', 'de']:
        m = eval(m_name)
        for nm in filter(lambda x: isinstance(getattr(m, x), ds.Dataset), dir(m)):
            d = getattr(m, nm)
            st = ''
            if not d.is_ready():
                st = ' [PENDING]'
                has_pending = True
            print(f'  {m_name}.{nm:30} at data/{str(d):30}{st}')
    if has_pending:
        print('\nThere are pending datasets. Use their .download() method or ' +
              'the get_all() function.')
        
            
