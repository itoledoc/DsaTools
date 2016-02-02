#!/usr/bin/env python
import time
import os

import pandas as pd
import DsaDataBase3 as Data
import DsaAlgorithm3 as Dsa
import warnings

from sqlalchemy import create_engine
from astropy.utils.data import download_file
from astropy.utils import iers
try:
    iers.IERS.iers_table = iers.IERS_A.open(
        download_file(iers.IERS_A_URL, cache=True))
except OSError:
    iers.IERS.iers_table = iers.IERS_A.open(
        download_file(iers.IERS_A_URL, cache=False))
warnings.simplefilter(action="ignore", category=RuntimeWarning)

engine = create_engine(
        'postgresql://dsacore:dsa2020@tableau.alma.cl:5432/dsa_data')
refr = False

try:
    path = os.environ['APDM_PREFIX']
except KeyError:
    path = os.environ['HOME'] + '/.apdm_'

try:
    if time.time() - os.path.getmtime(path + '_tabl_all/') > 3600.:
        refr = True
except OSError:
    os.mkdir(path + '_tabl_all/')
    refr = True

try:
    datas = Data.DsaDatabase3(refresh_apdm=refr, path=path + '_tabl_all/',
                              allc2=False, loadp1=False)
except IOError:
    datas = Data.DsaDatabase3(path=path + '_tabl_all/',
                              allc2=False, loadp1=False)

dsa = Dsa.DsaAlgorithm3(datas)

dsa.write_ephem_coords()
dsa.static_param()
pwv = pd.read_sql('pwv_data', engine).pwv.values[0]
dsa.selector(
    minha=-3., maxha=3., letterg=['A', 'B', 'C'],
    conf=['C36-2'], pwv=pwv)
dsa.selection_df['PWV now'] = pwv
dsa.selection_df['PWV now date'] = (
    pd.read_sql('pwv_data', engine).date.values[0] + ' ' +
    pd.read_sql('pwv_data', engine).time.values[0])
dsa.selection_df['date'] = str(dsa._ALMA_ephem.date)
dsa.selection_df['arrayname'] = 'N/A'

dsa.master_dsa_df['allconfs'] = dsa.obs_param.apply(
    lambda x: ','.join(
        [str(x['C36_1']), str(x['C36_2']), str(x['C36_3']), str(x['C36_4']),
         str(x['C36_5']), str(x['C36_6']), str(x['C36_7']), str(x['C36_8'])]),
    axis=1)

dsa.inputs.to_sql('inputs_tableau', engine, index_label='Cycle',
                  if_exists='replace')
print('inputs written')
dsa.master_dsa_df.to_sql('master_tableau', engine, index_label='SBUID',
                         if_exists='replace')
print('master written')
dsa.obs_param.to_sql('staticparam_tableau', engine, index_label='SBUID',
                     if_exists='replace')
print('stat param written')

visits = pd.merge(dsa.data.visits, dsa.data.sciencegoals[['OUS_ID', 'SG_ID']],
                  on='SG_ID', how='left')

temp_param = pd.merge(dsa.data.temp_param,
                      dsa.data.sciencegoals[['OUS_ID', 'SG_ID']],
                      on='SG_ID', how='left')

visits.to_sql('sgvisits_tableau', engine, index_label='SBUID',
              if_exists='replace')
temp_param.to_sql('temp_param_tableau', engine, index_label='SBUID',
                  if_exists='replace')

dsa.data.aqua_execblock.to_sql('aqua_tableau', engine, index_label='SBUID',
                               if_exists='replace')

datas._cursor.close()
datas._connection.close()
