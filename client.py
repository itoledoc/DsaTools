import xmlrpclib
import pandas as pd
from sqlalchemy import create_engine

pd.options.display.width = 200
pd.options.display.max_columns = 100

client = xmlrpclib.Server('http://red-osf.osf.alma.cl:7080/')
engine = create_engine('postgresql://wto:wto2020@dmg02.sco.alma.cl:5432/aidadb')


def rundsa(
        array_kind='TWELVE-M',
        bands=('ALMA_RB_03', 'ALMA_RB_04', 'ALMA_RB_06',
               'ALMA_RB_07', 'ALMA_RB_08', 'ALMA_RB_09',
               'ALMA_RB_10'),
        conf='',
        cal_blratio=False,
        numant=0,
        array_id='',
        horizon=20,
        minha=-3.,
        maxha=3.,
        pwv=0.5,
        timestring=''):

    r = client.run(array_kind, bands, conf, cal_blratio, numant, array_id,
                   horizon, minha, maxha, pwv, timestring)

    return pd.read_json(r, orient='index')


def rundsa_full(
        array_kind='TWELVE-M',
        bands=('ALMA_RB_03', 'ALMA_RB_04', 'ALMA_RB_06',
               'ALMA_RB_07', 'ALMA_RB_08', 'ALMA_RB_09',
               'ALMA_RB_10'),
        conf='',
        cal_blratio=False,
        numant=0,
        array_id='',
        horizon=20,
        minha=-3.,
        maxha=3.,
        pwv=0.5,
        timestring=''):

    r = client.run_full(array_kind, bands, conf, cal_blratio, numant, array_id,
                        horizon, minha, maxha, pwv, timestring)

    return pd.read_json(r, orient='index')


def get_arrays(array_kind):

    r = client.get_arrays(array_kind)

    return r


def get_ar(array_id):

    r = client.get_ar(array_id)

    return r


def update_data():

    client.update_data()


def update_apdm(obsprojectuid):

    client.update_apdm(obsprojectuid)


def get_pwv():

    return client.get_pwv()

