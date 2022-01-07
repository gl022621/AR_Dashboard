import pandas as pd
import numpy as np
import os
import csv
import re
from datetime import datetime, timedelta
import teradata
import pysftp
# import teradata
# import keyring
# import getpass
# from sqlalchemy import create_engine, types
# from sqlalchemy import create_engine
# import sqlalchemy_teradata
# import sqlalchemy_teradata
# import time
# import antenneregisterparse


############################ FUNCTIONS ###########################
def xy_to_latitude(x, y):
    K = [[0, 3235.65389, -0.24750, -0.06550],
         [-0.00738, -0.00012, 0, 0],
         [-32.58297, -0.84978, -0.01709, -0.00039],
         [0, 0, 0, 0],
         [0.00530, 0, 0, 0]]
    x0 = 155000.0
    y0 = 463000.0
    phi0 = 52.15517440
    dx = (x-x0)/(10**5)
    dy = (y-y0)/(10**5)
    ls = []
    for p in range(5):
        for q in range(4):
            ls.append(K[p][q]*(dx**p)*(dy**q))
    # print(ls)
    return phi0 + sum(ls)/3600


def xy_to_longitude(x, y):
    L = [[0, 0.01199, 0.00022, 0, 0],
         [5260.52916, 105.94684, 2.45656, 0.05594, 0.00128],
         [-0.00022, 0, 0, 0, 0],
         [-0.81885, -0.05607, -0.00256, 0, 0],
         [0, 0, 0, 0, 0],
         [0.00026, 0, 0, 0, 0]]
    x0 = 155000.0
    y0 = 463000.0
    lambda0 = 5.38720621
    dx = (x-x0)/(10**5)
    dy = (y-y0)/(10**5)
    ls = []
    for p in range(6):
        for q in range(5):
            ls.append(L[p][q]*(dx**p)*(dy**q))
    # print(ls)
    return lambda0 + sum(ls)/3600


def value_in_the_range(value: float, ls: list):
    lst = []
    for i in ls:
        if value > i[0] and value < i[1]:
            lst.append(True)
        else:
            lst.append(False)
    return sum(lst)


# def operator(frequencyValue, frequencyListTMobile, frequencyListKPN, frequencyListVodafone, frequencyListTele2):
#     if value_in_the_range(frequencyValue, frequencyListTMobile) == 1:
#         return 'T-Mobile'
#     elif value_in_the_range(frequencyValue, frequencyListKPN) == 1:
#         return 'KPN'
#     elif value_in_the_range(frequencyValue, frequencyListVodafone) == 1:
#         return 'Vodafone'
#     elif value_in_the_range(frequencyValue, frequencyListTele2) == 1:
#         return 'Tele2'
#     else:
#         return 'Unidentified'

def operator(frequencyValue, frequencyListTMobile, frequencyListKPN, frequencyListVodafone):
    if value_in_the_range(frequencyValue, frequencyListTMobile) == 1:
        return 'T-Mobile'
    elif value_in_the_range(frequencyValue, frequencyListKPN) == 1:
        return 'KPN'
    elif value_in_the_range(frequencyValue, frequencyListVodafone) == 1:
        return 'Vodafone'
    else:
        return 'Unidentified'

def frequency_band(value):
    if value < 600:
        return 'Other'
    if value < 788:
        return '700'
    if value < 880:
        return '800'
    elif value < 1000:
        return '900'
    elif value < 1500:
        return '1400'
    elif value < 1900:
        return '1800'
    elif value < 2200:
        return '2100'
    elif ((value > 2500 and value <= 2565) or (value > 2620 and value <= 2685)):
        return '2600FDD'
    elif ((value > 2565 and value <= 2620) or (value > 2685 and value <= 2690)):
        return '2600TDD'
    elif value > 3100 and value<3800:
        return '3500'
    elif value >= 3800:
        return 'Other'
    else:
        return 'Other'


def technology(value):
    if value == 'LTE':
        return '4G'
    elif value == 'UMTS':
        return '3G'
    elif value == 'GSM 900' or value == 'GSM 1800':
        return '2G'
    elif value == 'NB-IoT':
        return 'NB-IoT'
    elif value == '5G NR':
        return '5G'
    else: 'Other'


def technology1(value):
    if value == 'LTE':
        return 'LTE'
    elif value == 'UMTS':
        return 'UMTS'
    elif value == 'GSM 900' or value == 'GSM 1800':
        return 'GSM'
    elif value == 'NB-IoT':
        return 'NB-IoT'
    elif value == '5G NR':
        return 'NR'
    else: 'Other'


def outdoor_macro(row):
    row = row.fillna('15')
    # print(row)
    # '-' in row['Hoofdstraalrichting'] or row['Vermogen'] < 15:
    # print(row['Hoofdstraalrichting'])
    # print(('0' if row['Hoofdstraalrichting'] is None else row['Hoofdstraalrichting']))
    if '-' in row['Hoofdstraalrichting'] or float(row['Vermogen']) < 15:
        if (row['Technology'] == 'NB-IoT') or (row['Band'] == '2600TDD' and row['Operator'] == 'KPN' and row['Vermogen']==12.3):
            return 'Yes'
        else:
            return 'No'
    else:
        return 'Yes'





def lat(row):
    return round(xy_to_latitude(row['x'], row['y']), 15)


def lon(row):
    return round(xy_to_longitude(row['x'], row['y']), 15)


def recent_file_list_in_directory(directory, datetime):
    dir= directory
    day = datetime
    searchstring = 'opt_{}'.format(day.strftime('%Y%m'))
    # print('opt_{}'.format(day.strftime('%Y%m')))
    regex = re.compile(".*({}).*".format(searchstring))
    return [m.group(0) for l in os.listdir(dir) for m in [regex.search(l)] if m]


def GHz_to_MHz(row):
    # print(row)
    if 'ghz' in row.lower():
        row = row.lower().replace(' ghz', '')
        # print(row)
        if '-' in row:
            [row_1, row_2] = row.split('-', 1)
            # print(str(int(1000*float(row_1)))+'-'+str(int(1000*float(row_2)))+' MHz')
            row = str(int(1000 * float(row_1))) + '-' + str(int(1000 * float(row_2))) + ' MHz'
        else:
            # print(str(int(1000 * float(row))) + ' MHz')
            row = str(int(1000 * float(row))) + ' MHz'
    elif 'khz' in row.lower():
        row = row.lower().replace(' khz', '')
        # print(row)
        if '-' in row:
            [row_1, row_2] = row.split('-', 1)
            # print(str(int(1000*float(row_1)))+'-'+str(int(1000*float(row_2)))+' MHz')
            row = str(int(float(row_1)/1000)) + '-' + str(int(float(row_2)/1000)) + ' MHz'
        else:
            # print(str(int(1000 * float(row))) + ' MHz')
            row = str(int(float(row)/1000)) + ' MHz'
    return row


#################################################################

def main():
    GHz_to_MHz('3.59-3.595 GHz')
    GHz_to_MHz('3.595 GHz')
    GHz_to_MHz('3598 MHz')
    desired_width = 320
    pd.set_option('display.width', desired_width)
    pd.set_option('display.max_columns', 20)

    # today = datetime.today()
    # print(today)
    # pastDate = datetime.strptime('20210228', '%Y%m%d')
    # print(pastDate)

    dt = datetime.today()
    # dt = datetime.strptime('20211227', '%Y%m%d')

    fileDirectory = 'D:\\Giorgi\\Antenneregister'
    outputDirectory = 'D:\\Giorgi\\Antenneregister\\QV file'
    # outputfileName = 'final_all_opt_{0}.csv'.format(today.strftime('%Y%m%d'))
    outputfileName = 'final_all_opt_{0}.csv'.format(dt.strftime('%Y%m%d'))
    # fileNameAll = 'all_20180104.csv'

    # print(os.path.join(outputDirectory, outputfileName))

    df = pd.DataFrame()

    # print(recent_file_list_in_directory(fileDirectory, pastDate))
    # print(recent_file_list_in_directory(fileDirectory, today))

    # list_of_files = recent_file_list_in_directory(fileDirectory, today)
    list_of_files = recent_file_list_in_directory(fileDirectory, dt)
    # list_of_files = ['OverigMobile_opt_20210518.csv']
    # print(list_of_files)
    # print(len(list_of_files))

    for name in list_of_files:
        # print(os.path.join(fileDirectory, name))
        df_current = pd.read_csv(os.path.join(fileDirectory, name), encoding='latin1')
        # print(df_current.head())
        if ~df_current.empty:
            df = pd.concat([df, df_current],sort=False)
        # print(df.head())

    # print(recent_file_list_in_directory(fileDirectory, pastDate))
    # for name in recent_file_list_in_directory(fileDirectory, pastDate):
    #     df_current = pd.read_csv(os.path.join(fileDirectory, name), encoding='latin1')
    #     df = pd.concat([df, df_current])


    # print(df.head())

    # frequencyListTMobile = [
    #                         [723, 733],
    #                         [778, 788],
    #                         [791, 801],
    #                         [832, 842],
    #                         [900, 915],
    #                         [945, 960],
    #                         [1482, 1492],
    #                         [1750, 1780],
    #                         [1845, 1875],
    #                         [1949.7, 1959.7],
    #                         [1969.7, 1980],
    #                         [2139.7, 2149.7],
    #                         [2159.7, 2170],
    #                         [2530, 2535],
    #                         [2545, 2590],
    #                         [2650, 2655],
    #                         [2665, 2690]]
    #
    # frequencyListKPN = [
    #                     [713, 723],
    #                     [768, 778],
    #                     [811, 821],
    #                     [852, 862],
    #                     [890, 900],
    #                     [935, 945],
    #                     [1467, 1482],
    #                     [1710, 1730],
    #                     [1805, 1825],
    #                     [1934.9, 1949.7],
    #                     [1959.7, 1964.7],
    #                     [2124.9, 2139.7],
    #                     [2149.7, 2154.7],
    #                     [2535, 2545],
    #                     [2590, 2620],
    #                     [2655, 2665]]
    #
    # frequencyListVodafone = [
    #                         [703, 713],
    #                         [758, 768],
    #                         [801, 811],
    #                         [842, 852],
    #                         [880, 890],
    #                         [924.99, 935],
    #                         [1452, 1467],
    #                         [1730, 1750],
    #                         [1825, 1845],
    #                         [1920, 1934.9],
    #                         [1964.7, 1969.7],
    #                         [2110, 2124.9],
    #                         [2154.7, 2159.7],
    #                         [2500, 2530],
    #                         [2620, 2650]]

    frequencyListTMobile = [
        [723, 733],
        [778, 788],
        [791, 801],
        [832, 842],
        [900, 915],
        [945, 960],
        [1482, 1492],
        [1750, 1780],
        [1845, 1875],
        [1940, 1960],
        [2130, 2150],
        [2530, 2535],
        [2545, 2590],
        [2650, 2655],
        [2665, 2690]]

    frequencyListKPN = [
        [713, 723],
        [768, 778],
        [811, 821],
        [852, 862],
        [890, 900],
        [935, 945],
        [1467, 1482],
        [1710, 1730],
        [1805, 1825],
        [1960, 1980],
        [2150, 2170],
        [2535, 2545],
        [2590, 2620],
        [2655, 2665]]

    frequencyListVodafone = [
        [703, 713],
        [758, 768],
        [801, 811],
        [842, 852],
        [880, 890],
        [924.99, 935],
        [1452, 1467],
        [1730, 1750],
        [1825, 1845],
        [1920, 1940],
        [2110, 2130],
        [2500, 2530],
        [2620, 2650]]

    # frequencyListTele2 = [[791, 801],
    #                       [832, 842],
    #                       [2545, 2565],
    #                       [2665, 2690]]

    #################################################################
    ##df = pd.read_csv(os.path.join(fileDirectory, fileNameAll), encoding='latin1')
    ##print(df)

    df['Hoogte'] = df['Hoogte'].str.replace(' m', '').astype(float) if 'Hoogte' in df.columns else None
    df['Hoofdstraalrichting'] = df['Hoofdstraalrichting'].str.replace(' gr', '') if 'Hoofdstraalrichting' in df.columns else None
    df['Frequentie'] = df['Frequentie'].apply(lambda x: GHz_to_MHz(x))
    df['Frequentie'] = df['Frequentie'].str.replace(' MHz', '')
    # print(df.Frequentie)
    df['Vermogen'] = df['Vermogen'].str.replace(' dBW', '').astype(float) if 'Vermogen' in df.columns else None
    # df['Veilige afstand'] = df['Veilige afstand'].str.replace(' m', '').astype(float)
    df['Veilige afstand'] = df['Veilige afstand'].str.replace(' m', '').astype(float) if 'Veilige afstand' in df.columns else None

    # df[['Frequentie1', 'Frequentie2']] = pd.DataFrame(df.Frequentie.str.split('-', 1).tolist(),
    #                                                   columns=['Frequentie1', 'Frequentie2'])
    # df['Frequentie1'], df['Frequentie2'] = df['Frequentie'].str.split('-', 1).str
    df[['Frequentie1', 'Frequentie2']] = df['Frequentie'].str.split('-', n=1, expand=True)
    df['Frequentie1'] = df['Frequentie1'].astype(float)
    df['Frequentie2'] = df['Frequentie2'].astype(float)

    df['Technology'] = df['SAT_CODE'].apply(lambda x: technology(x))
    # df['Technology1'] = df['SAT_CODE'].apply(lambda x: technology1(x))
    # df['Operator'] = df['Frequentie1'].apply(lambda x: operator(x, frequencyListTMobile, frequencyListKPN, frequencyListVodafone, frequencyListTele2))
    # print(df.dtypes)
    df['Operator'] = df['Frequentie1'].apply(lambda x: operator(x, frequencyListTMobile, frequencyListKPN, frequencyListVodafone))
    df['Band'] = df['Frequentie1'].apply(lambda x: frequency_band(x))


    df['Outdoor_Macro'] = df.apply(outdoor_macro, axis=1)
    # df['Load_Date'] = (today- timedelta(15)).strftime('%Y-%m')
    df['Load_Date'] = (dt- timedelta(15)).strftime('%Y-%m')

    df['lat'] = df.apply(lat, axis=1)
    df['lon'] = df.apply(lon, axis=1)

    # print(df)

    df.drop(['SHAPE', 'Samenvatting', 'OBJECTID', 'id', 'DATUM_PLAATSING'], inplace=True, axis=1)

    df = df.drop_duplicates()
    print(df.head())
    # print(os.path.join(outputDirectory, outputfileName))
    df[df.HOOFDSOORT != 'OVERIGMOBIEL'].to_csv(os.path.join(outputDirectory, outputfileName), index=False, quoting=csv.QUOTE_ALL)
    df[df.HOOFDSOORT == 'OVERIGMOBIEL'].to_csv(os.path.join(outputDirectory,'OVERIGMOBIEL.csv'), index=False, quoting=csv.QUOTE_ALL)

    host = "prohadoope01.ux.nl.tmo"
    username = "antenna_registry"
    password = "Karidia123!"

    local_path = os.path.join(outputDirectory, outputfileName)
    remote_in_progress_path = "/IN_PROGRESS/{}".format(outputfileName)
    remote_final_path = "/FINAL/{}".format(outputfileName)

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(host=host, username=username, password=password, cnopts=cnopts) as sftp:
        sftp.put(local_path, remote_in_progress_path)
        sftp.rename(remote_in_progress_path, remote_final_path)

    df = df.replace({np.nan: None})

    df_to_load = df[df.HOOFDSOORT != 'OVERIGMOBIEL']

    host, user, pwd, db = 'prdcop1.ux.nl.tmo', 'ID022621', 'Saqartvelo!!15', 'DL_NETWORK_GEN'
    # print(pwd)

    #  Establish the connection to the Teradata database
    udaExec = teradata.UdaExec(appName="AR_deshboard", version="1.0", logConsole=False)
    with udaExec.connect(method="odbc", system=host, username=user, password=pwd, charset='UTF8') as connection:
        connection.execute(
            "DELETE FROM DL_NETWORK_GEN.ARDashboard WHERE Load_date = '{0}';".format(dt.strftime('%Y-%m')))

        # print(df_to_load.shape)
        chunk_size = 33000
        chunks = [df_to_load[i:i + chunk_size][:] for i in range(0, df_to_load.shape[0], chunk_size)]

        for df_n in chunks:
            # print(df_n)
            data = [tuple(x) for x in df_n.to_records(index=False)]
        # connection.execute("DELETE FROM DL_NETWORK_GEN.ARDashboard WHERE Load_date = '{0}';".format(dt.strftime('%Y-%m')))
            connection.executemany('INSERT INTO DL_NETWORK_GEN.ARDashboard values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'
                                   , data
                                   , batch=True
                                   )


if __name__ == '__main__':
    main()