"""Cleaning scraped data and uploading to SFTP server

Usage:
    python CleaningAndLoadingData.py
"""

import pandas as pd
import numpy as np
import os
import csv
import re
from datetime import datetime, timedelta
import pysftp
import smtplib
import zipfile
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication


desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 40)


############################ FUNCTIONS ###########################
def send_email_with_attachment(sender, password, recipients, subject, body, file_path=None, cc=None, bcc=None):
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)
    msg['Cc'] = cc
    msg['Subject'] = subject

    msg.attach(MIMEText(body))

    if file_path:
        with zipfile.ZipFile(file_path + '.zip', 'w') as myzip:
            myzip.write(file_path)

        with open(file_path + '.zip', "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(file_path + '.zip'))
            part['Content-Disposition'] = 'attachment; filename="{}"'.format(os.path.basename(file_path + '.zip'))
            msg.attach(part)

    if cc and bcc:
        rcpt = cc + bcc + recipients
    elif cc:
        rcpt = cc + recipients
    elif bcc:
        rcpt = bcc + recipients
    else:
        rcpt = recipients

    # print(rcpt)

    smtp = smtplib.SMTP('smtp.gmail.com', 587)
    smtp.ehlo()
    smtp.starttls()
    smtp.login(sender, password)
    smtp.sendmail(sender, rcpt, msg.as_string())
    smtp.quit()


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


def operator(frequencyValue, frequencyListOdido, frequencyListKPN, frequencyListVodafone):
    if value_in_the_range(frequencyValue, frequencyListOdido) == 1:
        return 'Odido'
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
    if '-' in str(row['HOOFDSTRAALRICHTING']) or row['ZENDVERMOGEN'] < 15:
        if (row['Technology'] == 'NB-IoT') or (row['Band'] == '2600TDD' and row['Operator'] == 'KPN' and row['ZENDVERMOGEN']==12.3):
            return 'Yes'
        else:
            return 'No'
    else:
        return 'Yes'


def lat(row):
    return round(xy_to_latitude(row['X'], row['Y']), 15)


def lon(row):
    return round(xy_to_longitude(row['X'], row['Y']), 15)


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
    # print(row)
    return row

#################################################################

def main():
    dt = datetime.today()
    # dt = datetime.strptime('20241027', '%Y%m%d')

    fileDirectory = 'D:\\Giorgi\\Antenneregister'
    outputDirectory = 'D:\\Giorgi\\Antenneregister\\QV file'
    # outputfileName = 'final_all_opt_{0}.csv'.format(today.strftime('%Y%m%d'))
    outputFileNameMain = 'final_all_opt_{0}.csv'.format(dt.strftime('%Y%m%d'))
    outputFileNameOM = 'OVERIGMOBIEL_{0}.csv'.format(dt.strftime('%Y%m%d'))
    outputFileNameVV = 'VASTEVERB_{0}.csv'.format(dt.strftime('%Y%m%d'))
    outputFileNameO = 'OMROEP_{0}.csv'.format(dt.strftime('%Y%m%d'))

    outputFileNameMain_v2 = 'final_all_opt_{0}_v2.csv'.format(dt.strftime('%Y%m%d'))

    df = pd.DataFrame()

    # list_of_files = recent_file_list_in_directory(fileDirectory, today)
    list_of_files = recent_file_list_in_directory(fileDirectory, dt)
    # list_of_files = ['OverigMobile_opt_20210518.csv']
    print(list_of_files)
    # print(len(list_of_files))
    # print()

    for name in [x for x in list_of_files if "Zenda" not in x]:
        # print(os.path.join(fileDirectory, name))
        df_current = pd.read_csv(os.path.join(fileDirectory, name), encoding='utf8', low_memory=False)
        # print(df_current.head())
        if ~df_current.empty:
            df = pd.concat([df, df_current],sort=False)
        # print(df.head())

    # print(df.head())

    frequencyListOdido = [
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
        [2665, 2690],
        [3550, 3650]
    ]

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
        [2655, 2665],
        [3650, 3750]
    ]

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
        [2620, 2650],
        [3450, 3551] #vodafone lables 3.5GHz cells with 3550MHz, so to assign operators correctly to the cells I changed end frequency here from 3550 to 3551
    ]


    #################################################################
    ##df = pd.read_csv(os.path.join(fileDirectory, fileNameAll), encoding='latin1')
    ##print(df)
    # df=df.head(n=100)

    df = df.dropna(subset=['FREQUENTIE'])

    # df['Hoogte'] = df['HOOGTE'].str.replace(' m', '').astype(float) if 'HOOGTE' in df.columns else None
    # df['Hoofdstraalrichting'] = df['Hoofdstraalrichting'].str.replace(' gr', '') if 'Hoofdstraalrichting' in df.columns else None
    # print(df.loc[df.FREQUENTIE.isnull(),])
    df['FREQUENTIE'] = df['FREQUENTIE'].apply(lambda x: GHz_to_MHz(x))
    df['FREQUENTIE'] = df['FREQUENTIE'].str.replace(' MHz', '')
    df['ZENDVERMOGEN'] = df.ZENDVERMOGEN.mask(df.ZENDVERMOGEN=='Niet aanwezig') #if 'Vermogen' in df.columns else None
    # df['Vermogen'] = df['Vermogen'].str.replace(' dBW', '').astype(float) if 'Vermogen' in df.columns else None
    # df['Veilige afstand'] = df['Veilige afstand'].str.replace(' m', '').astype(float)
    # df['Veilige afstand'] = df['Veilige afstand'].str.replace(' m', '').astype(float) if 'Veilige afstand' in df.columns else None
    df[['Frequentie1', 'Frequentie2']] = df['FREQUENTIE'].str.split('-', n=1, expand=True)
    df['Frequentie1'] = df['Frequentie1'].astype(float)
    df['Frequentie2'] = df['Frequentie2'].astype(float)

    df['Technology'] = df['SAT_CODE'].apply(lambda x: technology(x))
    df['Operator'] = df['Frequentie1'].apply(lambda x: operator(x, frequencyListOdido, frequencyListKPN, frequencyListVodafone))
    df['Band'] = df['Frequentie1'].apply(lambda x: frequency_band(x))


    df['Outdoor_Macro'] = df.apply(outdoor_macro, axis=1)
    df['Load_Date'] = (dt- timedelta(15)).strftime('%Y-%m')

    df['lat'] = df.apply(lat, axis=1)
    df['lon'] = df.apply(lon, axis=1)

    # print(df.head())
    df.to_csv(os.path.join(outputDirectory, outputFileNameMain_v2), index=False, quoting=csv.QUOTE_ALL)


    df_out=(df
     .filter([
        'SAT_CODE'
        , 'WOONPLAATSNAAM'
        , 'DATUM_INGEBRUIKNAME'
        , 'HOOFDSOORT'
        , 'GEMEENTE'
        , 'X'
        , 'Y'
        , 'POSTCODE'
        , 'HOOGTE'
        , 'HOOFDSTRAALRICHTING'
        , 'FREQUENTIE'
        , 'ZENDVERMOGEN'
        , 'VEILIGE_AFSTAND'
        , 'Frequentie1'
        , 'Frequentie2'
        , 'Technology'
        , 'Operator'
        , 'Band'
        , 'Outdoor_Macro'
        , 'Load_Date'
        , 'lat'
        , 'lon'
        , 'ID_LOCATIE'
        , 'ID_CELL'
        , 'DATUM_PLAATSING'
        , 'DATUM_WIJZIGING'
        , 'SMALL_CELL_INDICATOR'
      ])
     .rename(columns={
        'GEMEENTE': 'GEMNAAM'
        , 'X': 'x'
        , 'Y': 'y'
        , 'POSTCODE': 'postcode'
        , 'HOOGTE': 'Hoogte'
        , 'HOOFDSTRAALRICHTING': 'Hoofdstraalrichting'
        , 'FREQUENTIE': 'Frequentie'
        , 'ZENDVERMOGEN': 'Vermogen'
        , 'VEILIGE_AFSTAND': 'Veilige afstand'
        , 'ID_LOCATIE': 'OBJECTID'
        , 'ID_CELL': 'id'
        , 'DATUM_WIJZIGING': 'DATUM_LAATSTE_AANPASSING'
     })
    )

    df_out = df_out.drop_duplicates()
    df_out["SMALL_CELL_INDICATOR"] = df_out["SMALL_CELL_INDICATOR"].replace({'Null': None})
    df_out["DATUM_LAATSTE_AANPASSING"] = df_out["DATUM_LAATSTE_AANPASSING"].replace({'Null': None})

    df_out["WOONPLAATSNAAM"] = df_out["WOONPLAATSNAAM"].replace({np.nan: 'Null'})

    # export to csv
    df_out.loc[~df_out.HOOFDSOORT.str.contains('|'.join(['OVERIGMOBIEL', 'VASTE VERB', 'OMROEP'])),].to_csv(os.path.join(outputDirectory, outputFileNameMain), index=False, quoting=csv.QUOTE_ALL)
    df_out.loc[df_out.HOOFDSOORT == 'OMROEP',].to_csv(os.path.join(outputDirectory,outputFileNameO), index=False, quoting=csv.QUOTE_ALL)
    df_out.loc[df_out.HOOFDSOORT == 'VASTE VERB',].to_csv(os.path.join(outputDirectory,outputFileNameVV), index=False, quoting=csv.QUOTE_ALL)
    df_out.loc[df_out.HOOFDSOORT == 'OVERIGMOBIEL',].to_csv(os.path.join(outputDirectory,outputFileNameOM), index=False, quoting=csv.QUOTE_ALL)
    print('Data cleaned and stored localy in csv file')

    print('Trying to store cleaned data on sftp location')
    hostname = "172.27.0.69"
    username = "ardashboard"
    keyFile = "D:\\Giorgi\\Antenneregister\\Useful files\\ardashboard_key"
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    local_path = os.path.join(outputDirectory, outputFileNameMain)
    remote_path = "/{}".format(outputFileNameMain)

    with pysftp.Connection(hostname, username=username, private_key=keyFile, cnopts=cnopts) as sftp:
        print("Connection succesfully stablished ... ")
        sftp.put(local_path, remote_path)

    print('Data stored on sftp location')

    # # Example usage
    # send_email_with_attachment(sender='gio.labadze+antenneregister@gmail.com',
    #                            password='ghitfworqormtqxl',
    #                            recipients=['rob.hormes@t-mobile.nl'],
    #                            subject='Antenneregister data',
    #                            body='Hi Rob,\nPlease find the attached with the latest Anntenaregister data.\nCheers,\nGiorgi',
    #                            file_path=os.path.join(outputDirectory, outputFileNameMain),
    #                            bcc=['giorgi.labadze@t-mobile.nl']
    #                            )


if __name__ == '__main__':
    main()