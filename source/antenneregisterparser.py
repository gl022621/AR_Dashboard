"""Retrieves antenna information from www.antennebureau.nl database and saving data in csv file.

Usage:
    python antenneregisterparser.py
"""

import os
import csv
import pandas as pd
import requests
import time
from datetime import datetime
import random
import ast

desired_display_width=320
desired_columns = 40
desired_column_width=200

pd.set_option('display.width', desired_display_width)
pd.set_option('display.max_columns',desired_columns)
pd.set_option('display.max_colwidth', desired_column_width)


def equally_split_list(zmin, zmax, zdelta):
    """Creates list of equally separated numbers between two values, zmin and zmax.

    Args:
        zmin: Lower number.
        zmax: Higher number.
        zdelta: Number of splits

    Returns:
        A list of equally separated numbers.
    """
    ls = list(range(zmin, zmax, round((zmax-zmin)/zdelta)))
    return ls


def user_agent_string():
    """Creates user agent to be included into request.
    Returns:
        A string of user agent.
    """
    return random.choice([
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko',
        'Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.61/63 Safari/537.36 Edg/100.0.1185.39'
    ])


def make_request(session, method, base_url, params=None, data=None, headers=None):
    """
    Makes an HTTP request using the specified method.

    Args:
        session (requests.Session): The requests session to use
        method (str): 'GET' or 'POST'
        base_url (str): The endpoint URL
        params (dict): Query string parameters (appended to the URL)
        data (dict): Payload for POST (sent in body)
        headers (dict): Optional request headers

    Returns:
        requests.Response: Response object
    """
    method = method.upper()

    if method == "GET":
        try:
            # print('I am in GET')
            response = session.get(url=base_url, params=params, headers=headers)
            out = response.json()
            print('url: ',response.request.url)
            # print('headers: ', response.request.headers)
            # print('out: ', out)
        except requests.exceptions.Timeout:
            print(f'Timeout Error for {params}')
            out = {'features': []}
        except requests.exceptions.ConnectionError:
            print(f'Connection Error for params={params}')
            out = {'features': []}
        except Exception as e:
            print(f"Unexpected error for params={params}: {e}")
            out = {'features': []}

        # if response:
        #     print('response.json: ', response.json())
    elif method == "POST":
        try:
            response = session.post(url=base_url, params=params, json=data, headers=headers)
            out = response.json()
            print('post out: ', out)
        except requests.exceptions.Timeout:
            print(f'Timeout Error for {params}')
            out = {'features': []}
        except requests.exceptions.ConnectionError:
            print(f'Connection Error for params={params}')
            out = {'features': []}
        except Exception as e:
            print(f"Unexpected error for params={params}: {e}")
            out = {'features': []}
    else:
        raise ValueError(f"Unsupported method: {method}")

    # print('out: ', out)
    return out


def make_request_params_locations(xmin, ymin, xmax, ymax, eps):
    """
    Creates parameters for the request.

    Args:
        xmin (float): Minimum x-coordinate
        ymin (float): Minimum y-coordinate
        xmax (float): Maximum x-coordinate
        ymax (float): Maximum y-coordinate
        eps (float): Extra space to assure overlap between neighboring rectangles

    Returns:
        dict: Parameters for the request
    """
    return {
        'outputformat': 'application/json',
        'request': 'GetFeature',
        'service': 'WFS',
        'version': '2.0.0',
        'srsname': 'EPSG:28992',
        'bbox': f'{xmin},{ymin},{xmax},{ymax+eps}',
        'typename': 'Antennes'
    }


def make_request_params_antennas(list_of_ids):
    """
    Creates parameters for the request.

    Args:
        list_of_ids (list): List of antenna IDs

    Returns:
        dict: Parameters for the request
    """
    return {
        'outputformat': 'application/json',
        'request': 'GetFeature',
        'service': 'WFS',
        'version': '2.0.0',
        'filter' : '<Filter>' + ''.join(f'<ResourceId id="{id_}"/>' for id_ in list_of_ids) + '</Filter>',
        'typename': 'Antennes_Groepen'
    }


def make_request_headers(type):
    """
    Creates headers for the request.

    Returns:
        dict: Headers for the request
    """
    if type.upper()=="LOCATIONS":
        return {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Host': 'antenneregister.nl',
            'Referer': 'https://antenneregister.nl/viewer/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': user_agent_string(),
            'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
    elif type.upper()=="ANTENNAS":
        return {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            # 'Content-Length': '178',
            # 'Content-type': 'application/x-www-form-urlencoded',
            'Host': 'antenneregister.nl',
            # 'Origin': 'https://antenneregister.nl',
            'Referer': 'https://antenneregister.nl/viewer/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': user_agent_string(),
            'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
    else:
        raise ValueError(f"Unsupported type: {type}")


def list_of_locations(xmin, xmax, ylist, eps):
    """Fetches information for locations.

    Args:
        xmin: The smallest x coordinate.
        xmax: The largest x coordinate.
        ylist: List of y coordinates.
        eps: Extra space to assure overlap between neighboring rectangles.

    Returns:
        A List of antenna locations and partial information from this locations.
    """

    base_url = 'https://antenneregister.nl/mapserver/wfs/'

    outputLocs = []
    s = requests.Session()
    for i in range(len(ylist)-1): #loop crating url for all small rectangles and shooting request to collect antenna ids
        #  print(ylist[i], ylist[i+1])
        ymin = ylist[i]
        ymax = ylist[i+1]

        params = make_request_params_locations(xmin, ymin, xmax, ymax, eps)
        # print('params: ', params)
        headers = make_request_headers('locations')
        # print('headers: ', headers)

        # request the URL and parse the JSON
        response = make_request(session=s, method='get', base_url=base_url, params=params, headers=headers)

        # print('response type:', type(response))
        while 'error' in response:
            n_random = random.randint(100, 150)
            print('error, sleeping for {}sec'.format(n_random))
            time.sleep(n_random)
            response = make_request(session=s, method='get', base_url=base_url, params=params, headers=headers)

        if i%1000==0:
            n_random = random.randint(25, 35)
            print('sleeping for {}sec'.format(n_random))
            time.sleep(n_random)
        elif i%100==0:
            n_random=random.randint(5, 15)
            print('sleeping for {}sec'.format(n_random))
            time.sleep(n_random)

        outputLocs.append(response)
        # print(outputLocs)
    return outputLocs


def extract_antennas_grouped(geojson_list):
    """
    Extracts location information from a list of GeoJSON-like dicts,
    keeping ANT_IDS and HOOFDSOORT as lists (not exploding into rows).

    Returns a pandas DataFrame.
    """
    records = []

    for geojson in geojson_list:
        for feature in geojson.get("features", []):
            props = feature.get("properties", {}).copy()
            records.append(props)

    return pd.DataFrame(records)


def formating_date(dateString):
    """Converts string into date.

    Args:
        dateString: Date string formatted either mm/dd/yyyy or dd-mm-yyyy.
    """
    if dateString == 'Null':
        return 'Null'
    elif dateString.find('/') != -1:
        return datetime.strptime(dateString, '%m/%d/%Y').date()
    elif dateString.find('-') != -1:
        return datetime.strptime(dateString, '%d-%m-%Y').date()
    else:
        return 'Unknown Format'


def formating_date_column(df, column):
    """Converts string column of dataframe into date.

    Args:
        df: Dataframe.
        column: String column which needs to be converted to date type.
    """
    return df[column].apply(lambda x: formating_date(x) if pd.notnull(x) and str(x).strip() != '' else None)


def save_df_in_csv(df, dir, file):
    """Saves datafreme into csv file in directory 'dir' under 'file' name.

    Args:
        df: Dataframe to save.
        dir: Directory path.
        file: File name.
    """
    df.to_csv(os.path.join(dir,file), index=False, quoting=csv.QUOTE_ALL)
    print('File has been exported!')


def url_response_id(session, id_list):

    # print(id_list)

    base_url = 'https://antenneregister.nl/mapserver/wfs/'

    params = make_request_params_antennas(id_list)
    # print('params: ', params)
    headers = make_request_headers('antennas')
    # print('headers: ', headers)

    response = make_request(session=session, method='get', base_url=base_url, params=params, headers=headers)
    # print('response: ', response)

    return response


def main():
    """Retrieves antenna ids from www.antennebureau.nl database and saving data in csv file.
    """

    print('Parsing started at {0}.'.format(time.strftime("%c")))

    t0 = time.time()
    today = datetime.today()

    # File location information
    fileDirectory = r'D:\Giorgi\Antenneregister'
    fileNameIDs = 'ids_all_{0}.csv'.format(today.strftime('%Y%m%d'))

    # definition of vertices of rectangle that fully covers the Netherlands on the Antennaregister map
    xMin = 10400
    xMax = 278000
    yMin = 306000
    yMax = 640000
    # yMin = 490000
    # yMax = 500000

    # number of small rectangles that we split above big rectangle
    # yDelta = 2
    yDelta = 12000
    epsilon = 1

    yList = equally_split_list(yMin, yMax+1, yDelta)
    print(yList)

    # creates dataframe with anntena details including ids
    dfIDsDubs = extract_antennas_grouped(list_of_locations(xmin=xMin, xmax=xMax, ylist=yList, eps=epsilon))

    dfIDs = dfIDsDubs.drop_duplicates().copy()
    dfIDs.rename(columns={
        'ID': 'ID_LOCATIE',
        'ANT_IDS': 'IDS_ANTENNE'
    }, inplace=True)

    # writing above dataframe into a file
    save_df_in_csv(dfIDs,fileDirectory, fileNameIDs)

    dfIDs["IDS_ANTENNE"] = dfIDs["IDS_ANTENNE"].apply(lambda x: [s.strip() for s in x.split(",")] if isinstance(x, str) else x)
    dfIDs["HOOFDSOORT"] = dfIDs["HOOFDSOORT"].apply(lambda x: [s.strip() for s in x.split(",")] if isinstance(x, str) else x)

    df_MOBIELE = dfIDs[dfIDs.MOBIELE_COMMUNICATIE == 1]
    df_OM = dfIDs[dfIDs.OVERIGMOBIEL == 1]
    df_TDAB = dfIDs[dfIDs.OMROEP == 1]
    df_ZM = dfIDs[dfIDs.ZENDAMATEURS == 1]
    df_VV = dfIDs[dfIDs.VASTE_VERB == 1]

    # save_df_in_csv(df_ZM, fileDirectory, '{0}_opt_{1}.csv'.format('ZendaMateurs', today.strftime('%Y%m%d')))

    dict_df = {
        'MOBIELE': df_MOBIELE
        , 'OverigMobile': df_OM
        , 'Omroep': df_TDAB
        # , 'ZendaMateurs': df_ZM.head()
        , 'VasteVerb': df_VV
    }

    # print(dfList)


    for technology, df in dict_df.items():
        # print(f'{technology}\n',df.info())

        s = requests.Session()
        list_antennas = []
        # iterate over the rows of the dataframe
        for index, row in df.iterrows():
            # print(type(row['IDS_ANTENNE']))
            response = url_response_id(session=s, id_list=row['IDS_ANTENNE'])

            list_antennas.append(response)

        df_antennas = extract_antennas_grouped(list_antennas)
        df_antennas= (df_antennas
         .assign(
            DATUM_INGEBRUIKNAME= formating_date_column(df_antennas, 'DATUM_INGEBRUIKNAME')  # dfIDsDubs['DATUM_INGEBRUIKNAME'].apply(lambda x: formating_date(x))
            ,DATUM_PLAATSING = formating_date_column(df_antennas, 'DATUM_PLAATSING')  # dfIDsDubs['DATUM_PLAATSING'].apply(lambda x: formating_date(x))
            ,DATUM_WIJZIGING = formating_date_column(df_antennas, 'DATUM_WIJZIGING')  # dfIDsDubs['DATUM_WIJZIGING'].apply(lambda x: formating_date(x))
        )
         .rename(
            columns={
                'ID': 'ID_CELL',
                'AI_ID': 'ID_ANTENNE'
            }
        ))


        df = df.copy()
        df['pairs'] = df.apply(lambda row: list(zip(row['IDS_ANTENNE'], row['HOOFDSOORT'])), axis=1)
        df = df.explode('pairs')
        df['ID_ANTENNE'] = df['pairs'].apply(lambda x: int(x[0]))
        df['HOOFDSOORT'] = df['pairs'].apply(lambda x: x[1])
        df = df.drop(columns=['pairs', 'IDS_ANTENNE'])

        df_all = pd.merge(
            df
            , df_antennas
            , how='right'
            , left_on=['ID_ANTENNE']
            , right_on=['ID_ANTENNE']
        )

        df_out = (df_all.filter([
             'ID_LOCATIE'
            , 'ID_ANTENNE'
            , 'ID_CELL'
            , 'DATUM_PLAATSING'
            , 'DATUM_INGEBRUIKNAME'
            , 'DATUM_WIJZIGING'
            , 'GEMEENTE'
            , 'WOONPLAATSNAAM'
            , 'POSTCODE'
            , 'X'
            , 'Y'
            , 'SAT_CODE'
            , 'HOOFDSOORT'
            , 'SMALL_CELL_INDICATOR'
            , 'HOOGTE'
            , 'HOOFDSTRAALRICHTING'
            , 'FREQUENTIE'
            , 'ZENDVERMOGEN'
            , 'VEILIGE_AFSTAND'
        ])
         )

        # writing final dataframe to local file
        outFileName = '{0}_opt_{1}.csv'.format(technology, today.strftime('%Y%m%d'))
        save_df_in_csv(df_out,fileDirectory, outFileName)

    t1 = time.time()
    i, d = divmod((t1 - t0) / 60 / 60, 1)

    print('Parsing ended at {0}.'.format(time.strftime("%c")))
    print('Parsing took {0} hours and {1} minutes.'.format(round(i), round(d * 60, 1)))


if __name__ == '__main__':
    main()
