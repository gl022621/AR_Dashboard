"""Retrieves antenna ids from www.antennebureau.nl database and saving data in csv file.

Usage:
    python3 playground.py
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
desired_columns = 30
desired_column_width=200

pd.set_option('display.width', desired_display_width)
pd.set_option('display.max_columns',desired_columns)
pd.set_option('display.max_colwidth', desired_column_width)


# def layers(*args):
#     """Creates string for url with all layers that are set as an argument.
#
#     Args:
#         args: Layers that needs to be fetched separated by comma.
#
#     Returns:
#         A sting containing layers in the form that can be included in url.
#     """
#     layerString = ''
#     for ar in args:
#         if len(layerString) == 0:
#             layerString += '%3A' + str(ar)
#         else:
#             layerString += '%2C' + str(ar)
#     return layerString


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


# def url_generator(xmin, xmax, ymin, ymax, eps):
#     """Creates url string for fetching antenna ids.
#
#     Args:
#         xmin: The smallest x coordinate.
#         xmax: The largest x coordinate.
#         ymin: The smallest y coordinate.
#         ymax: The largest y coordinate.
#         eps: Extra space to assure overlap between neighboring rectangular.
#         layerstring: Output string of layers(*args) function.
#
#     Returns:
#         A url string.
#     """
#     url =f'https://antenneregister.nl/mapserver/wfs/' \
#           '?outputformat=application%2Fjson&request=GetFeature&service=WFS&version=2.0.0&srsname=EPSG%3A28992&' \
#           'bbox={0}%' \
#           '2C{1}%' \
#           '2C{2}%' \
#           '2C{3}' \
#           '&typename=Antennes'.format(xmin, ymin, xmax, ymax+eps)
#
#
#     # url = 'https://gisextern.dictu.nl/arcgis/rest/services/Antenneregister/Antennes_extern' \
#     #       '/MapServer/identify?f=json&geometry=%7B%22rings%22%3A%5B%5B%' \
#     #       '5B{0}%' \
#     #       '2C{1}%5D%2C%' \
#     #       '5B{2}%' \
#     #       '2C{1}%5D%2C%' \
#     #       '5B{2}%' \
#     #       '2C{3}%5D%2C%' \
#     #       '5B{0}%' \
#     #       '2C{3}%5D%2C%' \
#     #       '5B{0}%' \
#     #       '2C{1}%' \
#     #       '5D%5D%5D%2C%22spatialReference%22%3A%7B%22wkid%22%3A28992%7D%7D&' \
#     #       'tolerance=5&returnGeometry=true&mapExtent=%7B%22' \
#     #       'xmin%22%3A0%2C%22' \
#     #       'ymin%22%3A0%2C%22' \
#     #       'xmax%22%3A0%2C%22' \
#     #       'ymax%22%3A0%2C%22' \
#     #       'spatialReference%22%3A%7B%22wkid%22%3A28992%7D%7D&' \
#     #       'imageDisplay=811%2C421%2C96&geometryType=esriGeometryPolygon&sr=28992&' \
#     #       'layers=all{4}'.format(xmin, ymin, xmax, ymax+eps, layerstring)
#     return url

def user_agent_string():
    """Creates user agent to be included into request.

    Args:
        tech: Technology indicator that is being parsed. format of argument is: 'LTE', 'UMTS', 'GSM', 'IoT'. Any other string will result in default value.

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
            print('I am in GET')
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

    print('out: ', out)
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
            # 'Cookie': 'f5_cspm=1234; atrv-sessie=e5324b62-1131-487f-974e-97b599061356; TS01517c46=015d4243a64aeaeea03fd49bf0f7a6850daeeea1f2f8addc69576d9e7754864c420b0049e61c53b841827d989f474a84e731f9599c5c8fb9c77883c3e65a67ac9ac929ac442fc8a4351f5ececad451a5e77a6cbbaa',
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
            # 'Cookie': 'f5_cspm=1234; atrv-sessie=e5324b62-1131-487f-974e-97b599061356; TS01517c46=015d4243a64aeaeea03fd49bf0f7a6850daeeea1f2f8addc69576d9e7754864c420b0049e61c53b841827d989f474a84e731f9599c5c8fb9c77883c3e65a67ac9ac929ac442fc8a4351f5ececad451a5e77a6cbbaa',
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


# def url_response(session, url):
#     """Creates url string for fetching antenna ids.
#
#     Args:
#         session: Requests session.
#         url: URL string.
#
#     Returns:
#         A json formatted response of url.
#     """
#     headers = {'User-Agent': user_agent_string('')}
#     response = None
#     # request the URL and parse the JSON
#     try:
#         response = session.get(url, headers=headers)
#         out = response.json()
#         print('out: ', out)
#     except requests.exceptions.Timeout:
#         print('Timeout Error for url={0}'.format(url))
#         out = {'results': []}
#     except requests.exceptions.ConnectionError:
#         print('Connection Error for url={0}'.format(url))
#         out = {'results': []}
#     except Exception as e:
#         print(f"Unexpected error for url={url}: {e}")
#         out = {'results': []}
#
#     if response:
#         print('response.json: ', response.json())
#
#     return out


def list_of_locations(xmin, xmax, ylist, eps):
    """Fetches partial information from antenna locations.

    Args:
        session: Requests session.
        url: URL string.

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
        # URL definition
        # url =url_generator(xmin, xmax, ymin, ymax, eps)
        # print(i, ': ', url)

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
            n_random = random.randint(50, 60)
            print('sleeping for {}sec'.format(n_random))
            time.sleep(n_random)
        elif i%100==0:
            n_random=random.randint(5, 15)
            print('sleeping for {}sec'.format(n_random))
            time.sleep(n_random)

        # inputLocs =response['features']

        # # inputLocs =['results']

        # print('inputLocs: ', inputLocs)

        # for x in inputLocs: # loop to get all the antenna ids and add them into list
        #     # print(type(x))
        #     attrs = x['attributes']
        #     # print(attrs)
        #     outputLocs.append(attrs)
        #     # print(outputLocs)
        outputLocs.append(response)
        # print(outputLocs)
    return outputLocs


def extract_antennas_grouped(geojson_list):
    """
    Extracts antenna data from a list of GeoJSON-like dicts,
    keeping ANT_IDS and HOOFDSOORT as lists (not exploding into rows).

    Returns a pandas DataFrame.
    """
    records = []

    for geojson in geojson_list:
        for feature in geojson.get("features", []):
            props = feature.get("properties", {}).copy()
            coords = feature.get("geometry", {}).get("coordinates", [None, None])

            # Convert comma-separated strings to lists
            # props["ANT_IDS"] = [s.strip() for s in props.get("ANT_IDS", "").split(",") if s.strip()]
            # props["HOOFDSOORT"] = [s.strip() for s in props.get("HOOFDSOORT", "").split(",") if s.strip()]

            props["geometry_x"] = coords[0]
            props["geometry_y"] = coords[1]

            records.append(props)

    return pd.DataFrame(records)


# def extract_antennas_from_geojson_list(geojson_list):
#     """
#     Extracts and flattens antenna data from a list of GeoJSON-like dictionaries.
#     Returns a pandas DataFrame with one row per ANT_ID and matched HOOFDSOORT.
#     """
#     all_records = []
#
#     for geojson in geojson_list:
#         features = geojson.get("features", [])
#         if not features:
#             continue  # Skip if no features
#
#         for feature in features:
#             props = feature.get("properties", {}).copy()
#             coords = feature.get("geometry", {}).get("coordinates", [None, None])
#
#             ant_ids = props.pop("ANT_IDS", "")
#             hoofsoort = props.pop("HOOFDSOORT", "")
#
#             ant_id_list = [id_.strip() for id_ in ant_ids.split(",") if id_.strip()]
#             hoofsoort_list = [h.strip() for h in hoofsoort.split(",") if h.strip()]
#
#             # Match ANT_IDS and HOOFDSOORT by position
#             if len(ant_id_list) != len(hoofsoort_list):
#                 print(f"⚠️ Mismatch in lengths for ID {props.get('ID')}: ANT_IDS={ant_id_list}, HOOFDSOORT={hoofsoort_list}")
#                 continue
#
#             for ant_id, soort in zip(ant_id_list, hoofsoort_list):
#                 record = props.copy()
#                 record["ANT_ID"] = ant_id
#                 record["HOOFDSOORT"] = soort
#                 record["longitude"] = coords[0]
#                 record["latitude"] = coords[1]
#                 all_records.append(record)
#
#     return pd.DataFrame(all_records)


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
    return df[column].apply(lambda x: formating_date(x))


def save_df_in_csv(df, dir, file):
    """Saves datafreme into csv file in directory 'dir' under 'file' name.

    Args:
        df: Dataframe to save.
        dir: Directory path.
        file: File name.
    """
    df.to_csv(os.path.join(dir,file), index=False, quoting=csv.QUOTE_ALL)
    print('File has been exported!')


def url_string(tech, id):
    """Creates url string for fetching antenna frequency information.

    Args:
        id: ID of the location.
        tech: Technology indicator that is being parsed. format of argument is: 'LTE', 'UMTS', 'GSM', 'IoT'. Any other string will result in default value.

    Returns:
        A url string.
    """
    return {
        'NR':           'https://antenneregister.nl/Geocortex/Essentials/REST/Sites/Antennes_extern/map/mapservices/9/layers/11/datalinks/DETAILS_WIMAX/link?maxRecords=&pff_ID={0}&f=json#'.format(id),
        'LTE':          'https://antenneregister.nl/Geocortex/Essentials/REST/Sites/Antennes_extern/map/mapservices/9/layers/7/datalinks/DETAILS_LTE/link?maxRecords=&pff_ID={0}&f=json#'.format(id),
        'UMTS':         'https://antenneregister.nl/Geocortex/Essentials/REST/Sites/Antennes_extern/map/mapservices/9/layers/4/datalinks/DETAILS2_UMTS/link?maxRecords=&pff_ID={0}&f=json#'.format(id),
        'GSM':          'https://antenneregister.nl/Geocortex/Essentials/REST/Sites/Antennes_extern/map/mapservices/9/layers/1/datalinks/DETAILS_GSM/link?maxRecords=&pff_ID={0}&f=json#'.format(id),
        'IoT':          'https://antenneregister.nl/Geocortex/Essentials/REST/Sites/Antennes_extern/map/mapservices/9/layers/23/datalinks/DETAILS2_WIMAX/link?maxRecords=&pff_ID={0}&f=json#'.format(id),
        'OverigMobile': 'https://antenneregister.nl/Geocortex/Essentials/REST/Sites/Antennes_extern/map/mapservices/9/layers/23/datalinks/DETAILS2_OVER/link?maxRecords=&pff_ID={0}&f=json#'.format(id),
        'VasteVerb':    'https://antenneregister.nl/Geocortex/Essentials/REST/Sites/Antennes_extern/map/mapservices/9/layers/20/datalinks/DETAILS2_VAST/link?maxRecords=&pff_ID={0}&f=json#'.format(id),
        'Omroep':       'https://antenneregister.nl/Geocortex/Essentials/REST/Sites/Antennes_extern/map/mapservices/9/layers/14/datalinks/DETAILS2_OMROEP/link?maxRecords=&pff_ID={0}&f=json#'.format(id)
    }.get(tech, 'https://antenneregister.nl/Geocortex/Essentials/REST/Sites/Antennes_extern/map/mapservices/9/layers/7/datalinks/DETAILS_LTE/link?maxRecords=&pff_ID={0}&f=json#'.format(id))


def from_dict_to_list(rowlist, rows, id):
    """Loops to get data from dicts and puts it into convenient list form.

    Returns:
        A list of rows for each id.
    """
    for row in rowlist: # loop to get data from dicts and put it into convenient list form
        row['row'].append(id)
        rows.append(row['row'])


def url_response_id(session, id_list):
    # url = url_string(technology, id)
    # # print(url)
    # headers = {'User-Agent': user_agent_string(technology)}
    # # request the URL and parse the JSON

    print(id_list)

    base_url = 'https://antenneregister.nl/mapserver/wfs/'

    params = make_request_params_antennas(id_list)
    print('params: ', params)
    headers = make_request_headers('antennas')
    print('headers: ', headers)

    response = make_request(session=session, method='get', base_url=base_url, params=params, headers=headers)
    print('response: ', response)

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

    # number of small rectangles that we split above big rectangle
    # yDelta = 3950
    # yDelta = 6320
    # yDelta = 7900
    yDelta = 12000
    epsilon = 1

    yList = equally_split_list(yMin, yMax+1, yDelta)

    # creates dataframe with anntena details including ids
    dfIDsDubs = extract_antennas_grouped(list_of_locations(xmin=xMin, xmax=xMax, ylist=yList, eps=epsilon))

    # removing duplicates from above dataframe
    dfIDsDubs['ANT_IDS'] = dfIDsDubs['ANT_IDS'].apply(lambda x: ','.join(x) if isinstance(x, list) else x)
    dfIDsDubs['HOOFDSOORT'] = dfIDsDubs['HOOFDSOORT'].apply(lambda x: ','.join(x) if isinstance(x, list) else x)
    dfIDs = dfIDsDubs.drop_duplicates()
    dfIDs["ANT_IDS"] = dfIDs["ANT_IDS"].apply(lambda x: [s.strip() for s in x.split(",")] if isinstance(x, str) else x)
    dfIDs["HOOFDSOORT"] = dfIDs["HOOFDSOORT"].apply(lambda x: [s.strip() for s in x.split(",")] if isinstance(x, str) else x)

    # dfIDs['DATUM_INGEBRUIKNAME'] = formating_date_column(dfIDs, 'DATUM_INGEBRUIKNAME') #dfIDsDubs['DATUM_INGEBRUIKNAME'].apply(lambda x: formating_date(x))
    # dfIDs['DATUM_PLAATSING'] = formating_date_column(dfIDs, 'DATUM_PLAATSING') #dfIDsDubs['DATUM_PLAATSING'].apply(lambda x: formating_date(x))

    # writing above dataframe into a file
    save_df_in_csv(dfIDs,fileDirectory, fileNameIDs)

    dfIDs = pd.read_csv('D:\\Giorgi\\Antenneregister\\ids_all_20250514.csv')
    # dfIDs = pd.read_csv('D:\\Giorgi\\Antenneregister\\ids_overig_opt_20210618.csv')

    # print(dfIDs.head())
    # print(dfIDs.HOOFDSOORT.unique())

    dfIDs['ANT_IDS'] = dfIDs['ANT_IDS'].apply(ast.literal_eval)
    dfIDs['HOOFDSOORT'] = dfIDs['HOOFDSOORT'].apply(ast.literal_eval)

    # dfIDs["ANT_IDS"] = dfIDs["ANT_IDS"].apply(lambda x: [s.strip() for s in x.split(",")] if isinstance(x, str) else x)
    # dfIDs["HOOFDSOORT"] = dfIDs["HOOFDSOORT"].apply(lambda x: [s.strip() for s in x.split(",")] if isinstance(x, str) else x)

    df_MOBIELE = dfIDs[dfIDs.MOBIELE_COMMUNICATIE == 1]
    df_OM = dfIDs[dfIDs.OVERIGMOBIEL == 1]
    df_TDAB = dfIDs[dfIDs.OMROEP == 1]
    df_ZM = dfIDs[dfIDs.ZENDAMATEURS == 1]
    df_VV = dfIDs[dfIDs.VASTE_VERB == 1]


    # save_df_in_csv(df_ZM, fileDirectory, '{0}_opt_{1}.csv'.format('ZendaMateurs', today.strftime('%Y%m%d')))

    dfList = {
        'MOBIELE': df_MOBIELE.head()
        # , 'OverigMobile': df_OM
        # , 'Omroep': df_TDAB
        # , 'ZendaMateurs': df_ZM
        # , 'VasteVerb': df_VV
    }

    # print(dfList)
    #


    for technology, df in dfList.items():
        print(f'{technology}\n',df.info())

        s = requests.Session()
        list_antennas = []
        # iterate over the rows of the dataframe
        for index, row in df.iterrows():
            print(type(row['ANT_IDS']))
            response = url_response_id(session=s, id_list=row['ANT_IDS'])

            list_antennas.append(response)

        df_test = extract_antennas_grouped(list_antennas)


        # idList = df['id'].tolist()
        # rows = []
        # print('Parsing antenna details for {0} ids ... '.format(technology))
        # print('Parsing {0} antenna details started on {1}.'.format(technology, time.strftime("%c")))
        #
        # # looping through all antenna ids, creating url for each ids, requesting and getting frequency data and organizing them into dataframe
        # s = requests.Session()
        # # m = 0
        # for id in idList:
        #     # URL definition
        #     response = url_response_id(technology, id, s)
        #     # response = requests.get(url, headers=headers)
        #     # response.raise_for_status() # raise exception if invalid response
        #     # print(response.json())
        #     if len(response.json()['results']) != 0:
        #         inputFrequencies = response.json()['results'][0]
        #         # column names for the frequency dataframe
        #         columnList = inputFrequencies['linkedData']['columns']
        #         columnList.append('id')  # adds 'id' to column names
        #         #  list of dicts with frequency information
        #         rowList = inputFrequencies['linkedData']['rows']
        #         # print(rowList)
        #         # print(rows)
        #         # loop to get data from dicts and put it into convenient list form
        #         from_dict_to_list(rowList, rows, id)
        #
        #
        # dfFrequencies = pd.DataFrame(rows, columns=columnList)
        # # print(dfFrequencies)
        # #  joining two dataframes
        # df_out = pd.merge(df, dfFrequencies, how='left', left_on=['id'], right_on=['id'])
        # # writing final dataframe to local file
        # outFileName = '{0}_opt_{1}.csv'.format(technology, today.strftime('%Y%m%d'))
        # save_df_in_csv(df_out,fileDirectory, outFileName)

    t1 = time.time()
    i, d = divmod((t1 - t0) / 60 / 60, 1)

    print('Parsing ended at {0}.'.format(time.strftime("%c")))
    print('Parsing took {0} hours and {1} minutes.'.format(round(i), round(d * 60, 1)))


if __name__ == '__main__':
    main() # The 0th arg is the module filename
