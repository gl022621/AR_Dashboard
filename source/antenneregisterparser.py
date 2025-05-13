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

desired_display_width=320
desired_columns = 20
desired_column_width=200

pd.set_option('display.width', desired_display_width)
pd.set_option('display.max_columns',desired_columns)
pd.set_option('display.max_colwidth', desired_column_width)


def layers(*args):
    """Creates string for url with all layers that are set as an argument.

    Args:
        args: Layers that needs to be fetched separated by comma.

    Returns:
        A sting containing layers in the form that can be included in url.
    """
    layerString = ''
    for ar in args:
        if len(layerString) == 0:
            layerString += '%3A' + str(ar)
        else:
            layerString += '%2C' + str(ar)
    return layerString


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


def url_generator(xmin, xmax, ymin, ymax, eps, layerstring):
    """Creates url string for fetching antenna ids.

    Args:
        xmin: The smallest x coordinate.
        xmax: The largest x coordinate.
        ymin: The smallest y coordinate.
        ymax: The largest y coordinate.
        eps: Extra space to assure overlap between neighboring rectangular.
        layerstring: Output string of layers(*args) function.

    Returns:
        A url string.
    """
    url = 'https://gisextern.dictu.nl/arcgis/rest/services/Antenneregister/Antennes_extern' \
          '/MapServer/identify?f=json&geometry=%7B%22rings%22%3A%5B%5B%' \
          '5B{0}%' \
          '2C{1}%5D%2C%' \
          '5B{2}%' \
          '2C{1}%5D%2C%' \
          '5B{2}%' \
          '2C{3}%5D%2C%' \
          '5B{0}%' \
          '2C{3}%5D%2C%' \
          '5B{0}%' \
          '2C{1}%' \
          '5D%5D%5D%2C%22spatialReference%22%3A%7B%22wkid%22%3A28992%7D%7D&' \
          'tolerance=5&returnGeometry=true&mapExtent=%7B%22' \
          'xmin%22%3A0%2C%22' \
          'ymin%22%3A0%2C%22' \
          'xmax%22%3A0%2C%22' \
          'ymax%22%3A0%2C%22' \
          'spatialReference%22%3A%7B%22wkid%22%3A28992%7D%7D&' \
          'imageDisplay=811%2C421%2C96&geometryType=esriGeometryPolygon&sr=28992&' \
          'layers=all{4}'.format(xmin, ymin, xmax, ymax+eps, layerstring)
    return url

def user_agent_string(tech):
    """Creates user agent to be included into request.

    Args:
        tech: Technology indicator that is being parsed. format of argument is: 'LTE', 'UMTS', 'GSM', 'IoT'. Any other string will result in default value.

    Returns:
        A string of user agent.
    """
    return {
        'LTE': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
        'UMTS': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
        'GSM': 'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko',
        'IoT': 'Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16',
        'OverigMobile': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
        'VasteVerb': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15',
        'Omroep': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.61/63 Safari/537.36 Edg/100.0.1185.39'
    }.get(tech, 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36')

def url_response(session, url):
    """Creates url string for fetching antenna ids.

    Args:
        session: Requests session.
        url: URL string.

    Returns:
        A json formatted response of url.
    """
    headers = {'User-Agent': user_agent_string('')}
    response = None
    # request the URL and parse the JSON
    try:
        response = session.get(url, headers=headers)
        out = response.json()
    except requests.exceptions.Timeout:
        print('Timeout Error for url={0}'.format(url))
        out = {'results': []}
    except requests.exceptions.ConnectionError:
        print('Connection Error for url={0}'.format(url))
        out = {'results': []}
    except Exception as e:
        print(f"Unexpected error for url={url}: {e}")
        out = {'results': []}

    if response:
        print(response.json())

    return out


def list_of_locations(xmin, xmax, ylist, eps, layerstring):
    """Fetches partial information from antenna locations.

    Args:
        session: Requests session.
        url: URL string.

    Returns:
        A List of antenna locations and partial information from this locations.
    """
    outputLocs = []
    s = requests.Session()
    for i in range(len(ylist)-1): #loop crating url for all small rectangles and shooting request to collect antenna ids
        #  print(ylist[i], ylist[i+1])
        ymin = ylist[i]
        ymax = ylist[i+1]
        # URL definition
        url =url_generator(xmin, xmax, ymin, ymax, eps, layerstring)
        print(i, ': ', url)

        if i%1000==0:
            n_random = random.randint(50, 60)
            print('sleeping for {}sec'.format(n_random))
            time.sleep(n_random)
        elif i%100==0:
            n_random=random.randint(5, 15)
            print('sleeping for {}sec'.format(n_random))
            time.sleep(n_random)

        response =  url_response(s, url)
        while 'error' in response:
            n_random = random.randint(100, 150)
            print('error, sleeping for {}sec'.format(n_random))
            time.sleep(n_random)
            response = url_response(s, url)

        inputLocs =response['results']

        # inputLocs =['results']

        # print(inputLocs)

        for x in inputLocs: # loop to get all the antenna ids and add them into list
            # print(type(x))
            attrs = x['attributes']
            # print(attrs)
            outputLocs.append(attrs)
            # print(outputLocs)
    return outputLocs


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


def url_response_id(technology, id, session):
    url = url_string(technology, id)
    # print(url)
    headers = {'User-Agent': user_agent_string(technology)}
    # request the URL and parse the JSON
    try:
        response = session.get(url, headers=headers)
    except requests.exceptions.Timeout:
        print('Timeout Error for id={0}'.format(id))
        url = url_string(technology, 0)
        response = session.get(url, headers=headers)
    except requests.exceptions.ConnectionError:
        print('Connection Error for id={0}'.format(id))
        url = url_string(technology, 0)
        response = session.get(url, headers=headers)
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

    # Anttena layers
    gsmLayer = 2
    umtsLayer = 5
    lteLayer = 8
    # nbiotLayer = 23
    nrLayer=11
    tdabLayer = 14
    zmLayer = 16
    vvLayer = 20
    OverigMobileLayer = 23

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

    layerString = layers(
        gsmLayer
        , umtsLayer
        , lteLayer
        , nrLayer
        , tdabLayer
        , zmLayer
        , vvLayer
        , OverigMobileLayer)
    # layerString = layers(gsmLayer)
    # print(layerString)
    # layerString = layers(gsmLayer,umtsLayer)
    # print(layerString)

    outputLocs = list_of_locations(xMin, xMax, yList, epsilon, layerString)
    # print(outputLocs)

    # creates dataframe with anntena details including ids
    dfIDsDubs = pd.DataFrame(outputLocs)

    dfIDsDubs['DATUM_INGEBRUIKNAME'] = formating_date_column(dfIDsDubs, 'DATUM_INGEBRUIKNAME') #dfIDsDubs['DATUM_INGEBRUIKNAME'].apply(lambda x: formating_date(x))
    dfIDsDubs['DATUM_PLAATSING'] = formating_date_column(dfIDsDubs, 'DATUM_PLAATSING') #dfIDsDubs['DATUM_PLAATSING'].apply(lambda x: formating_date(x))

    # removing duplicates from above dataframe
    dfIDs = dfIDsDubs.drop_duplicates()
    print(dfIDs)

    # writing above dataframe into a file
    save_df_in_csv(dfIDs,fileDirectory, fileNameIDs)

    # dfIDs = pd.read_csv('D:\\Giorgi\\Antenneregister\\ids_all_20210616.csv')
    # dfIDs = pd.read_csv('D:\\Giorgi\\Antenneregister\\ids_overig_opt_20210618.csv')

    # print(dfIDs.head())
    # print(dfIDs.HOOFDSOORT.unique())

    df_LTE = dfIDs[dfIDs.HOOFDSOORT == 'LTE']
    df_UMTS = dfIDs[dfIDs.HOOFDSOORT == 'UMTS']
    df_GSM = dfIDs[dfIDs.HOOFDSOORT == 'GSM']
    # df_IoT = dfIDs[dfIDs.HOOFDSOORT == 'IoT']
    df_NR = dfIDs[dfIDs.HOOFDSOORT == '5G NR']
    df_OM = dfIDs[dfIDs.HOOFDSOORT == 'OVERIGMOBIEL']
    df_TDAB = dfIDs[dfIDs.HOOFDSOORT == 'OMROEP']
    df_ZM = dfIDs[dfIDs.HOOFDSOORT == 'ZENDAMATEURS']
    df_VV = dfIDs[dfIDs.HOOFDSOORT == 'VASTE VERB']


    save_df_in_csv(df_ZM, fileDirectory, '{0}_opt_{1}.csv'.format('ZendaMateurs', today.strftime('%Y%m%d')))

    dfList = {
        'LTE': df_LTE
        , 'UMTS': df_UMTS
        , 'GSM': df_GSM
        # , 'IoT': df_IoT
        , 'NR':df_NR
        , 'OverigMobile': df_OM
        , 'Omroep': df_TDAB
        # , 'ZendaMateurs': df_ZM
        , 'VasteVerb': df_VV
    }

    # print(dfList)
    #
    for technology, df in dfList.items():
        idList = df['id'].tolist()
        rows = []
        print('Parsing antenna details for {0} ids ... '.format(technology))
        print('Parsing {0} antenna details started on {1}.'.format(technology, time.strftime("%c")))

        # looping through all antenna ids, creating url for each ids, requesting and getting frequency data and organizing them into dataframe
        s = requests.Session()
        # m = 0
        for id in idList:
            # URL definition
            response = url_response_id(technology, id, s)
            # response = requests.get(url, headers=headers)
            # response.raise_for_status() # raise exception if invalid response
            # print(response.json())
            if len(response.json()['results']) != 0:
                inputFrequencies = response.json()['results'][0]
                # column names for the frequency dataframe
                columnList = inputFrequencies['linkedData']['columns']
                columnList.append('id')  # adds 'id' to column names
                #  list of dicts with frequency information
                rowList = inputFrequencies['linkedData']['rows']
                # print(rowList)
                # print(rows)
                # loop to get data from dicts and put it into convenient list form
                from_dict_to_list(rowList, rows, id)


        dfFrequencies = pd.DataFrame(rows, columns=columnList)
        # print(dfFrequencies)
        #  joining two dataframes
        df_out = pd.merge(df, dfFrequencies, how='left', left_on=['id'], right_on=['id'])
        # writing final dataframe to local file
        outFileName = '{0}_opt_{1}.csv'.format(technology, today.strftime('%Y%m%d'))
        save_df_in_csv(df_out,fileDirectory, outFileName)

    t1 = time.time()
    i, d = divmod((t1 - t0) / 60 / 60, 1)

    print('Parsing ended at {0}.'.format(time.strftime("%c")))
    print('Parsing took {0} hours and {1} minutes.'.format(round(i), round(d * 60, 1)))


# def main():
#     print('This is parser!')

if __name__ == '__main__':
    main() # The 0th arg is the module filename
