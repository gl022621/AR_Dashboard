# # # # from teradataml.dataframe.fastload import fastload
# # # # from teradatasqlalchemy.types import *
# # # # from teradataml import create_context, DataFrame, get_connection, in_schema
# # # # import pandas as pd
# # # #
# # # # eng = create_context(host = 'prdcop1.ux.nl.tmo', username='ID022621', password = 'Saqartvelo!!15', temp_database_name='DL_NETWORK_GEN')
# # # # # print(eng)
# # # # conn = get_connection()
# # # # print(conn)
# # # #
# # # # print(eng.dialect.has_table(connection=conn, table_name="'DL_NETWORK_GEN'.'employee_info'"))
# # # # create_stmt ="create table DL_NETWORK_GEN.employee_info (col_1 BIGINT, col_2 VARCHAR(50));"
# # # # output = conn.execute(create_stmt)
# # # #
# # # # for row in output:
# # # #     print(row)
# # #
# # # # df=DataFrame.from_table('"DL_NETWORK_GEN"."employee_info"')
# # # # print(df.columns)
# # # # print(df.shape)
# # # #
# # # # ins_stmt = "insert into DL_NETWORK_GEN.employee_info values (28, 'tim')"
# # # # ins_stmt1 = "insert into DL_NETWORK_GEN.employee_info values (31, 'tam')"
# # # #
# # # # ins = conn.execute(ins_stmt)
# # # # ins1 = conn.execute(ins_stmt1)
# # #
# # # # df = {
# # # #     'col_1': [100, 200, 300, 400, 100, 200, 300, 400],
# # # #     'col_2': ['A1', 'A2', 'A3', 'A4', 'A1', 'A2', 'A3', 'A4'],
# # # # }
# # # #
# # # # df_pandas = pd.DataFrame(df)
# # # #
# # # # n=2
# # # #
# # # # chunks = [df_pandas[i:i+n] [:] for i in range(0,df_pandas.shape[0],n)]
# # # #
# # # # for df_temp in chunks:
# # # #     print(df_temp)
# # #
# # # # print(df_pandas)
# # # #
# # # # fastload(
# # # #     df=df_pandas,
# # # #     table_name='employee_info',
# # # #     schema_name ='DL_NETWORK_GEN',
# # # #     if_exists = 'append',
# # # #     types = {'col_1': BIGINT, 'col_2': VARCHAR(50)}
# # # # )
# # # #
# # # # df=DataFrame.from_table('"DL_NETWORK_GEN"."employee_info"')
# # # # print(df.shape)
# # #
# # # # df=DataFrame.from_table('"DL_NETWORK_GEN"."employee_info"')
# # # # print(df.columns)
# # #
# # #
# # #
# # # # print(eng.dialect.has_table(connection=conn, table_name='"DL_NETWORK_GEN"."woonplaats_info"'))
# # #
# # # # query  = "select * from DL_NETWORK_GEN.ARDashboard sample 10"
# # # # output = conn.execute(query)
# # # #
# # # # for row in output:
# # # #     print(row)
# # # #
# # # # df = DataFrame("DL_NETWORK_GEN.ARDashboard")
# # # # print(df)
# # #
# # #
# # # # from teradataml import create_context, DataFrame
# # # #
# # # # create_context(host = 'prdcop1.ux.nl.tmo', username='ID022621', password = 'Saqartvelo!!15')
# # # #
# # # # # df =DataFrame.from_query("select * from DL_NETWORK_GEN.ARDashboard sample 10")
# # # # df=DataFrame.from_table('"DL_NETWORK_GEN"."woonplaats_info"')
# # # # print(df)
# # #
# # # # import os
# # # # from datetime import datetime, timedelta
# # # # import pysftp
# # # #
# # # # # dt = datetime.today()
# # # # dt = datetime.strptime('20211227', '%Y%m%d')
# # # #
# # # # fileDirectory = 'D:\\Giorgi\\Antenneregister'
# # # # outputDirectory = 'D:\\Giorgi\\Antenneregister\\QV file'
# # # # # outputfileName = 'final_all_opt_{0}.csv'.format(today.strftime('%Y%m%d'))
# # # # outputfileName = 'final_all_opt_{0}.csv'.format(dt.strftime('%Y%m%d'))
# # # #
# # # # host = "prohadoope02.ux.nl.tmo"
# # # # username = "antenna_registry"
# # # # password = "Karidia123!"
# # # #
# # # # local_path = os.path.join(outputDirectory, outputfileName)
# # # # print(local_path)
# # # # remote_in_progress_path = "/IN_PROGRESS/{}".format(outputfileName)
# # # # print(remote_in_progress_path)
# # # # remote_final_path = "/FINAL/{}".format(outputfileName)
# # # # print(remote_final_path)
# # # #
# # # # print('Trying to store cleaned data on sftp location')
# # # # cnopts = pysftp.CnOpts()
# # # # cnopts.hostkeys = None
# # # # with pysftp.Connection(host=host, username=username, password=password, cnopts=cnopts) as sftp:
# # # #     print('i am here')
# # # #     sftp.put(local_path, remote_in_progress_path)
# # # #     sftp.rename(remote_in_progress_path, remote_final_path)
# # # # print('Data stored on sftp location')
# # # #
# # # #
# # # # dict = {'error': {'code': 500, 'message': 'Error performing identify', 'details': []}}
# # # #
# # # # while 'error' in dict:
# # # #     print(dict)
# # # #     dict = {'result': {'code': 500, 'message': 'Error performing identify', 'details': []}}
# # # #
# # # # print(dict)
# # #
# # #
# # # # import teradata
# # # # import pandas as pd
# # # #
# # # # host = 'prdcop1.ux.nl.tmo'
# # # # username = 'ID022621'
# # # # password = 'Saqartvelo!!19'
# # # # # host,username,password = 'HOST','UID', 'PWD'
# # # # #Make a connection
# # # # udaExec = teradata.UdaExec (appName="test", version="1.0", logConsole=False)
# # # #
# # # #
# # # # with udaExec.connect(method="odbc",system=host, username=username, password=password) as connect:
# # # #
# # # #     query = """
# # # #                 select
# # # #                     Load_date
# # # #                     ,count(*)
# # # #                 from DL_NETWORK_GEN.ARdashboard
# # # #                 group by
# # # #                     Load_date
# # # #                 order by
# # # #                     Load_date desc;
# # # #             """
# # # #
# # # #     #Reading query to df
# # # #     df = pd.read_sql(query,connect)
# # # #     # do something with df,e.g.
# # # #     print(df.head()) #
# # # #
# # # # import teradatasql
# # # #
# # # # query = """
# # # #                 select
# # # #                     Load_date
# # # #                     ,count(*)
# # # #                 from DL_NETWORK_GEN.ARdashboard
# # # #                 group by
# # # #                     Load_date
# # # #                 order by
# # # #                     Load_date desc;
# # # #             """
# # # #
# # # # with teradatasql.connect(host=host, user=username, password=password) as connect:
# # # #     df = pd.read_sql(query, connect)
# # # #     print(df)
# # #
# # # # import jaydebeapi
# # # # import pandas as pd
# # # #
# # # # # file_path = 'data.csv'
# # # # # pdf = pd.read_csv(file_path)
# # # #
# # # # database = "DL_NETWORK_GEN"
# # # # table = "ARdashboard"
# # # # user = "ID022621"
# # # # password = "Saqartvelo!!19"
# # # # driver = 'com.teradata.jdbc.TeraDriver'
# # # # conn = jaydebeapi.connect(driver,
# # # #                           f'jdbc:teradata://prdcop1.ux.nl.tmo/Database={database}',
# # # #                           [user, password],
# # # #                           ["C:\\Users\\giorgi\\Documents\\TeraJDBC__indep_indep.17.20.00.12\\terajdbc4.jar"])
# # # # cursor = conn.cursor()
# # # # # cursor.execute(f"create multiset table {database}.{table} (ID int, Text1 VARCHAR(100), Text2 VARCHAR(100))")
# # # # # cursor.executemany(f"""
# # # # #         insert into {database}.{table} (ID, Text1, Text2)
# # # # #         values (?, ?, ?)""", pdf.values.tolist())
# # # #
# # # # query = """
# # # #                 select
# # # #                     Load_date
# # # #                     ,count(*)
# # # #                 from DL_NETWORK_GEN.ARdashboard
# # # #                 group by
# # # #                     Load_date
# # # #                 order by
# # # #                     Load_date desc
# # # #             """
# # # #
# # # # cursor.execute(query)
# # # # print(cursor.fetchall())
# # # # #
# # # # # cursor.close()
# # # # # conn.close()
# # # #
# # # #
# # # # # from sqlalchemy import create_engine
# # # # # import pandas as pd
# # # #
# # # # # file_path = 'data.csv'
# # # # # pdf = pd.read_csv(file_path)
# # # # #
# # # # # database = "TestDb"
# # # # # table = "csv_sqlalchemy"
# # # # # user = "dbc"
# # # # # password = "dbc"
# # # #
# # # #
# # # # # database = "DL_NETWORK_GEN"
# # # # # table = "ARdashboard"
# # # # # user = "ID022621"
# # # # # password = "Saqartvelo!!19"
# # # # # host = 'prdcop1.ux.nl.tmo'
# # # #
# # # # # td_engine = create_engine(
# # # # #     f'teradata://{user}:{password}@{host}/?database={database}&driver=Teradata Database ODBC Driver 16.10')
# # # # # conn = td_engine.connect()
# # # # # pdf.to_sql(name=table, con=conn, index=False, )
# # # #
# # # # #
# # # # # # from sqlalchemy import create_engine
# # # # #
# # # # # #Make a connection
# # # # #
# # # # # # link = 'teradata://{username}:{password}@{hostname}/?driver={DRIVERNAME}'.format(
# # # # # #                username=username,hostname=hostname,DRIVERNAME=DRIVERNAME)
# # # # # #
# # # # # # with create_engine(link) as connect:
# # # # # #
# # # # # #     #Reading query to df
# # # # # #     df = pd.read_sql(query,connect)
# # # # # #
# # # # # #
# # # # # # conn.close()
# # # # #
# # # # # td_engine = create_engine('teradata://'+ user +':' + password + '@'+ host + ':22/')
# # # # #
# # # # # # execute sql
# # # # #
# # # # # # td_engine = create_engine(f'teradata://{user}:{password}@{host}/?database={database}')
# # # # # conn = td_engine.connect()
# # # # # # pdf.to_sql(name=table, con=conn, index=False, if_exists='replace')
# # # # # sql = """
# # # # # #                 select
# # # # # #                     Load_date
# # # # # #                     ,count(*)
# # # # # #                 from DL_NETWORK_GEN.ARdashboard
# # # # # #                 group by
# # # # # #                     Load_date
# # # # # #                 order by
# # # # # #                     Load_date desc
# # # # # #             """
# # # # # result = td_engine.execute(sql)
# # # # # conn.close()
# # #
# # #
# # #
# # # # from sqlalchemy import create_engine
# # # # import sqlalchemy
# # # # import teradatasqlalchemy
# # #
# # #
# # # # engine = sqlalchemy.create_engine('teradata://' + user + ':' + password + '@' + host + ':22/' + database)
# # # # # engine = create_engine('teradata://' + user + ':' + password + '@' + host + ':22/' + database)
# # # # connection = engine.connect()
# # # # # connection = engine.raw_connection()
# # # # cursor = connection.cursor()
# # # # cursor.execute(
# # # #     """
# # # #         select
# # # #             Load_date
# # # #             ,count(*)
# # # #         from DL_NETWORK_GEN.ARdashboard
# # # #         group by
# # # #             Load_date
# # # #             order by
# # # #             Load_date desc
# # # #     """
# # # # )
# # # # connection.commit()
# # #
# # # #conn.close()
# # # # connection.close()
# # # #
# # # #
# # # # from sqlalchemy import create_engine
# # # # database = "DL_NETWORK_GEN"
# # # # table = "ARdashboard"
# # # # user = "ID022621"
# # # # password = "Saqartvelo!!19"
# # # # host = 'prdcop1.ux.nl.tmo'
# # # #
# # # #
# # # # # connect
# # # # td_engine = create_engine('teradata://'+ user +':' + password + '@'+ host + ':22/'+ database)
# # # #
# # # # # execute sql
# # # # sql = """select
# # # #             Load_date
# # # #             ,count(*)
# # # #         from DL_NETWORK_GEN.ARdashboard
# # # #         group by
# # # #             Load_date
# # # #             order by
# # # #             Load_date desc"""
# # # # result = td_engine.execute(sql)
# # # # print(result)
# # #
# # #
# # # # import module
# # # from datetime import datetime
# # #
# # # # get current date and time
# # # current_datetime = f"{datetime.now():%Y%m%d%H%M%S}"
# # # print("Current date & time : ", current_datetime)
# # #
# # # # convert datetime obj to string
# # # str_current_datetime = str(current_datetime)
# # #
# # # # create a file object along with extension
# # # file_name = "file_"+str_current_datetime + ".txt"
# # # file = open(file_name, 'w')
# # #
# # # print("File created : ", file.name)
# # # file.close()
# #
# #
# # # import urllib.parse
# # # import pandas as pd
# # # import requests
# # # import csv
# # # from math import floor, ceil
# # # import numpy as np
# # #
# # # desired_display_width=320
# # # desired_columns = 20
# # # desired_column_width=200
# # #
# # # pd.set_option('display.width', desired_display_width)
# # # pd.set_option('display.max_columns',desired_columns)
# # # pd.set_option('display.max_colwidth', desired_column_width)
# # #
# # #
# # # def main():
# # #     s = requests.Session()
# # #
# # #     XMin = 13666.3859999999
# # #     YMin = 306922.077
# # #     XMax = 277709.452100001
# # #     # YMax = 308000
# # #     YMax = 611847
# # #     Spatial_Reference = 28992
# # #     n = 2000
# # #
# # #     # print(np.linspace(YMin, YMax, n))
# # #     YList = np.linspace(YMin, YMax, n)
# # #
# # #     dataList = []
# # #
# # #     for i in range(len(YList)-1):
# # #         xmin = XMin
# # #         ymin = YList[i]
# # #         xmax = XMax
# # #         ymax = YList[i+1]
# # #
# # #         # coords='{"xmin":85513.92539401863,"ymin":452411.66116276896,"xmax":88151.00286985651,"ymax":455048.73863860685,"spatialReference":{"wkid":28992,"latestWkid":28992}}'
# # #         coords='{{"xmin":{0},"ymin":{1},"xmax":{2},"ymax":{3},"spatialReference":{{"wkid":{4},"latestWkid":{4}}}}}'.format(xmin, ymin, xmax, ymax, Spatial_Reference)
# # #         # print(coords)
# # #
# # #         payload = {
# # #             'f': 'json'
# # #             ,'returnGeometry': 'true'
# # #             ,'spatialRel': 'esriSpatialRelIntersects'
# # #             # ,'geometry': '{"xmin":85513.92539401863,"ymin":452411.66116276896,"xmax":88151.00286985651,"ymax":455048.73863860685,"spatialReference":{"wkid":28992,"latestWkid":28992}}'
# # #             ,'geometry': coords
# # #             ,'geometryType': 'esriGeometryEnvelope'
# # #             ,'inSR': '28992'
# # #             ,'outFields': '*'
# # #             ,'returnCentroid': 'false'
# # #             ,'returnExceededLimitFeatures': 'false'
# # #             ,'maxRecordCountFactor': '3'
# # #             ,'outSR': '28992'
# # #             ,'resultType': 'tile'
# # #             # ,'quantizationParameters': '{"mode":"view","originPosition":"upperLeft","tolerance":4.77731426782227,"extent":{"xmin":83163.48677425008,"ymin":454953.1923532504,"xmax":85609.47167937508,"ymax":457399.1772583754,"spatialReference":{"wkid":28992,"latestWkid":28992}}}'
# # #         }
# # #
# # #         # print(type(urllib.parse.urlencode(payload)))
# # #         request_url = 'https://services.arcgis.com/kE0BiyvJHb5SwQv7/arcgis/rest/services/Internetverbindingen_kaart_20211_WFL1/FeatureServer/5/query?'\
# # #                       + urllib.parse.urlencode(payload)
# # #         print("{}: ".format(i), request_url)
# # #
# # #         response = s.get(request_url)
# # #         data = response.json()['features']
# # #         # print(type(data))
# # #         # print(data)
# # #
# # #         dataList=dataList+data
# # #         # print(dataList)
# # #     #
# # #     #     df = pd.json_normalize(data)
# # #     #     print(df.shape)
# # #     #     print(df.head())
# # #     #
# # #     #
# # #     df = pd.json_normalize(dataList)
# # #     print(df.shape)
# # #     print(df.head())
# # #     df_out = df.drop_duplicates()
# # #     df_out.to_csv('results.csv', index=False, quoting=csv.QUOTE_ALL)
# # #
# # # # Press the green button in the gutter to run the script.
# # # if __name__ == '__main__':
# # #     main()
# # #
# # #
# # # import csv
# # # import re
# # # import teradata
# # # import pysftp
# # #
# # # from datetime import datetime
# # # import smtplib
# # # import zipfile
# # # import os
# # #
# # # from email.mime.text import MIMEText
# # # from email.mime.multipart import MIMEMultipart
# # # from email.mime.application import MIMEApplication
# # #
# # # def send_email_with_attachment(sender, password, recipients, subject, body, file_path=None, cc=None, bcc=None):
# # #     print("num 1")
# # #     msg = MIMEMultipart()
# # #     msg['From'] = sender
# # #     msg['To'] = ", ".join(recipients)
# # #     msg['Cc'] = cc
# # #     msg['Subject'] = subject
# # #
# # #     msg.attach(MIMEText(body))
# # #
# # #     print("num 2")
# # #
# # #     if file_path:
# # #         with zipfile.ZipFile(file_path + '.zip', 'w') as myzip:
# # #             myzip.write(file_path)
# # #
# # #         print("num 3")
# # #         with open(file_path + '.zip', "rb") as f:
# # #             part = MIMEApplication(f.read(), Name=os.path.basename(file_path + '.zip'))
# # #             part['Content-Disposition'] = 'attachment; filename="{}"'.format(os.path.basename(file_path + '.zip'))
# # #             msg.attach(part)
# # #             print("num 4")
# # #
# # #     print("num 5")
# # #     if cc and bcc:
# # #         rcpt = cc + bcc + recipients
# # #     elif cc:
# # #         rcpt = cc + recipients
# # #     elif bcc:
# # #         rcpt = bcc + recipients
# # #     else:
# # #         rcpt = recipients
# # #
# # #     print(rcpt)
# # #
# # #     print("num 6")
# # #     # smtp = smtplib.SMTP('smtp.gmail.com', 587)
# # #     smtp = smtplib.SMTP("smtp.gmail.com", 587, timeout=120)
# # #     # smtp = smtplib.SMTP_SSL("smtp.gmail.com", 465)
# # #     print("num 7")
# # #     smtp.ehlo()
# # #     print("num 8")
# # #     smtp.starttls()
# # #     print("num 9")
# # #     smtp.login(sender, password)
# # #     print("num 10")
# # #     smtp.sendmail(sender, rcpt, msg.as_string())
# # #     print("num 11")
# # #     smtp.quit()
# # #
# # # dt = datetime.strptime('20230127', '%Y%m%d')
# # # # fileDirectory = 'D:\\Giorgi\\Antenneregister'
# # # outputDirectory = 'D:\\Giorgi\\Antenneregister\\QV file'
# # # # outputfileName = 'final_all_opt_{0}.csv'.format(today.strftime('%Y%m%d'))
# # # outputFileNameMain = 'final_all_opt_{0}.csv'.format(dt.strftime('%Y%m%d'))
# # # # outputFileNameOM = 'OVERIGMOBIEL_{0}.csv'.format(dt.strftime('%Y%m%d'))
# # # # outputFileNameVV = 'VASTEVERB_{0}.csv'.format(dt.strftime('%Y%m%d'))
# # # # outputFileNameO = 'OMROEP_{0}.csv'.format(dt.strftime('%Y%m%d'))
# # #
# # # print(os.path.join(outputDirectory, outputFileNameMain))
# # #
# # # send_email_with_attachment(sender='gio.labadze+antenneregister@gmail.com',
# # #                            password='iqimjptucwkermtp',
# # #                            recipients=['giorgi.labadze@t-mobile.nl'],
# # #                            subject='Antenneregister data',
# # #                            body='Hi Rob,\nPlease find the attached with the latest Anntenaregister data.\nCheers,\nGiorgi',
# # #                            file_path=os.path.join(outputDirectory, outputFileNameMain),
# # #                            # bcc=['giorgi.labadze@t-mobile.nl']
# # #                            )
# #
# # import pysftp
# #
# # local_path = os.path.join(outputDirectory, outputFileNameMain)
# # remote_final_path = "/{}".format(outputFileNameMain)
# #
# # hostname = "172.27.0.69"
# # username = "ardashboard"
# # keyFile = "D:\\Giorgi\\Antenneregister\\Useful files\\ardashboard_key"
# # cnopts = pysftp.CnOpts()
# # cnopts.hostkeys = None
# #
# # with pysftp.Connection(hostname, username=username, private_key=keyFile, cnopts=cnopts) as sftp:
# #     print("Connection succesfully stablished ... ")
#
# # import paramiko
# # ssh = paramiko.SSHClient()
#
# # hostname = "172.27.0.69"
# # username = "ardashboard"
# # keyFile = "D:\\Giorgi\\Antenneregister\\Useful files\\ardashboard_key"
# # # keyFile = "../source/ardashboard_key"
# # #
# # ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
# #
# # ssh.connect(hostname, username=username, key_filename=keyFile)
# #
# # stdin, stdout, stderr = ssh.exec_command('ls')
# # print (stdout.readlines())
# # ssh.close()
#
# import teradata
# import pandas as pd
#
# host = 'prdcop1.ux.nl.tmo'
# username = 'ID022621'
# password = 'Saqartvelo!!05'
#
# udaExec = teradata.UdaExec(appName="AR_deshboard", version="1.0", logConsole=False)
# with udaExec.connect(method="odbc", system=host, username=username, password=password, charset='UTF8') as connect:
#     # query = """
#     #         select * from DL_NETWORK_GEN.ARdashboard_main
#     #         where 1=1
#     #         and band = '3500'
#     #         sample 10
#     #         """
#     #
#     # df_ar = pd.read_sql(query, connect)
#
#     connect.execute(
#         '''
#                 update DL_NETWORK_GEN.ARdashboard
#                 set Operator = 'Odido'
#                 where 1=1
#                 and Operator = 'T-Mobile'
#         '''
#     )
#
# # print(df_ar)

import smtplib
import zipfile
import os
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

def send_email_with_attachment(sender, password, recipients, subject, body, file_path, cc=None, bcc=None):
    # Create a multipart message
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject

    if cc:
        msg['Cc'] = ", ".join(cc)
    if bcc:
        msg['Bcc'] = ", ".join(bcc)

    # Attach the body with the msg instance
    msg.attach(MIMEText(body, 'plain'))

    # Create a zip file
    zip_file_path = file_path + '.zip'
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        zipf.write(file_path, os.path.basename(file_path))

    # Open the file in binary mode
    with open(zip_file_path, "rb") as attachment:
        part = MIMEApplication(attachment.read(), Name=os.path.basename(zip_file_path))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(zip_file_path)}"'
        msg.attach(part)

    # Create SMTP session for sending the mail
    with smtplib.SMTP('smtp.gmail.com', 587, timeout=120) as server:
        server.starttls()  # Enable security
        server.login(sender, password)  # Login with mail_id and password
        server.sendmail(sender, recipients + (cc if cc else []) + (bcc if bcc else []), msg.as_string())



dt = datetime.strptime('20250227', '%Y%m%d')
outputFileNameMain = 'final_all_opt_{0}.csv'.format(dt.strftime('%Y%m%d'))
dirName = 'D:\Giorgi\Antenneregister\QV file'
file_path = os.path.join(dirName, outputFileNameMain)
# Example usage
send_email_with_attachment(
    sender='gio.labadze@gmail.com',
    password='48918091',
    recipients=['giorgi.labadze@odido.nl'],
    subject='test',
    body='Body of the email',
    file_path=file_path
)
