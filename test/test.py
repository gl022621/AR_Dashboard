# from teradataml.dataframe.fastload import fastload
# from teradatasqlalchemy.types import *
# from teradataml import create_context, DataFrame, get_connection, in_schema
# import pandas as pd
#
# eng = create_context(host = 'prdcop1.ux.nl.tmo', username='ID022621', password = 'Saqartvelo!!15', temp_database_name='DL_NETWORK_GEN')
# # print(eng)
# conn = get_connection()
# print(conn)
#
# print(eng.dialect.has_table(connection=conn, table_name="'DL_NETWORK_GEN'.'employee_info'"))
# create_stmt ="create table DL_NETWORK_GEN.employee_info (col_1 BIGINT, col_2 VARCHAR(50));"
# output = conn.execute(create_stmt)
#
# for row in output:
#     print(row)

# df=DataFrame.from_table('"DL_NETWORK_GEN"."employee_info"')
# print(df.columns)
# print(df.shape)
#
# ins_stmt = "insert into DL_NETWORK_GEN.employee_info values (28, 'tim')"
# ins_stmt1 = "insert into DL_NETWORK_GEN.employee_info values (31, 'tam')"
#
# ins = conn.execute(ins_stmt)
# ins1 = conn.execute(ins_stmt1)

# df = {
#     'col_1': [100, 200, 300, 400, 100, 200, 300, 400],
#     'col_2': ['A1', 'A2', 'A3', 'A4', 'A1', 'A2', 'A3', 'A4'],
# }
#
# df_pandas = pd.DataFrame(df)
#
# n=2
#
# chunks = [df_pandas[i:i+n] [:] for i in range(0,df_pandas.shape[0],n)]
#
# for df_temp in chunks:
#     print(df_temp)

# print(df_pandas)
#
# fastload(
#     df=df_pandas,
#     table_name='employee_info',
#     schema_name ='DL_NETWORK_GEN',
#     if_exists = 'append',
#     types = {'col_1': BIGINT, 'col_2': VARCHAR(50)}
# )
#
# df=DataFrame.from_table('"DL_NETWORK_GEN"."employee_info"')
# print(df.shape)

# df=DataFrame.from_table('"DL_NETWORK_GEN"."employee_info"')
# print(df.columns)



# print(eng.dialect.has_table(connection=conn, table_name='"DL_NETWORK_GEN"."woonplaats_info"'))

# query  = "select * from DL_NETWORK_GEN.ARDashboard sample 10"
# output = conn.execute(query)
#
# for row in output:
#     print(row)
#
# df = DataFrame("DL_NETWORK_GEN.ARDashboard")
# print(df)


# from teradataml import create_context, DataFrame
#
# create_context(host = 'prdcop1.ux.nl.tmo', username='ID022621', password = 'Saqartvelo!!15')
#
# # df =DataFrame.from_query("select * from DL_NETWORK_GEN.ARDashboard sample 10")
# df=DataFrame.from_table('"DL_NETWORK_GEN"."woonplaats_info"')
# print(df)

# import os
# from datetime import datetime, timedelta
# import pysftp
#
# # dt = datetime.today()
# dt = datetime.strptime('20211227', '%Y%m%d')
#
# fileDirectory = 'D:\\Giorgi\\Antenneregister'
# outputDirectory = 'D:\\Giorgi\\Antenneregister\\QV file'
# # outputfileName = 'final_all_opt_{0}.csv'.format(today.strftime('%Y%m%d'))
# outputfileName = 'final_all_opt_{0}.csv'.format(dt.strftime('%Y%m%d'))
#
# host = "prohadoope02.ux.nl.tmo"
# username = "antenna_registry"
# password = "Karidia123!"
#
# local_path = os.path.join(outputDirectory, outputfileName)
# print(local_path)
# remote_in_progress_path = "/IN_PROGRESS/{}".format(outputfileName)
# print(remote_in_progress_path)
# remote_final_path = "/FINAL/{}".format(outputfileName)
# print(remote_final_path)
#
# print('Trying to store cleaned data on sftp location')
# cnopts = pysftp.CnOpts()
# cnopts.hostkeys = None
# with pysftp.Connection(host=host, username=username, password=password, cnopts=cnopts) as sftp:
#     print('i am here')
#     sftp.put(local_path, remote_in_progress_path)
#     sftp.rename(remote_in_progress_path, remote_final_path)
# print('Data stored on sftp location')
#
#
# dict = {'error': {'code': 500, 'message': 'Error performing identify', 'details': []}}
#
# while 'error' in dict:
#     print(dict)
#     dict = {'result': {'code': 500, 'message': 'Error performing identify', 'details': []}}
#
# print(dict)


# import teradata
# import pandas as pd
#
# host = 'prdcop1.ux.nl.tmo'
# username = 'ID022621'
# password = 'Saqartvelo!!19'
# # host,username,password = 'HOST','UID', 'PWD'
# #Make a connection
# udaExec = teradata.UdaExec (appName="test", version="1.0", logConsole=False)
#
#
# with udaExec.connect(method="odbc",system=host, username=username, password=password) as connect:
#
#     query = """
#                 select
#                     Load_date
#                     ,count(*)
#                 from DL_NETWORK_GEN.ARdashboard
#                 group by
#                     Load_date
#                 order by
#                     Load_date desc;
#             """
#
#     #Reading query to df
#     df = pd.read_sql(query,connect)
#     # do something with df,e.g.
#     print(df.head()) #
#
# import teradatasql
#
# query = """
#                 select
#                     Load_date
#                     ,count(*)
#                 from DL_NETWORK_GEN.ARdashboard
#                 group by
#                     Load_date
#                 order by
#                     Load_date desc;
#             """
#
# with teradatasql.connect(host=host, user=username, password=password) as connect:
#     df = pd.read_sql(query, connect)
#     print(df)

# import jaydebeapi
# import pandas as pd
#
# # file_path = 'data.csv'
# # pdf = pd.read_csv(file_path)
#
# database = "DL_NETWORK_GEN"
# table = "ARdashboard"
# user = "ID022621"
# password = "Saqartvelo!!19"
# driver = 'com.teradata.jdbc.TeraDriver'
# conn = jaydebeapi.connect(driver,
#                           f'jdbc:teradata://prdcop1.ux.nl.tmo/Database={database}',
#                           [user, password],
#                           ["C:\\Users\\giorgi\\Documents\\TeraJDBC__indep_indep.17.20.00.12\\terajdbc4.jar"])
# cursor = conn.cursor()
# # cursor.execute(f"create multiset table {database}.{table} (ID int, Text1 VARCHAR(100), Text2 VARCHAR(100))")
# # cursor.executemany(f"""
# #         insert into {database}.{table} (ID, Text1, Text2)
# #         values (?, ?, ?)""", pdf.values.tolist())
#
# query = """
#                 select
#                     Load_date
#                     ,count(*)
#                 from DL_NETWORK_GEN.ARdashboard
#                 group by
#                     Load_date
#                 order by
#                     Load_date desc
#             """
#
# cursor.execute(query)
# print(cursor.fetchall())
# #
# # cursor.close()
# # conn.close()
#
#
# # from sqlalchemy import create_engine
# # import pandas as pd
#
# # file_path = 'data.csv'
# # pdf = pd.read_csv(file_path)
# #
# # database = "TestDb"
# # table = "csv_sqlalchemy"
# # user = "dbc"
# # password = "dbc"
#
#
# # database = "DL_NETWORK_GEN"
# # table = "ARdashboard"
# # user = "ID022621"
# # password = "Saqartvelo!!19"
# # host = 'prdcop1.ux.nl.tmo'
#
# # td_engine = create_engine(
# #     f'teradata://{user}:{password}@{host}/?database={database}&driver=Teradata Database ODBC Driver 16.10')
# # conn = td_engine.connect()
# # pdf.to_sql(name=table, con=conn, index=False, )
#
# #
# # # from sqlalchemy import create_engine
# #
# # #Make a connection
# #
# # # link = 'teradata://{username}:{password}@{hostname}/?driver={DRIVERNAME}'.format(
# # #                username=username,hostname=hostname,DRIVERNAME=DRIVERNAME)
# # #
# # # with create_engine(link) as connect:
# # #
# # #     #Reading query to df
# # #     df = pd.read_sql(query,connect)
# # #
# # #
# # # conn.close()
# #
# # td_engine = create_engine('teradata://'+ user +':' + password + '@'+ host + ':22/')
# #
# # # execute sql
# #
# # # td_engine = create_engine(f'teradata://{user}:{password}@{host}/?database={database}')
# # conn = td_engine.connect()
# # # pdf.to_sql(name=table, con=conn, index=False, if_exists='replace')
# # sql = """
# # #                 select
# # #                     Load_date
# # #                     ,count(*)
# # #                 from DL_NETWORK_GEN.ARdashboard
# # #                 group by
# # #                     Load_date
# # #                 order by
# # #                     Load_date desc
# # #             """
# # result = td_engine.execute(sql)
# # conn.close()



# from sqlalchemy import create_engine
# import sqlalchemy
# import teradatasqlalchemy


# engine = sqlalchemy.create_engine('teradata://' + user + ':' + password + '@' + host + ':22/' + database)
# # engine = create_engine('teradata://' + user + ':' + password + '@' + host + ':22/' + database)
# connection = engine.connect()
# # connection = engine.raw_connection()
# cursor = connection.cursor()
# cursor.execute(
#     """
#         select
#             Load_date
#             ,count(*)
#         from DL_NETWORK_GEN.ARdashboard
#         group by
#             Load_date
#             order by
#             Load_date desc
#     """
# )
# connection.commit()

#conn.close()
# connection.close()
#
#
# from sqlalchemy import create_engine
# database = "DL_NETWORK_GEN"
# table = "ARdashboard"
# user = "ID022621"
# password = "Saqartvelo!!19"
# host = 'prdcop1.ux.nl.tmo'
#
#
# # connect
# td_engine = create_engine('teradata://'+ user +':' + password + '@'+ host + ':22/'+ database)
#
# # execute sql
# sql = """select
#             Load_date
#             ,count(*)
#         from DL_NETWORK_GEN.ARdashboard
#         group by
#             Load_date
#             order by
#             Load_date desc"""
# result = td_engine.execute(sql)
# print(result)


# import module
from datetime import datetime

# get current date and time
current_datetime = f"{datetime.now():%Y%m%d%H%M%S}"
print("Current date & time : ", current_datetime)

# convert datetime obj to string
str_current_datetime = str(current_datetime)

# create a file object along with extension
file_name = "file_"+str_current_datetime + ".txt"
file = open(file_name, 'w')

print("File created : ", file.name)
file.close()