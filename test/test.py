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

import os
from datetime import datetime, timedelta
import pysftp

# dt = datetime.today()
dt = datetime.strptime('20211227', '%Y%m%d')

fileDirectory = 'D:\\Giorgi\\Antenneregister'
outputDirectory = 'D:\\Giorgi\\Antenneregister\\QV file'
# outputfileName = 'final_all_opt_{0}.csv'.format(today.strftime('%Y%m%d'))
outputfileName = 'final_all_opt_{0}.csv'.format(dt.strftime('%Y%m%d'))

host = "prohadoope02.ux.nl.tmo"
username = "antenna_registry"
password = "Karidia123!"

local_path = os.path.join(outputDirectory, outputfileName)
print(local_path)
remote_in_progress_path = "/IN_PROGRESS/{}".format(outputfileName)
print(remote_in_progress_path)
remote_final_path = "/FINAL/{}".format(outputfileName)
print(remote_final_path)

print('Trying to store cleaned data on sftp location')
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
with pysftp.Connection(host=host, username=username, password=password, cnopts=cnopts) as sftp:
    print('i am here')
    sftp.put(local_path, remote_in_progress_path)
    sftp.rename(remote_in_progress_path, remote_final_path)
print('Data stored on sftp location')