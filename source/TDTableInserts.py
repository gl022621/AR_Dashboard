import numpy as np
import pandas as pd
import geopandas as gpd
import teradata
from datetime import datetime
from dateutil.relativedelta import relativedelta
import jaydebeapi

desired_width = 520
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option("display.precision", 13)


def main():

    shp_path = '../woonplaats_shapefiles/City_population_region.shp'
    df = gpd.read_file(shp_path)
    print(df.dtypes)
    print(df.memory_usage(deep=True))
    print(df.select_dtypes(float).describe())

    df = (df
     .astype({
        'Population': 'int32',
     })
     .rename(
        columns = {
            "WOONPLAATS": "WOONPLAATS_SF"
            , "GEMNAAM": "GEMNAAM_SF"
        }
     )
     .assign(
        wp_pop_rank = df.Population.rank(na_option = 'bottom', ascending = False).astype(int),
        wp_pop_density = df.Population / df.OPP,
        wp_pop_density_rank=lambda x: (x.wp_pop_density.rank(na_option='bottom', ascending=False).astype(int))
     )
    )

    print(df.columns)
    print(df.head())

    # df['Population'] = df.Population.astype(int)
    # df = df.rename(columns={"WOONPLAATS": "WOONPLAATS_SF", "GEMNAAM": "GEMNAAM_SF"})
    #
    # df['wp_pop_rank'] = df['Population'].rank(na_option='bottom', ascending=False).astype(int)
    # df['wp_pop_density'] = df['Population'] / df['OPP']
    # df['wp_pop_density_rank'] = df['wp_pop_density'].rank(na_option='bottom', ascending=False).astype(int)
    #
    pop_q1 = 0.02
    pop_den_q1 = 0.04
    pop_q2 = 0.13
    pop_den_q2 = 0.13

    df = (df
        .assign(
        gem_type = np.where(((df.wp_pop_rank < df.wp_pop_rank.quantile(pop_q1)) & (df.wp_pop_density_rank < df.wp_pop_density_rank.quantile(pop_den_q1)))
                              , 'Urban'
                              , np.where(((df.wp_pop_rank > df.wp_pop_rank.quantile(pop_q2)) | (df.wp_pop_density_rank > df.wp_pop_density_rank.quantile(pop_den_q2)))
                                         , 'Rural'
                                         , 'Sub-urban')
                              )
    )
    )

    # df['gem_type'] = np.where(((df.wp_pop_rank < df.wp_pop_rank.quantile(pop_q1)) & (
    #             df.wp_pop_density_rank < df.wp_pop_density_rank.quantile(pop_den_q1)))
    #                           , 'Urban'
    #                           , np.where(((df.wp_pop_rank > df.wp_pop_rank.quantile(pop_q2)) | (
    #                 df.wp_pop_density_rank > df.wp_pop_density_rank.quantile(pop_den_q2)))
    #                                      , 'Rural'
    #                                      , 'Sub-urban')
    #                           )
    print('df: ', df.dtypes)
    print(df.head(n=100))
    print(df.loc[df.GEMNAAM_SF=='De Fryske Marren',])


    host = 'prdcop1.ux.nl.tmo'
    username = 'ID022621'
    password = 'Saqartvelo!!08'

    # #  Establish the connection to the Teradata database
    # database = "DL_NETWORK_GEN"
    # table = "woonplaats_info"
    # user = "ID022621"
    # password = "Saqartvelo!!01"
    # driver = 'com.teradata.jdbc.TeraDriver'
    #
    # conn = jaydebeapi.connect(driver,
    #                           f'jdbc:teradata://prdcop1.ux.nl.tmo/Database={database}',
    #                           [user, password],
    #                           ["C:\\Users\\giorgi\\Documents\\TeraJDBC__indep_indep.17.20.00.12\\terajdbc4.jar"])
    # cursor = conn.cursor()
    #
    # query = """
    #                 select
    #                     round(lat, 13) as lat
    #                     ,round(lon, 13) as lon
    #                     , lower(gemnaam) as gemnaam
    #                     , lower(woonplaatsnaam) as woonplaatsnaam
    #                 from DL_NETWORK_GEN.ARdashboard
    #                 group by
    #                     round(lat, 13)
    #                     ,round(lon, 13)
    #                     , lower(gemnaam)
    #                     , lower(woonplaatsnaam)
    #                 """;
    udaExec = teradata.UdaExec(appName="AR_deshboard", version="1.0", logConsole=False)
    with udaExec.connect(method="odbc", system=host, username=username, password=password, charset='UTF8') as connect:
        query = """
                select
                    round(lat, 13) as lat
                    ,round(lon, 13) as lon
                    , lower(gemnaam) as gemnaam
                    , lower(woonplaatsnaam) as woonplaatsnaam
                from DL_NETWORK_GEN.ARdashboard
                group by
                    round(lat, 13)
                    ,round(lon, 13)
                    , lower(gemnaam)
                    , lower(woonplaatsnaam)
                """;

        # Reading query to df
        df_ar = pd.read_sql(query, connect)

    # # Reading query to df
    # # df_ar = pd.read_sql(query, connect)
    # cursor.execute(query)
    # print(cursor.fetchall())
    # df_ar=pd.DataFrame(cursor.fetchall())
    # df_ar=(df_ar
    #        .rename(columns={0: "lat", 1: "lon", 2: "gemnaam", 3: "woonplaatsnaam"})
    #        .query("lat!=52.3640509710163 |  woonplaatsnaam != 'null' ")
    #        )
    # print(df_ar.shape)
    # (24873, 4)

    # print('df_ar:',df_ar.dtypes)
    # print(df_ar.loc[df_ar.GEMNAAM_SF == 'De Fryske Marren',])
    # print(df_ar.head())
    # print(df_ar.loc[df_ar.lat == 52.9743069774552,])
    # print(df_ar.loc[df_ar.gemnaam == 'Gaasterlân-Sleat',])

    # df_ar = df_ar.loc[~((df_ar.lat == 52.3640509710163) & (df_ar.woonplaatsnaam == 'null')),]
    # print(df_ar.shape)
    df_ar_gpd = gpd.GeoDataFrame(
        df_ar
        , geometry=gpd.points_from_xy(df_ar.lon, df_ar.lat)
        , crs="EPSG:4326"
    )
    # print(df_ar_gpd.loc[df_ar_gpd.gemnaam == 'harderwijk',].sort_values())
    # print('df_ar_gpd: ',df_ar_gpd.dtypes)
    # print(df_ar_gpd.loc[df_ar_gpd.gemnaam == 'De Fryske Marren',])

    df_merged = gpd.sjoin(df_ar_gpd, df, how="inner", predicate='intersects')
    # print(df_merged.head())
    # print(df_merged.loc[df_merged.gemnaam=='harderwijk' ,])
    # print(df_merged.shape)
    # print(df_merged.loc[df_merged.GEMNAAM_SF=='De Fryske Marren',])


    df_temp = df_ar_gpd.merge(
        df_merged
        , how='left'
        , left_on=['lat', 'lon', 'gemnaam', 'woonplaatsnaam']
        , right_on=['lat', 'lon', 'gemnaam', 'woonplaatsnaam']
        , suffixes=('_left', '_right')
    )
    df_missing = df_temp.loc[df_temp.GEMNAAM_SF.isnull()].drop(
        ['geometry_left', 'geometry_right', 'index_right', 'WOONPLAATS_SF', 'GEMNAAM_SF', 'PROVAFK', 'OPP', 'Population',
         'wp_pop_rank', 'wp_pop_density', 'wp_pop_density_rank', 'gem_type'], axis=1)
    # df_missing.sort_values('gemnaam', ascending=False)

    # print(df_missing)
    # print(df_missing.loc[df_missing.gemnaam=='harderwijk' ,])

    df_merged = pd.concat([df_merged.drop(['index_right', 'geometry'], axis=1),
                           df_missing.merge(
                               df
                               , how='inner'
                               , left_on=[df_missing['gemnaam'].str.lower(), df_missing['woonplaatsnaam'].str.lower()]
                               , right_on=[df['GEMNAAM_SF'].str.lower(), df['WOONPLAATS_SF'].str.lower()]
                               , suffixes=('_left', '_right')
                           ).drop(['key_0', 'key_1', 'geometry'], axis=1)])

    df_temp = df_ar_gpd.merge(
        df_merged
        , how='left'
        , left_on=['lat', 'lon', 'gemnaam', 'woonplaatsnaam']
        , right_on=['lat', 'lon', 'gemnaam', 'woonplaatsnaam']
        , suffixes=('_left', '_right')
    )
    # print(df_temp.shape)
    # print(df_temp.head())

    df_missing = (df_temp
                  .query('GEMNAAM_SF.isnull()')
                  .drop(['geometry'], axis=1)
                  .assign(
                        OPP = 0 ,
                        Population = 0,
                        wp_pop_rank = 0,
                        wp_pop_density = 0,
                        wp_pop_density_rank = 0,
                        gem_type = 'Unknown'
                    )
                  )
    print(df_missing.shape)

    # df_missing = df_temp.loc[df_temp.GEMNAAM_SF.isnull()].drop(['geometry'], axis=1)
    # df_missing['OPP'] = 0
    # df_missing['Population'] = 0
    # df_missing['wp_pop_rank'] = 0
    # df_missing['wp_pop_density'] = 0
    # df_missing['wp_pop_density_rank'] = 0
    # df_missing['gem_type'] = 'Unknown'
    # print(df_missing)
    # df_missing
    # df_missing.sort_values('gemnaam', ascending=False)


    df_out = pd.concat([df_merged, df_missing])
    # df_out=df_merged.drop(['geometry', 'index_right'], axis=1)
    # print(df_out.shape)
    # print(df_out.dtypes)
    # print(df_out.head())

    # with udaExec.connect(method="odbc", system=host, username=username, password=password) as connection:
    #     connection.execute('DELETE FROM DL_NETWORK_GEN.woonplaats_info;')
    #     connection.execute('DELETE FROM DL_NETWORK_GEN.ARDashboard_joined;')
    #     connection.execute('DELETE FROM DL_NETWORK_GEN.ARDashboard_main;')
    #     connection.execute('DELETE FROM DL_NETWORK_GEN.ARDashboard_agg;')

    # cursor.execute('DELETE FROM DL_NETWORK_GEN.woonplaats_info;')
    # cursor.execute('DELETE FROM DL_NETWORK_GEN.ARDashboard_joined;')
    # cursor.execute('DELETE FROM DL_NETWORK_GEN.ARDashboard_main;')
    # cursor.execute('DELETE FROM DL_NETWORK_GEN.ARDashboard_agg;')

    dt = datetime.now()
    # # dt = datetime.strptime('20220928', '%Y%m%d')
    month_start = dt + relativedelta(months=-12)
    month_start = month_start.strftime('%Y-%m')
    # print(month_start)

    # data = [tuple(x) for x in df_out.to_records(index=False)]

    # print(df_out.head())
    df_out = (df_out
              .assign(
                OPP = df_out.OPP.round(3),
                wp_pop_density = df_out.wp_pop_density.round(3)
               )
              )
    # df_out["OPP"]=df_out.OPP.round(3)
    # df_out["wp_pop_density"] = df_out.wp_pop_density.round(3)
    # print(df_out.head())
    # print(df_out.loc[df_out.GEMNAAM_SF == 'De Fryske Marren',])

    # df_out.to_csv('woonplaats_info.csv', index=False)

    # cursor.execute('DELETE FROM DL_NETWORK_GEN.woonplaats_info;')
    # cursor.executemany(f"""
    #         insert into DL_NETWORK_GEN.woonplaats_info ("lat", "lon", "gemnaam", "woonplaatsnaam", "WOONPLAATS_SF", "GEMNAAM_SF",
    #                                          "PROVAFK", "OPP", "Population", "wp_pop_rank", "wp_pop_density",
    #                                          "wp_pop_density_rank", "gem_type")
    #         values (?,?,?,?,?,?,?,?,?,?,?,?,?)""", df_out.values.tolist())
    #
    # cursor.execute('DELETE FROM DL_NETWORK_GEN.ARDashboard_joined;')
    # cursor.execute('''
    #                                 INSERT INTO DL_NETWORK_GEN.ARDashboard_joined
    #                                 SELECT
    #                                     mn.DATUM_INGEBRUIKNAME,
    #                                     mn.GEMNAAM,
    #                                     mn.HOOFDSOORT,
    #                                     mn.SAT_CODE,
    #                                     mn.WOONPLAATSNAAM,
    #                                     mn.postcode,
    #                                     mn.x,
    #                                     mn.y,
    #                                     mn.Hoogte,
    #                                     mn.Hoofdstraalrichting,
    #                                     mn.Frequentie,
    #                                     mn.Vermogen,
    #                                     mn."Veilige afstand",
    #                                     mn.Frequentie1,
    #                                     mn.Frequentie2,
    #                                     mn.Technology,
    #                                     mn.Operator,
    #                                     mn.Band,
    #                                     mn.Outdoor_Macro,
    #                                     mn.Load_Date,
    #                                     mn.lat,
    #                                     mn.lon,
    #                                     wp.WOONPLAATS_SF,
    #                                     wp.GEMNAAM_SF,
    #                                     wp.PROVAFK,
    #                                     wp.OPP,
    #                                     wp.Population,
    #                                     wp.wp_pop_rank,
    #                                     wp.wp_pop_density,
    #                                     wp.wp_pop_density_rank,
    #                                     wp.gem_type,
    #                                     CASE WHEN mn.Technology = '2G' THEN 1 ELSE 0 END as tag_GSM,
    #                                     CASE WHEN mn.Technology = '3G' THEN 1 ELSE 0 END as tag_UMTS,
    #                                     CASE WHEN mn.Technology = '4G' THEN 1 ELSE 0 END as tag_LTE,
    #                                     CASE WHEN mn.Technology = '5G' THEN 1 ELSE 0 END as tag_NR,
    #                                     CASE WHEN mn.Technology = 'NB-IoT' THEN 1 ELSE 0 END as tag_IoT,
    #                                     CASE WHEN mn.Band = '700' THEN 1 ELSE 0 END as tag_700MHz,
    #                                     CASE WHEN mn.Band = '800' THEN 1 ELSE 0 END as tag_800MHz,
    #                                     CASE WHEN mn.Band = '900' THEN 1 ELSE 0 END as tag_900MHz,
    #                                     CASE WHEN mn.Band = '1400' THEN 1 ELSE 0 END as tag_1400MHz,
    #                                     CASE WHEN mn.Band = '1800' THEN 1 ELSE 0 END as tag_1800MHz,
    #                                     CASE WHEN mn.Band = '2100' THEN 1 ELSE 0 END as tag_2100MHz,
    #                                     CASE WHEN mn.Band = '2600FDD' THEN 1 ELSE 0 END as tag_2600MHz_FDD,
    #                                     CASE WHEN mn.Band = '2600TDD' THEN 1 ELSE 0 END as tag_2600MHz_TDD,
    #                                     CASE WHEN mn.Band = '3500TDD' THEN 1 ELSE 0 END as tag_3500MHz_TDD,
    #                                     CASE WHEN (mn.Technology = '2G' and mn.Band = '900') THEN 1 ELSE 0 END as tag_GSM900,
    #                                     CASE WHEN (mn.Technology = '2G' and mn.Band = '1800') THEN 1 ELSE 0 END as tag_GSM1800,
    #                                     CASE WHEN (mn.Technology = '3G' and mn.Band = '900') THEN 1 ELSE 0 END as tag_UMTS900,
    #                                     CASE WHEN (mn.Technology = '3G' and mn.Band = '2100') THEN 1 ELSE 0 END as tag_UMTS2100,
    #                                     CASE WHEN (mn.Technology = '4G' and mn.Band = '700') THEN 1 ELSE 0 END as tag_LTE700,
    #                                     CASE WHEN (mn.Technology = '4G' and mn.Band = '800') THEN 1 ELSE 0 END as tag_LTE800,
    #                                     CASE WHEN (mn.Technology = '4G' and mn.Band = '900') THEN 1 ELSE 0 END as tag_LTE900,
    #                                     CASE WHEN (mn.Technology = '4G' and mn.Band = '1400') THEN 1 ELSE 0 END as tag_LTE1400,
    #                                     CASE WHEN (mn.Technology = '4G' and mn.Band = '1800') THEN 1 ELSE 0 END as tag_LTE1800,
    #                                     CASE WHEN (mn.Technology = '4G' and mn.Band = '2100') THEN 1 ELSE 0 END as tag_LTE2100,
    #                                     CASE WHEN (mn.Technology = '4G' and mn.Band = '2600FDD') THEN 1 ELSE 0 END as tag_LTE2600FDD,
    #                                     CASE WHEN (mn.Technology = '4G' and mn.Band = '2600TDD') THEN 1 ELSE 0 END as tag_LTE2600TDD,
    #                                     CASE WHEN (mn.Technology = 'NB-IoT' and mn.Band = '900') THEN 1 ELSE 0 END as tag_IoT900,
    #                                     CASE WHEN (mn.Technology = '5G' and mn.Band = '700') THEN 1 ELSE 0 END as tag_NR700,
    #                                     CASE WHEN (mn.Technology = '5G' and mn.Band = '800') THEN 1 ELSE 0 END as tag_NR800,
    #                                     CASE WHEN (mn.Technology = '5G' and mn.Band = '900') THEN 1 ELSE 0 END as tag_NR900,
    #                                     CASE WHEN (mn.Technology = '5G' and mn.Band = '1400') THEN 1 ELSE 0 END as tag_NR1400,
    #                                     CASE WHEN (mn.Technology = '5G' and mn.Band = '1800') THEN 1 ELSE 0 END as tag_NR1800,
    #                                     CASE WHEN (mn.Technology = '5G' and mn.Band = '2100') THEN 1 ELSE 0 END as tag_NR2100,
    #                                     CASE WHEN (mn.Technology = '5G' and mn.Band = '2600FDD') THEN 1 ELSE 0 END as tag_NR2600FDD,
    #                                     CASE WHEN (mn.Technology = '5G' and mn.Band = '2600TDD') THEN 1 ELSE 0 END as tag_NR2600TDD,
    #                                     CASE WHEN (mn.Technology = '5G' and mn.Band = '3500TDD') THEN 1 ELSE 0 END as tag_NR3500TDD
    #                                 FROM DL_NETWORK_GEN.ARdashboard mn
    #                                 LEFT JOIN DL_NETWORK_GEN.woonplaats_info wp ON (round(mn.lat, 13)=wp.lat AND round(mn.lon, 13)=wp.lon)
    #                                 ;'''
    #                    )
    #
    # cursor.execute('DELETE FROM DL_NETWORK_GEN.ARDashboard_main;')
    # cursor.execute('''
    #                             INSERT INTO DL_NETWORK_GEN.ARDashboard_main
    #                             SELECT
    #                                 *
    #                             FROM DL_NETWORK_GEN.ARdashboard_joined
    #                             WHERE Load_Date>'{0}'
    #                             ;
    #                             '''.format(month_start)
    #                    )
    #
    # cursor.execute('DELETE FROM DL_NETWORK_GEN.ARDashboard_agg;')
    # cursor.execute('''
    #                             INSERT INTO DL_NETWORK_GEN.ARDashboard_agg
    #                             SELECT
    #                                 a.Load_Date
    #                                 ,a.WOONPLAATS_SF
    #                                 ,a.GEMNAAM_SF
    #                                 ,a.Operator
    #                                 ,a.Outdoor_Macro
    #                                 ,a.PROVAFK
    #                                 ,a.wp_pop_rank
    #                                 ,a.wp_pop_density_rank
    #                                 ,a.gem_type
    #                                 ,a.GSM
    #                                 ,a.UMTS
    #                                 ,a.LTE
    #                                 ,a.NR
    #                                 ,a.IoT
    #                                 ,a.B_700MHz
    #                                 ,a.B_800MHz
    #                                 ,a.B_900MHz
    #                                 ,a.B_1400MHz
    #                                 ,a.B_1800MHz
    #                                 ,a.B_2100MHz
    #                                 ,a.B_2600MHz_FDD
    #                                 ,a.B_2600MHz_TDD
    #                                 ,a.B_3500MHz_TDD
    #                                 ,a.GSM900
    #                                 ,a.GSM1800
    #                                 ,a.UMTS900
    #                                 ,a.UMTS2100
    #                                 ,a.LTE700
    #                                 ,a.LTE800
    #                                 ,a.LTE900
    #                                 ,a.LTE1400
    #                                 ,a.LTE1800
    #                                 ,a.LTE2100
    #                                 ,a.LTE2600FDD
    #                                 ,a.LTE2600TDD
    #                                 ,a.IoT900
    #                                 ,a.NR700
    #                                 ,a.NR800
    #                                 ,a.NR900
    #                                 ,a.NR1400
    #                                 ,a.NR1800
    #                                 ,a.NR2100
    #                                 ,a.NR2600FDD
    #                                 ,a.NR2600TDD
    #                                 ,a.NR3500TDD
    #                                 ,count(distinct a.location_id) as location_count
    #                             FROM
    #                             (SELECT
    #                                 Load_Date as Load_Date
    #                                 ,lat||'_'||lon as location_id
    #                                 ,WOONPLAATS_SF
    #                                 ,GEMNAAM_SF
    #                                 ,Operator
    #                                 ,Outdoor_Macro
    #                                 ,PROVAFK
    #                                 ,wp_pop_rank
    #                                 ,wp_pop_density_rank
    #                                 ,gem_type
    #                                 ,lat as latitude
    #                                 ,lon as longitude
    #                                 ,case when (sum(tag_GSM)>0) then 1 else 0 end as GSM
    #                                 ,case when (sum(tag_UMTS)>0) then 1 else 0 end as UMTS
    #                                 ,case when (sum(tag_LTE)>0) then 1 else 0 end as LTE
    #                                 ,case when (sum(tag_NR)>0) then 1 else 0 end as NR
    #                                 ,case when (sum(tag_IoT)>0) then 1 else 0 end as IoT
    #                                 ,case when (sum(tag_700MHz)>0) then 1 else 0 end as B_700MHz
    #                                 ,case when (sum(tag_800MHz)>0) then 1 else 0 end as B_800MHz
    #                                 ,case when (sum(tag_900MHz)>0) then 1 else 0 end as B_900MHz
    #                                 ,case when (sum(tag_1400MHz)>0) then 1 else 0 end as B_1400MHz
    #                                 ,case when (sum(tag_1800MHz)>0) then 1 else 0 end as B_1800MHz
    #                                 ,case when (sum(tag_2100MHz)>0) then 1 else 0 end as B_2100MHz
    #                                 ,case when (sum(tag_2600MHz_FDD)>0) then 1 else 0 end as B_2600MHz_FDD
    #                                 ,case when (sum(tag_2600MHz_TDD)>0) then 1 else 0 end as B_2600MHz_TDD
    #                                 ,case when (sum(tag_3500MHz_TDD)>0) then 1 else 0 end as B_3500MHz_TDD
    #                                 ,case when (sum(tag_GSM900)>0) then 1 else 0 end as GSM900
    #                                 ,case when (sum(tag_GSM1800)>0) then 1 else 0 end as GSM1800
    #                                 ,case when (sum(tag_UMTS900)>0) then 1 else 0 end as UMTS900
    #                                 ,case when (sum(tag_UMTS2100)>0) then 1 else 0 end as UMTS2100
    #                                 ,case when (sum(tag_LTE700)>0) then 1 else 0 end as LTE700
    #                                 ,case when (sum(tag_LTE800)>0) then 1 else 0 end as LTE800
    #                                 ,case when (sum(tag_LTE900)>0) then 1 else 0 end  as LTE900
    #                                 ,case when (sum(tag_LTE1400)>0) then 1 else 0 end  as LTE1400
    #                                 ,case when (sum(tag_LTE1800)>0) then 1 else 0 end  as LTE1800
    #                                 ,case when (sum(tag_LTE2100)>0) then 1 else 0 end  as LTE2100
    #                                 ,case when (sum(tag_LTE2600FDD)>0) then 1 else 0 end  as LTE2600FDD
    #                                 ,case when (sum(tag_LTE2600TDD)>0) then 1 else 0 end  as LTE2600TDD
    #                                 ,case when (sum(tag_IoT900)>0) then 1 else 0 end  as IoT900
    #                                 ,case when (sum(tag_NR700)>0) then 1 else 0 end  as NR700
    #                                 ,case when (sum(tag_NR800)>0) then 1 else 0 end  as NR800
    #                                 ,case when (sum(tag_NR900)>0) then 1 else 0 end  as NR900
    #                                 ,case when (sum(tag_NR1400)>0) then 1 else 0 end  as NR1400
    #                                 ,case when (sum(tag_NR1800)>0) then 1 else 0 end  as NR1800
    #                                 ,case when (sum(tag_NR2100)>0) then 1 else 0 end  as NR2100
    #                                 ,case when (sum(tag_NR2600FDD)>0) then 1 else 0 end  as NR2600FDD
    #                                 ,case when (sum(tag_NR2600TDD)>0) then 1 else 0 end  as NR2600TDD
    #                                 ,case when (sum(tag_NR3500TDD)>0) then 1 else 0 end  as NR3500TDD
    #                             FROM DL_NETWORK_GEN.ARDashboard_joined
    #                             GROUP BY
    #                                 Load_Date
    #                                 ,lat||'_'||lon
    #                                 ,WOONPLAATS_SF
    #                                 ,GEMNAAM_SF
    #                                 ,Operator
    #                                 ,Outdoor_Macro
    #                                 ,PROVAFK
    #                                 ,wp_pop_rank
    #                                 ,wp_pop_density_rank
    #                                 ,gem_type
    #                                 ,lat
    #                                 ,lon) as a
    #                             GROUP BY
    #                                 a.Load_Date
    #                                 ,a.WOONPLAATS_SF
    #                                 ,a.GEMNAAM_SF
    #                                 ,a.Operator
    #                                 ,a.Outdoor_Macro
    #                                 ,a.PROVAFK
    #                                 ,a.wp_pop_rank
    #                                 ,a.wp_pop_density_rank
    #                                 ,a.gem_type
    #                                 ,a.GSM
    #                                 ,a.UMTS
    #                                 ,a.LTE
    #                                 ,a.NR
    #                                 ,a.IoT
    #                                 ,a.B_700MHz
    #                                 ,a.B_800MHz
    #                                 ,a.B_900MHz
    #                                 ,a.B_1400MHz
    #                                 ,a.B_1800MHz
    #                                 ,a.B_2100MHz
    #                                 ,a.B_2600MHz_FDD
    #                                 ,a.B_2600MHz_TDD
    #                                 ,a.B_3500MHz_TDD
    #                                 ,a.GSM900
    #                                 ,a.GSM1800
    #                                 ,a.UMTS900
    #                                 ,a.UMTS2100
    #                                 ,a.LTE700
    #                                 ,a.LTE800
    #                                 ,a.LTE900
    #                                 ,a.LTE1400
    #                                 ,a.LTE1800
    #                                 ,a.LTE2100
    #                                 ,a.LTE2600FDD
    #                                 ,a.LTE2600TDD
    #                                 ,a.IoT900
    #                                 ,a.NR700
    #                                 ,a.NR800
    #                                 ,a.NR900
    #                                 ,a.NR1400
    #                                 ,a.NR1800
    #                                 ,a.NR2100
    #                                 ,a.NR2600FDD
    #                                 ,a.NR2600TDD
    #                                 ,a.NR3500TDD
    #                             ;
    #                             '''
    #                    )
    #
    # cursor.close()
    # conn.close()
    #
    #
    udaExec = teradata.UdaExec(appName="test", version="1.0", logConsole=False)
    with udaExec.connect(method="odbc", system=host, username=username, password=password, charset='UTF8') as connection:
        connection.execute('DELETE FROM DL_NETWORK_GEN.woonplaats_info;')
        connection.executemany('INSERT INTO DL_NETWORK_GEN.woonplaats_info values(?,?,?,?,?,?,?,?,?,?,?,?,?);'
                               , df_out.values.tolist()
                               , batch=True
                               )

        connection.execute('DELETE FROM DL_NETWORK_GEN.ARDashboard_joined;')
        connection.execute('''
                                INSERT INTO DL_NETWORK_GEN.ARDashboard_joined
                                SELECT
                                    mn.DATUM_INGEBRUIKNAME,
                                    mn.GEMNAAM,
                                    mn.HOOFDSOORT,
                                    mn.SAT_CODE,
                                    mn.WOONPLAATSNAAM,
                                    mn.postcode,
                                    mn.x,
                                    mn.y,
                                    mn.Hoogte,
                                    mn.Hoofdstraalrichting,
                                    mn.Frequentie,
                                    mn.Vermogen,
                                    mn."Veilige afstand",
                                    mn.Frequentie1,
                                    mn.Frequentie2,
                                    mn.Technology,
                                    mn.Operator,
                                    mn.Band,
                                    mn.Outdoor_Macro,
                                    mn.Load_Date,
                                    mn.lat,
                                    mn.lon,
                                    wp.WOONPLAATS_SF,
                                    wp.GEMNAAM_SF,
                                    wp.PROVAFK,
                                    wp.OPP,
                                    wp.Population,
                                    wp.wp_pop_rank,
                                    wp.wp_pop_density,
                                    wp.wp_pop_density_rank,
                                    wp.gem_type,
                                    CASE WHEN mn.Technology = '2G' THEN 1 ELSE 0 END as tag_GSM,
                                    CASE WHEN mn.Technology = '3G' THEN 1 ELSE 0 END as tag_UMTS,
                                    CASE WHEN mn.Technology = '4G' THEN 1 ELSE 0 END as tag_LTE,
                                    CASE WHEN mn.Technology = '5G' THEN 1 ELSE 0 END as tag_NR,
                                    CASE WHEN mn.Technology = 'NB-IoT' THEN 1 ELSE 0 END as tag_IoT,
                                    CASE WHEN mn.Band = '700' THEN 1 ELSE 0 END as tag_700MHz,
                                    CASE WHEN mn.Band = '800' THEN 1 ELSE 0 END as tag_800MHz,
                                    CASE WHEN mn.Band = '900' THEN 1 ELSE 0 END as tag_900MHz,
                                    CASE WHEN mn.Band = '1400' THEN 1 ELSE 0 END as tag_1400MHz,
                                    CASE WHEN mn.Band = '1800' THEN 1 ELSE 0 END as tag_1800MHz,
                                    CASE WHEN mn.Band = '2100' THEN 1 ELSE 0 END as tag_2100MHz,
                                    CASE WHEN mn.Band = '2600FDD' THEN 1 ELSE 0 END as tag_2600MHz_FDD,
                                    CASE WHEN mn.Band = '2600TDD' THEN 1 ELSE 0 END as tag_2600MHz_TDD,
                                    CASE WHEN mn.Band = '3500' THEN 1 ELSE 0 END as tag_3500MHz_TDD,
                                    CASE WHEN (mn.Technology = '2G' and mn.Band = '900') THEN 1 ELSE 0 END as tag_GSM900,
                                    CASE WHEN (mn.Technology = '2G' and mn.Band = '1800') THEN 1 ELSE 0 END as tag_GSM1800,
                                    CASE WHEN (mn.Technology = '3G' and mn.Band = '900') THEN 1 ELSE 0 END as tag_UMTS900,
                                    CASE WHEN (mn.Technology = '3G' and mn.Band = '2100') THEN 1 ELSE 0 END as tag_UMTS2100,
                                    CASE WHEN (mn.Technology = '4G' and mn.Band = '700') THEN 1 ELSE 0 END as tag_LTE700,
                                    CASE WHEN (mn.Technology = '4G' and mn.Band = '800') THEN 1 ELSE 0 END as tag_LTE800,
                                    CASE WHEN (mn.Technology = '4G' and mn.Band = '900') THEN 1 ELSE 0 END as tag_LTE900,
                                    CASE WHEN (mn.Technology = '4G' and mn.Band = '1400') THEN 1 ELSE 0 END as tag_LTE1400,
                                    CASE WHEN (mn.Technology = '4G' and mn.Band = '1800') THEN 1 ELSE 0 END as tag_LTE1800,
                                    CASE WHEN (mn.Technology = '4G' and mn.Band = '2100') THEN 1 ELSE 0 END as tag_LTE2100,
                                    CASE WHEN (mn.Technology = '4G' and mn.Band = '2600FDD') THEN 1 ELSE 0 END as tag_LTE2600FDD,
                                    CASE WHEN (mn.Technology = '4G' and mn.Band = '2600TDD') THEN 1 ELSE 0 END as tag_LTE2600TDD,
                                    CASE WHEN (mn.Technology = 'NB-IoT' and mn.Band = '900') THEN 1 ELSE 0 END as tag_IoT900,
                                    CASE WHEN (mn.Technology = '5G' and mn.Band = '700') THEN 1 ELSE 0 END as tag_NR700,
                                    CASE WHEN (mn.Technology = '5G' and mn.Band = '800') THEN 1 ELSE 0 END as tag_NR800,
                                    CASE WHEN (mn.Technology = '5G' and mn.Band = '900') THEN 1 ELSE 0 END as tag_NR900,
                                    CASE WHEN (mn.Technology = '5G' and mn.Band = '1400') THEN 1 ELSE 0 END as tag_NR1400,
                                    CASE WHEN (mn.Technology = '5G' and mn.Band = '1800') THEN 1 ELSE 0 END as tag_NR1800,
                                    CASE WHEN (mn.Technology = '5G' and mn.Band = '2100') THEN 1 ELSE 0 END as tag_NR2100,
                                    CASE WHEN (mn.Technology = '5G' and mn.Band = '2600FDD') THEN 1 ELSE 0 END as tag_NR2600FDD,
                                    CASE WHEN (mn.Technology = '5G' and mn.Band = '2600TDD') THEN 1 ELSE 0 END as tag_NR2600TDD,
                                    CASE WHEN (mn.Technology = '5G' and mn.Band = '3500') THEN 1 ELSE 0 END as tag_NR3500TDD,
                                    mn.SMALL_CELL_INDICATOR
                                FROM DL_NETWORK_GEN.ARdashboard mn
                                LEFT JOIN DL_NETWORK_GEN.woonplaats_info wp ON (round(mn.lat, 13)=wp.lat AND round(mn.lon, 13)=wp.lon)
                                ;'''
                           )

        connection.execute('''
                                update DL_NETWORK_GEN.ARdashboard_joined
                                set Operator = 'Tampnet'
                                where 1=1
                                and gem_type = 'Unknown'
                                and Operator not in  ('Odido', 'T-Mobile')
                                and tag_1400MHz=0
                                and tag_2100MHz=0
                                and tag_NR = 0
                                ;'''
                           )

        connection.execute('DELETE FROM DL_NETWORK_GEN.ARDashboard_main;')
        connection.execute('''
                            INSERT INTO DL_NETWORK_GEN.ARDashboard_main
                            SELECT
                                *
                            FROM DL_NETWORK_GEN.ARdashboard_joined
                            WHERE Load_Date>='{0}'
                            ;
                            '''.format(month_start)
                           )

        connection.execute('DELETE FROM DL_NETWORK_GEN.ARDashboard_agg;')
        connection.execute('''
                            INSERT INTO DL_NETWORK_GEN.ARDashboard_agg
                            SELECT
                                a.Load_Date
                                ,a.WOONPLAATS_SF
                                ,a.GEMNAAM_SF
                                ,a.Operator
                                ,a.Outdoor_Macro
                                ,a.PROVAFK
                                ,a.wp_pop_rank
                                ,a.wp_pop_density_rank
                                ,a.gem_type
                                ,a.GSM
                                ,a.UMTS
                                ,a.LTE
                                ,a.NR
                                ,a.IoT
                                ,a.B_700MHz
                                ,a.B_800MHz
                                ,a.B_900MHz
                                ,a.B_1400MHz
                                ,a.B_1800MHz
                                ,a.B_2100MHz
                                ,a.B_2600MHz_FDD
                                ,a.B_2600MHz_TDD
                                ,a.B_3500MHz_TDD
                                ,a.GSM900
                                ,a.GSM1800
                                ,a.UMTS900
                                ,a.UMTS2100
                                ,a.LTE700
                                ,a.LTE800
                                ,a.LTE900
                                ,a.LTE1400
                                ,a.LTE1800
                                ,a.LTE2100
                                ,a.LTE2600FDD
                                ,a.LTE2600TDD
                                ,a.IoT900
                                ,a.NR700
                                ,a.NR800
                                ,a.NR900
                                ,a.NR1400
                                ,a.NR1800
                                ,a.NR2100
                                ,a.NR2600FDD
                                ,a.NR2600TDD
                                ,a.NR3500TDD
                                ,count(distinct a.location_id) as location_count
                                ,a.SMALL_CELL_INDICATOR
                            FROM
                            (SELECT
                                Load_Date as Load_Date
                                ,lat||'_'||lon as location_id
                                ,WOONPLAATS_SF
                                ,GEMNAAM_SF
                                ,Operator
                                ,Outdoor_Macro
                                ,PROVAFK
                                ,wp_pop_rank
                                ,wp_pop_density_rank
                                ,gem_type
                                ,lat as latitude
                                ,lon as longitude
                                ,SMALL_CELL_INDICATOR
                                ,case when (sum(tag_GSM)>0) then 1 else 0 end as GSM
                                ,case when (sum(tag_UMTS)>0) then 1 else 0 end as UMTS
                                ,case when (sum(tag_LTE)>0) then 1 else 0 end as LTE
                                ,case when (sum(tag_NR)>0) then 1 else 0 end as NR
                                ,case when (sum(tag_IoT)>0) then 1 else 0 end as IoT
                                ,case when (sum(tag_700MHz)>0) then 1 else 0 end as B_700MHz
                                ,case when (sum(tag_800MHz)>0) then 1 else 0 end as B_800MHz
                                ,case when (sum(tag_900MHz)>0) then 1 else 0 end as B_900MHz
                                ,case when (sum(tag_1400MHz)>0) then 1 else 0 end as B_1400MHz
                                ,case when (sum(tag_1800MHz)>0) then 1 else 0 end as B_1800MHz
                                ,case when (sum(tag_2100MHz)>0) then 1 else 0 end as B_2100MHz
                                ,case when (sum(tag_2600MHz_FDD)>0) then 1 else 0 end as B_2600MHz_FDD
                                ,case when (sum(tag_2600MHz_TDD)>0) then 1 else 0 end as B_2600MHz_TDD
                                ,case when (sum(tag_3500MHz_TDD)>0) then 1 else 0 end as B_3500MHz_TDD
                                ,case when (sum(tag_GSM900)>0) then 1 else 0 end as GSM900
                                ,case when (sum(tag_GSM1800)>0) then 1 else 0 end as GSM1800
                                ,case when (sum(tag_UMTS900)>0) then 1 else 0 end as UMTS900
                                ,case when (sum(tag_UMTS2100)>0) then 1 else 0 end as UMTS2100
                                ,case when (sum(tag_LTE700)>0) then 1 else 0 end as LTE700
                                ,case when (sum(tag_LTE800)>0) then 1 else 0 end as LTE800
                                ,case when (sum(tag_LTE900)>0) then 1 else 0 end  as LTE900
                                ,case when (sum(tag_LTE1400)>0) then 1 else 0 end  as LTE1400
                                ,case when (sum(tag_LTE1800)>0) then 1 else 0 end  as LTE1800
                                ,case when (sum(tag_LTE2100)>0) then 1 else 0 end  as LTE2100
                                ,case when (sum(tag_LTE2600FDD)>0) then 1 else 0 end  as LTE2600FDD
                                ,case when (sum(tag_LTE2600TDD)>0) then 1 else 0 end  as LTE2600TDD
                                ,case when (sum(tag_IoT900)>0) then 1 else 0 end  as IoT900
                                ,case when (sum(tag_NR700)>0) then 1 else 0 end  as NR700
                                ,case when (sum(tag_NR800)>0) then 1 else 0 end  as NR800
                                ,case when (sum(tag_NR900)>0) then 1 else 0 end  as NR900
                                ,case when (sum(tag_NR1400)>0) then 1 else 0 end  as NR1400
                                ,case when (sum(tag_NR1800)>0) then 1 else 0 end  as NR1800
                                ,case when (sum(tag_NR2100)>0) then 1 else 0 end  as NR2100
                                ,case when (sum(tag_NR2600FDD)>0) then 1 else 0 end  as NR2600FDD
                                ,case when (sum(tag_NR2600TDD)>0) then 1 else 0 end  as NR2600TDD
                                ,case when (sum(tag_NR3500TDD)>0) then 1 else 0 end  as NR3500TDD
                            FROM DL_NETWORK_GEN.ARDashboard_joined
                            GROUP BY
                                Load_Date
                                ,lat||'_'||lon
                                ,WOONPLAATS_SF
                                ,GEMNAAM_SF
                                ,Operator
                                ,Outdoor_Macro
                                ,PROVAFK
                                ,wp_pop_rank
                                ,wp_pop_density_rank
                                ,gem_type
                                ,lat
                                ,lon
                                ,SMALL_CELL_INDICATOR) as a
                            GROUP BY
                                a.Load_Date
                                ,a.WOONPLAATS_SF
                                ,a.GEMNAAM_SF
                                ,a.Operator
                                ,a.Outdoor_Macro
                                ,a.PROVAFK
                                ,a.wp_pop_rank
                                ,a.wp_pop_density_rank
                                ,a.gem_type
                                ,a.GSM
                                ,a.UMTS
                                ,a.LTE
                                ,a.NR
                                ,a.IoT
                                ,a.B_700MHz
                                ,a.B_800MHz
                                ,a.B_900MHz
                                ,a.B_1400MHz
                                ,a.B_1800MHz
                                ,a.B_2100MHz
                                ,a.B_2600MHz_FDD
                                ,a.B_2600MHz_TDD
                                ,a.B_3500MHz_TDD
                                ,a.GSM900
                                ,a.GSM1800
                                ,a.UMTS900
                                ,a.UMTS2100
                                ,a.LTE700
                                ,a.LTE800
                                ,a.LTE900
                                ,a.LTE1400
                                ,a.LTE1800
                                ,a.LTE2100
                                ,a.LTE2600FDD
                                ,a.LTE2600TDD
                                ,a.IoT900
                                ,a.NR700
                                ,a.NR800
                                ,a.NR900
                                ,a.NR1400
                                ,a.NR1800
                                ,a.NR2100
                                ,a.NR2600FDD
                                ,a.NR2600TDD
                                ,a.NR3500TDD
                                ,a.SMALL_CELL_INDICATOR
                            ;
                            '''
                           )


if __name__ == '__main__':
    main()