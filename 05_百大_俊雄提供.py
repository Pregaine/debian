
# coding: utf-8

# In[1]:

from datetime import datetime, timedelta
import glob
import time
import numpy as np
import talib

# from plotly import __version__
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot

import plotly as py
import cufflinks as cf
import pandas as pd
import plotly.tools as tls
import plotly.graph_objs as go

import plotly.figure_factory as ff
init_notebook_mode(connected=True)

import pyodbc
from sqlalchemy import create_engine


#  todo driver 修正
def conn():
    return pyodbc.connect(
        'DRIVER={ODBC Driver 13 for SQL Server};' + 'SERVER=localhost;' +
        'PORT=1443;' + 'DATABASE=StockDB;' + 'UID=sa;' + 'PWD=admin;')


# Create the sqlalchemy engine using the pyodbc connection
engine = create_engine(
    "mssql+pyodbc://?driver=ODBC+Driver+13?charset=utf8", creator=conn)
con = engine.connect()
con.execute("SET LANGUAGE us_english; set dateformat ymd;")
con.close()


# In[2]:

chip_path = r'C:\workspace\stock\01_Day process\券商分點\籌碼集中暫存.csv'

stock_list_path = r'C:\workspace\stock\goodinfo\StockList_股本.csv'


# In[3]:

tech_d_cmd = """SELECT
      TOP( 20 )
      [date],
      [close_price],
      [high_price],
      [volume],
      [ma120],
      [ma60],
      [ma20],
      [ma10],
      [ma5],
      [ma3],
      [W20]
      FROM [dbo].[TECH_D] WHERE stock = \'{}\' ORDER BY date DESC"""


revenue_cmd = """SELECT
      TOP( 6 )
      [date],
      [Month_Revenue],
      [Last_Year_Ration],
      [ration]
  FROM [dbo].[REVENUE] WHERE stock = \'{}\' ORDER BY date DESC"""

tdcc_cmd = """SELECT 
    TOP( 10 )
    [stock]
    ,[date]
    ,[Share_Rating_Proportion_1_999]
    ,[Share_Rating_Proportion_1000_5000]
    ,[Share_Rating_Proportion_5001_10000]
    ,[Share_Rating_Proportion_10001_15000]
    ,[Share_Rating_Proportion_15001_20000]
    ,[Share_Rating_Proportion_20001_30000]
    ,[Share_Rating_Proportion_30001_40000]
    ,[Share_Rating_Proportion_40001_50000]
    ,[Share_Rating_Proportion_50001_100000]
    ,[Share_Rating_Proportion_100001_200000]
    ,[Share_Rating_Proportion_200001_400000]
    ,[Share_Rating_Proportion_400001_600000]
    ,[Share_Rating_Proportion_600001_800000]
    ,[Share_Rating_Proportion_800001_1000000]
    ,[Share_Rating_Proportion_Up_1000001]
    ,[Share_Rating_People_1_999]
    ,[Share_Rating_People_1000_5000]
    ,[Share_Rating_People_5001_10000]
    ,[Share_Rating_People_10001_15000]
    ,[Share_Rating_People_15001_20000]
    ,[Share_Rating_People_20001_30000]
    ,[Share_Rating_People_30001_40000]
    ,[Share_Rating_People_40001_50000]
    ,[Share_Rating_People_50001_100000]
    ,[Share_Rating_People_100001_200000]
    ,[Share_Rating_People_200001_400000]
    ,[Share_Rating_People_400001_600000]
    ,[Share_Rating_People_600001_800000]
    ,[Share_Rating_People_800001_1000000]
    ,[Share_Rating_People_Up_1000001]
FROM [dbo].[TDCC]
WHERE stock = \'{}\' ORDER BY date DESC"""

# csv_df = csv_df[0:60]
# csv_df = csv_df.set_index('日期')

df = pd.read_csv(
    stock_list_path,
    lineterminator='\n',
    encoding='utf8',
    sep=',',
    index_col=False,
    na_filter=False,
    thousands=',')

df['代號'] = df['代號'].str.strip('="')
df['stock'] = df['代號'].astype(str)
del df['代號']

# df['股本(億)'] = df['股本(億)\r']
# del df['股本(億)\r']

df.sort_values(by=['股本(億)'], ascending=False, inplace=True)
df.reset_index(inplace=True)

del df['index']
df = df[ df['股本(億)'] >= 10 ]


# In[4]:

csv_df = pd.read_csv(
    chip_path,
    sep=',',
    encoding='utf8',
    false_values='NA',
    dtype={
        '股號': str,
        '日期': str
    },
)

csv_df = csv_df[[
    '股號', '日期', '收盤', '01天集中%', '03天集中%', '05天集中%', '10天集中%', '20天集中%',
    '60天集中%', '20天佔股本比重', '60天佔股本比重', '01天家數差'
]]

tmp_df = pd.DataFrame( columns=df.columns )

for query_num in df['stock']:
# for query_num in ['1465']:
    
    tech_d_df = pd.read_sql_query(tech_d_cmd.format(query_num), engine)

    if tech_d_df.empty:
        continue
            
    if tech_d_df.loc[ 0, 'ma60' ] is None:
        continue
        
    if tech_d_df.loc[ 0, 'ma120' ] is None:
        continue
        
    if tech_d_df.loc[ 0, 'close_price' ] < 10:
        continue
                           
    fail_cnt = 0
    
    # for index in [ 4, 3, 2, 1, 0 ]:
    for index in [ 7, 6, 5, 4, 3 ]:
        if tech_d_df.loc[ index, 'close_price' ] > ( tech_d_df.loc[ index, 'ma5' ] * 1.05 ):
            continue

        if tech_d_df.loc[ index, 'close_price' ] < ( tech_d_df.loc[ index, 'ma5' ] * 0.93 ):
            continue

        if tech_d_df.loc[ index, 'close_price' ] > ( tech_d_df.loc[ index, 'ma10' ] * 1.05 ):
            continue      

        if tech_d_df.loc[ index, 'close_price' ] < ( tech_d_df.loc[ index, 'ma10' ] * 0.93 ):
            continue
            
        fail_cnt += 1
            
    if fail_cnt != 5:
        continue
    
    C = np.array( tech_d_df['volume'], dtype=float, ndmin=1 )
    tech_d_df['MA10'] = talib.SMA( C, 10 )
    tech_d_df['MA10'] = tech_d_df['MA10'].shift( ( -1 * 10 ) + 1 )
    
    if tech_d_df.loc[ 0, 'MA10' ] < 500:
        continue
    
    compare_df = csv_df[csv_df['股號'] == query_num]

    compare_df = compare_df[0:20]

    compare_df.reset_index(inplace=True)

    if compare_df.empty:
        continue
        
#     if compare_df.loc[ 0, '05天集中%' ] < -5:
#         continue
   
#     if compare_df.loc[ 0, '01天家數差' ] > 25:
#         continue
                                                   
    tdcc_df = pd.read_sql_query(tdcc_cmd.format(query_num), engine)

    if tdcc_df.empty:
        continue
                
    tdcc_df[ '總股東人數' ] = tdcc_df[ 'Share_Rating_People_1_999' ] +      tdcc_df[ 'Share_Rating_People_1000_5000' ] +     tdcc_df[ 'Share_Rating_People_5001_10000' ] +     tdcc_df[ 'Share_Rating_People_10001_15000' ] +     tdcc_df[ 'Share_Rating_People_15001_20000' ] +     tdcc_df[ 'Share_Rating_People_20001_30000' ] +     tdcc_df[ 'Share_Rating_People_30001_40000' ] +     tdcc_df[ 'Share_Rating_People_40001_50000' ] +     tdcc_df[ 'Share_Rating_People_50001_100000' ] +     tdcc_df[ 'Share_Rating_People_100001_200000' ] +     tdcc_df[ 'Share_Rating_People_200001_400000' ] +     tdcc_df[ 'Share_Rating_People_400001_600000' ] +     tdcc_df[ 'Share_Rating_People_600001_800000' ] +     tdcc_df[ 'Share_Rating_People_800001_1000000' ] +     tdcc_df[ 'Share_Rating_People_Up_1000001' ] 
         
    tdcc_df[ '散戶持股比例10張以下' ] = tdcc_df[ 'Share_Rating_Proportion_1_999' ] +         tdcc_df['Share_Rating_Proportion_1000_5000'] +         tdcc_df['Share_Rating_Proportion_5001_10000']
    
    tdcc_df[ '散戶持股比例100張以下' ] = tdcc_df[ '散戶持股比例10張以下' ] +         tdcc_df['Share_Rating_Proportion_10001_15000'] +        tdcc_df['Share_Rating_Proportion_15001_20000'] +        tdcc_df['Share_Rating_Proportion_20001_30000'] +        tdcc_df['Share_Rating_Proportion_30001_40000'] +        tdcc_df['Share_Rating_Proportion_40001_50000'] +        tdcc_df['Share_Rating_Proportion_50001_100000']
    
    tdcc_df['大戶持股比例400張以上'] = tdcc_df['Share_Rating_Proportion_400001_600000'] +     tdcc_df['Share_Rating_Proportion_600001_800000']  +     tdcc_df['Share_Rating_Proportion_800001_1000000'] +     tdcc_df['Share_Rating_Proportion_Up_1000001']

    tdcc_df['大戶持股比例600張以上'] = tdcc_df['大戶持股比例400張以上'] -                                       tdcc_df['Share_Rating_Proportion_400001_600000']

    tdcc_df['大戶持股比例800張以上'] = tdcc_df['大戶持股比例600張以上'] -                                      tdcc_df['Share_Rating_Proportion_600001_800000']

    tdcc_df['大戶持股比例1000張以上'] = tdcc_df['大戶持股比例800張以上'] -                                        tdcc_df['Share_Rating_Proportion_800001_1000000']

    tdcc_df['date'] = pd.to_datetime(tdcc_df['date'])
    
    # if tdcc_df.loc[ 0, '總股東人數' ] > tdcc_df.loc[ 1, '總股東人數' ]:
    # continue 
        
    # if tdcc_df.loc[ 0, '散戶持股比例100張以下' ] > tdcc_df.loc[ 1, '散戶持股比例100張以下' ]:
    # continue

    # if tdcc_df.loc[ 0, '大戶持股比例1000張以上'] > 75:
    # continue

    if ( tdcc_df.loc[ 0, '大戶持股比例400張以上'] - tdcc_df.loc[ 1, '大戶持股比例400張以上'] ) < 0.4:
        continue 
                
    tmp_df = tmp_df.append( df[ df[ 'stock'] == query_num ], ignore_index=True )
    
    tmp_df.loc[ tmp_df['stock'] == query_num, '現價'] = tech_d_df.loc[ 0, 'close_price' ]
    
    tmp_df.loc[ tmp_df['stock'] == query_num, '持股10張以下減少'] = round( tdcc_df.loc[ 1, '散戶持股比例10張以下'] - tdcc_df.loc[ 0, '散戶持股比例10張以下'], 2 )
    
    tmp_df.loc[ tmp_df['stock'] == query_num, '布寬' ] = tech_d_df.loc[0, 'W20'] * 100
    
    tmp_df.loc[ tmp_df['stock'] == query_num, '持股400張以上增加' ] = round( tdcc_df.loc[ 0, '大戶持股比例400張以上' ] - tdcc_df.loc[ 1, '大戶持股比例400張以上' ], 2 ) 
    
    tmp_df.loc[ tmp_df['stock'] == query_num, '持股100張以下減少' ] = round( tdcc_df.loc[ 1, '散戶持股比例100張以下' ] - tdcc_df.loc[ 0, '散戶持股比例100張以下' ], 2 )
    
    tmp_df.loc[ tmp_df['stock'] == query_num, '股東減少人數比例' ] = round( ( tdcc_df.loc[ 1, '總股東人數' ] - tdcc_df.loc[ 0, '總股東人數' ] ) / tdcc_df.loc[ 0, '總股東人數' ] * 100, 2 )
    
    tmp_df.loc[ tmp_df['stock'] == query_num, '籌碼集中值' ] = compare_df[ '01天集中%' ].max()
    
    max_day = compare_df.loc[ compare_df[ '01天集中%' ] == compare_df[ '01天集中%' ].max(), '日期' ]
    
    clos_price = compare_df.loc[ compare_df[ '01天集中%' ] == compare_df[ '01天集中%' ].max(), '收盤' ]
          
    tmp_df.loc[ tmp_df['stock'] == query_num, '籌碼集中日' ] = max_day.values[ 0 ][ 5:]  
    
    tmp_df.loc[ tmp_df['stock'] == query_num, '籌碼集中收盤' ] = clos_price.values[ 0 ]
        
    date = tech_d_df.loc[ 0, 'date' ]
    
    # print( tech_d_df )
    
if tmp_df.empty is True:
    print( '無資料' )
    pause()

tmp_df.sort_values( [ '布寬', '籌碼集中值', '股東減少人數比例', '持股400張以上增加', '持股100張以下減少' ], inplace=True, ascending=[ True, False, False, False, False ] )    
tmp_df = tmp_df.reset_index( )     
tmp_df = tmp_df[ ['stock', '名稱', '股本(億)', '現價', '籌碼集中收盤', '籌碼集中值', '籌碼集中日', '布寬', '股東減少人數比例', '持股400張以上增加', '持股100張以下減少', '持股10張以下減少', '產業別', '相關概念' ] ]

# tmp_df


# In[5]:

investors_date_cmd = """SELECT TOP( 20 )
      [date]
  FROM [dbo].[INVESTORS]
  WHERE stock = '2330' ORDER BY date DESC """

investors_date_df = pd.read_sql_query( investors_date_cmd, engine )

investors_date_df[ 'date' ]


# In[6]:

investors_cmd = """SELECT
      [stock]
      ,[date]
      ,[foreign_sell]
      ,[investment_sell]
      ,[dealer_sell]
  FROM [dbo].[INVESTORS]
  WHERE date = \'{}\' """

foreign_list = list()
investment_list = list()
dealer_list = list()

for date in investors_date_df[ 'date' ]:
    
    investors_df = pd.read_sql_query( investors_cmd.format( date ), engine )
    
    foreign_df = investors_df.sort_values( by='foreign_sell', ascending = False )

    investment_df = investors_df.sort_values( by='investment_sell', ascending = False )
    
    dealer_df = investors_df.sort_values( by='dealer_sell', ascending = False )
    
    foreign_list.extend( foreign_df[ 'stock' ].head( 50 ).tolist() )
  
    investment_list.extend( investment_df[ 'stock' ].head( 50 ).tolist() )
    
    dealer_list.extend( dealer_df[ 'stock' ].head( 50 ).tolist() )
   


# In[7]:

compare_df = tmp_df.copy()

for stock in tmp_df[ 'stock' ]:
    
    compare_df.loc[ tmp_df['stock'] == stock, '外資買天數' ] = None
    compare_df.loc[ tmp_df['stock'] == stock, '投信買天數' ] = None
    compare_df.loc[ tmp_df['stock'] == stock, '自營買天數' ] = None
        
    if stock in foreign_list: 
        compare_df.loc[ tmp_df['stock'] == stock, '外資買天數' ] = foreign_list.count( stock )
        continue
        
    if stock in investment_list:
        compare_df.loc[ tmp_df['stock'] == stock, '投信買天數' ] = investment_list.count( stock )
        continue
        
    if stock in dealer_list:
        compare_df.loc[ tmp_df['stock'] == stock, '自營買天數' ] = dealer_list.count( stock )
        continue
        
    compare_df.drop( compare_df[ compare_df[ 'stock' ] == stock ].index, inplace=True )
   

compare_df.sort_values( [ '股東減少人數比例', '外資買天數', '布寬', '籌碼集中值', '持股400張以上增加' ], inplace=True, ascending=[ False, False, True, False, False ] )    
compare_df = compare_df.reset_index( )     
compare_df = compare_df[ ['stock', '名稱', '股本(億)', '現價', '籌碼集中收盤', '籌碼集中值', '籌碼集中日', '布寬', '外資買天數', '投信買天數', '自營買天數', '股東減少人數比例', '持股400張以上增加', '持股100張以下減少', '持股10張以下減少', '產業別', '相關概念' ] ]


compare_df


# In[8]:

date_str = investors_date_df.loc[ 0, 'date' ].strftime('%Y%m%d')

compare_df.to_html( r'.\10天內百大法人\10天內百大法人_{}.html'.format( date_str ) )

# df_writer = pd.ExcelWriter( '10天內百大法人_{}.xlsx'.format( date_str ) )
# compare_df.to_excel( df_writer, sheet_name = '10天內百大法人_{}'.format( date_str ) )

