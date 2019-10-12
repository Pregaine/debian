# -*- coding: utf-8 -*-

from datetime import datetime
import os
import os.path
import re
import sys
import pandas as pd
import numpy as np
import csv
import codes.codes as TWSE
import decimal
from collections import namedtuple
import goodinfo.capital as goodinfo
from sqlalchemy import create_engine
import mysql.connector

# Todo
# 建立 Table
# date, 股號, top 15 買均價 top 15賣均價 top 15 買進金額 top 15賣出金額
# 1日 5日 10日 20日 30日 45日 60日 120日


# Create the sqlalchemy engine using the pyodbc connection
engine = create_engine( "mysql+mysqldb://root:28wC75#D@127.0.0.1/StockDB?charset=utf8" )

class dbHandle:

    def __init__( self, server, database, username, password ):

        self.date_lst = [ ]
        print( "Initial Database connection..." + database )

        self.dbname = database

        self.con_db = mysql.connector.connect( host     = server,
                                               user     = username,
                                               passwd   = password,
                                               database = database,
                                               charset  = "utf8"
                                              )
        self.cur_db = self.con_db.cursor( )
        self.con_db.commit( )

    def ResetTable( self, table ):

        cmd = 'DROP TABLE IF EXISTS ' + table

        with self.cur_db.execute( cmd ):
            print( 'Successfully Deleter ' + table )

    def CreateTable( self ):

        cmd = '''CREATE TABLE dbo.CONCENTRATION
                        (
                        stock nvarchar(10) NOT NULL,
                        date date NOT NULL,
                        one decimal(6, 2) NULL,
                        three decimal(6, 2) NULL,
                        five decimal(6, 2) NULL,
                        ten decimal(6, 2) NULL,
                        twenty decimal(6, 2) NULL,
                        sixty decimal(6, 2) NULL
                        )  ON [PRIMARY]
                        
                        COMMIT'''

        self.cur_db.execute( cmd )

    def GetDates( self, num, days ):

        cmd = 'SELECT date FROM StockDB.BROKERAGE WHERE stock = \'{1}\' ' \
              'GROUP BY date ORDER BY date DESC LIMIT {0}'.format( days, num )
              
        # print( cmd )

        self.cur_db.execute( cmd )
        
        ft = self.cur_db.fetchall( )
        
        date = [ elt[ 0 ] for elt in ft ]
        self.date_lst = [ val.strftime( "%Y/%m/%d" ) for val in sorted( date, reverse = True ) ]

        # print( '日期範圍 self.date_lst =>', self.date_lst )

    def Get_BetweenDayList( self, interval ):

        # copy list from self
        cpy_lst = self.date_lst[ : ]
        ret_list = [ ]

        while len( cpy_lst ) > interval:
            ret_list.append( ( cpy_lst[ 0 ], cpy_lst[ interval - 1 ] ) )
            cpy_lst.pop( 0 )
        return ret_list

    def GetTopBuyBetweenDay( self, df ):

        in_df = df.copy()

        in_df[ 'buy_sell_vol' ] = in_df[ 'buy_volume' ] - in_df[ 'sell_volume' ]
        in_df[ 'buy_sell_vol' ] = in_df[ 'buy_sell_vol' ] / 1000
        in_df[ 'buy_sell_price' ] = in_df[ 'buy_sell_vol' ] * in_df[ 'price' ]

        del in_df[ 'price' ]

        in_df = in_df.groupby( [ 'brokerage' ], sort = False ).sum( )
        in_df.sort_values( 'buy_sell_vol', ascending = False, inplace = True )

        buy_num = ( in_df[ 'buy_volume' ] > 0 ).sum( )
        sell_num = ( in_df[ 'sell_volume' ] > 0 ).sum( )

        del in_df[ 'buy_volume' ]
        del in_df[ 'sell_volume' ]

        # in_df = in_df.round( { 'buy_sell_vol': 1, 'buy_sell_price': 1 } )
        in_df.reset_index( inplace = True )
        in_df.rename( index = str, columns = { "brokerage": "name", 
                                               'buy_sell_vol': 'vol', 
                                               'buy_sell_price': 'price' }, inplace = True )

        buy_df = in_df.head( 15 ).copy()

        sell_df = in_df.tail( 15 ).copy()
        sell_df = sell_df.iloc[ ::-1 ]

        buy_chip = [ ]
        sell_chip = [ ]

        try:
            for row in buy_df.itertuples( index = False, name = "buy" ):
                buy_chip.append( row )

            for row in sell_df.itertuples( index = False, name = "buy" ):
                sell_chip.append( row )

            buy_vol = buy_df[ 'vol' ].sum( )
            sell_vol = sell_df[ 'vol' ].sum( )

            return buy_chip, sell_chip, buy_vol, sell_vol, ( buy_num - sell_num )

        except Exception:
            print( 'Error 計算籌碼集中' )
            return None, None, None, None, None

    def GetConcentrateBetweenDay( self, day_lst, val, cap, df, vol_df ):

        chip = namedtuple( 'Info', [ 'concentrate', 'sum_vol', 'sub_vol', 'top15_buy', 'top15_sell'
                                     'weight', 'turnover', 'sub_security' ] )

        chip.concentrate  = None
        chip.sub_vol      = None
        chip.sum_vol      = None
        chip.top15_buy    = None
        chip.top15_sell   = None
        chip.weight       = None
        chip.turnover     = None
        chip.sub_security = None

        try:
            end = day_lst[ val ][ 0 ]
            start = day_lst[ val ][ 1 ]
        except IndexError:
            return chip

        mask = (df[ 'date' ] >= start) & (df[ 'date' ] <= end)
        df = df.loc[ mask ]

        vol_df = vol_df.loc[ (vol_df[ 'date' ] >= start) & (vol_df[ 'date' ] <= end) ]
        sum_vol = vol_df.volume.sum()

        # start = datetime.now( )

        buy_chip, sell_chip, buy_vol, sell_vol, sub_security = self.GetTopBuyBetweenDay( df )
        
        # end = datetime.now( )
        # print( 'group by it took: ', end-start )

        if buy_vol is None or sell_vol is None or sum_vol is None: return chip
        if sum_vol == 0 or ( buy_vol - sell_vol ) == 0: return chip

        chip.concentrate = round( ( ( buy_vol + sell_vol ) / sum_vol ) * 100, 1 )

        chip.sum_vol = sum_vol
        chip.sub_vol = round( buy_vol + sell_vol, 0 )
        chip.top15_sell = sell_chip
        chip.top15_buy  = buy_chip
        chip.sub_security = sub_security

        if chip.sub_vol is None: chip.weight = None
        else: chip.weight = round( chip.sub_vol / cap * 100, 2 )

        if chip.sum_vol is None: chip.turnover = None
        else: chip.turnover = round( chip.sum_vol / cap * 100, 2 )

        return chip

def chip_sort( df, stock, date, close_price, obj, day_range, sum_vol ):

    if obj is None or df is None:
        return df

    data = dict( )
    data[ '股號' ] = stock
    data[ '收盤' ] = close_price
    data[ '統計天數' ] = day_range
    data[ '日期' ] = date

    # print( obj )

    for i in range( 15 ):
        # print( obj[i].price, obj[i].vol, stock, date, day_range, obj[ i ].name )
        try:
            data[ '券商{}'.format( i + 1 ) ] = obj[ i ].name
            data[ '券商買賣超{}'.format( i + 1 ) ] = round( obj[ i ].vol, 0 )
            data[ '券商損益{}(萬)'.format( i + 1 ) ] = round( ( ( close_price * obj[ i ].vol ) - ( obj[ i ].price ) ) / 10, 0 )
            data[ '重押比{}'.format( i + 1 ) ] = round( obj[ i ].vol / sum_vol * 100, 1 )
        except Exception:
            data[ '券商{}'.format( i + 1 ) ]       = None
            data[ '券商買賣超{}'.format( i + 1 ) ] = None
            close_price = None
            data[ '券商損益{}(萬)'.format( i + 1 ) ] = None
            data[ '重押比{}'.format( i + 1 ) ] = None
        try:
            data[ '券商均價{}'.format( i + 1 ) ] = round( obj[ i ].price / obj[ i ].vol, 1 )
        except Exception:
            data[ '券商均價{}'.format( i + 1 ) ] = None

    df = df.append( data, ignore_index = True )
    return df

def unit( tar_file, stock_lst, capital_df ):

    # 回推天數，計算集中度
    days = 10
    tmp  = './籌碼集中/籌碼集中暫存.csv'

    db = dbHandle( '127.0.0.1', 'StockDB', 'root', "28wC75#D" )

    # db.ResetTable( 'CONCENTRATION' )
    # db.CreateTable(  )
    
    chip_cols = [ '股號', '日期', '統計天數', '收盤',

                  '券商1', '券商2', '券商3', '券商4', '券商5', '券商6',
                  '券商7', '券商8', '券商9', '券商10', '券商11', '券商12',
                  '券商13', '券商14', '券商15',

                  '券商均價1', '券商均價2', '券商均價3', '券商均價4', '券商均價5', '券商均價6',
                  '券商均價7', '券商均價8', '券商均價9', '券商均價10', '券商均價11',
                  '券商均價12', '券商均價13', '券商均價14', '券商均價15',

                  '券商損益1(萬)', '券商損益2(萬)', '券商損益3(萬)', '券商損益4(萬)', '券商損益5(萬)', '券商損益6(萬)',
                  '券商損益7(萬)', '券商損益8(萬)', '券商損益9(萬)', '券商損益10(萬)', '券商損益11(萬)', '券商損益12(萬)',
                  '券商損益13(萬)', '券商損益14(萬)', '券商損益15(萬)',

                  '重押比1', '重押比2', '重押比3', '重押比4', '重押比5', '重押比6', '重押比7', '重押比8',
                  '重押比9', '重押比10', '重押比11', '重押比12', '重押比13', '重押比14', '重押比15',

                  '券商買賣超1', '券商買賣超2', '券商買賣超3', '券商買賣超4', '券商買賣超5', '券商買賣超6',
                  '券商買賣超7', '券商買賣超8', '券商買賣超9', '券商買賣超10', '券商買賣超11',
                  '券商買賣超12', '券商買賣超13', '券商買賣超14', '券商買賣超15' ]

    ChipBuy_df = pd.DataFrame( columns = chip_cols )
    ChipSell_df = pd.DataFrame( columns = chip_cols )

    cols = [ '股號', '日期', '收盤',
             '01天集中%', '03天集中%', '05天集中%', '10天集中%', '20天集中%', '60天集中%',
             '01天佔股本比重', '03天佔股本比重', '05天佔股本比重', '10天佔股本比重', '20天佔股本比重', '60天佔股本比重',
             '01天周轉率', '03天周轉率', '05天周轉率', '10天周轉率', '20天周轉率', '60天周轉率',
             '01天主力買賣超(張)', '03天主力買賣超(張)', '05天主力買賣超(張)', '10天主力買賣超(張)', '20天主力買賣超(張)', '60天主力買賣超(張)',
             '01天家數差', '03天家數差', '05天家數差', '10天家數差', '20天家數差', '60天家數差',
             ]

    if os.path.isfile( tmp ) is False:
        with open( tmp, 'wb' ) as f:
            csv.writer( f )
        df = pd.DataFrame( columns = cols )
        print( "Create File {}".format( tmp ) )
    else:
        df = pd.read_csv( tmp, sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '股號': str, '日期': str } )
        del df[ 'Unnamed: 0' ]

    while stock_lst:

        stock = stock_lst.pop( 0 )
        print( '{0:<7} {1}'.format( stock, len( stock_lst ) ) )

        db.GetDates( stock, '75' )
        day01_lst = db.Get_BetweenDayList( 1 )
        day03_lst = db.Get_BetweenDayList( 3 )
        day05_lst = db.Get_BetweenDayList( 5 )
        day10_lst = db.Get_BetweenDayList( 10 )
        day20_lst = db.Get_BetweenDayList( 20 )
        day60_lst = db.Get_BetweenDayList( 60 )

        capital = capital_df.loc[ capital_df[ 'stock' ] == stock, '股本(億)' ].values[ 0 ] * 10000

        try:
            cmd = '''
            SELECT brokerage, buy_volume, sell_volume, price, date FROM BROKERAGE
            WHERE stock = \'{0}\' AND date between \'{1}\' and \'{2}\';'''.format( stock, db.date_lst[ -1 ], db.date_lst[ 0 ] )
        except Exception:
            continue

        # print( db.date_lst[ -1 ], db.date_lst[ 0 ] )
        # print( cmd )

        in_df = pd.read_sql_query( cmd, engine )
        in_df[ 'date' ] = pd.to_datetime( in_df[ 'date' ] )

        price_cmd = '''
                    SELECT volume, close_price, date FROM TECH_D 
                    WHERE stock = \'{0}\' AND date between \'{1}\' and \'{2}\';
                    '''.format( stock, db.date_lst[ -1 ], db.date_lst[ 0 ] )
                       
        price_df = pd.read_sql_query( price_cmd, engine )
        price_df[ 'date' ] = pd.to_datetime( price_df[ 'date' ] )

        if price_df.empty is True:
            continue

        for val in range( days ):

            if len( day01_lst ) <= val:
                continue

            codi = ( df['股號'] == stock ) & ( df[ '日期' ] == day01_lst[ val ][ 0 ] )
            if df[ codi ].empty is False:
                continue

            chip_01 = db.GetConcentrateBetweenDay( day01_lst, val, capital, in_df, price_df )
            chip_03 = db.GetConcentrateBetweenDay( day03_lst, val, capital, in_df, price_df )
            chip_05 = db.GetConcentrateBetweenDay( day05_lst, val, capital, in_df, price_df )
            chip_10 = db.GetConcentrateBetweenDay( day10_lst, val, capital, in_df, price_df )
            chip_20 = db.GetConcentrateBetweenDay( day20_lst, val, capital, in_df, price_df )
            chip_60 = db.GetConcentrateBetweenDay( day60_lst, val, capital, in_df, price_df )
            
            date  = day01_lst[ val ][ 0 ]

            # print( date )
            # print( price_df )

            try:
                price = price_df.loc[ price_df[ 'date' ] == date, 'close_price' ].values[ 0 ]
            except IndexError:
                continue

            df = df.append( { '股號': stock,
                              '日期': date,
                              '收盤': price,

                              '01天集中%': chip_01.concentrate,
                              '03天集中%': chip_03.concentrate,
                              '05天集中%': chip_05.concentrate,
                              '10天集中%': chip_10.concentrate,
                              '20天集中%': chip_20.concentrate,
                              '60天集中%': chip_60.concentrate,

                              '01天佔股本比重': chip_01.weight,
                              '03天佔股本比重': chip_03.weight,
                              '05天佔股本比重': chip_05.weight,
                              '10天佔股本比重': chip_10.weight,
                              '20天佔股本比重': chip_20.weight,
                              '60天佔股本比重': chip_60.weight,

                              '01天周轉率': chip_01.turnover,
                              '03天周轉率': chip_03.turnover,
                              '05天周轉率': chip_05.turnover,
                              '10天周轉率': chip_10.turnover,
                              '20天周轉率': chip_20.turnover,
                              '60天周轉率': chip_60.turnover,

                              '01天主力買賣超(張)': chip_01.sub_vol,
                              '03天主力買賣超(張)': chip_03.sub_vol,
                              '05天主力買賣超(張)': chip_05.sub_vol,
                              '10天主力買賣超(張)': chip_10.sub_vol,
                              '20天主力買賣超(張)': chip_20.sub_vol,
                              '60天主力買賣超(張)': chip_60.sub_vol,

                              '01天家數差': chip_01.sub_security,
                              '03天家數差': chip_03.sub_security,
                              '05天家數差': chip_05.sub_security,
                              '10天家數差': chip_10.sub_security,
                              '20天家數差': chip_20.sub_security,
                              '60天家數差': chip_60.sub_security,

                              }, ignore_index=True )

            # start = datetime.now( ) 

            ChipBuy_df = chip_sort( ChipBuy_df, stock, date, price, chip_01.top15_buy, 1, chip_01.sum_vol )
            ChipBuy_df = chip_sort( ChipBuy_df, stock, date, price, chip_03.top15_buy, 3, chip_03.sum_vol )
            ChipBuy_df = chip_sort( ChipBuy_df, stock, date, price, chip_05.top15_buy, 5, chip_05.sum_vol )
            ChipBuy_df = chip_sort( ChipBuy_df, stock, date, price, chip_10.top15_buy, 10, chip_10.sum_vol )
            ChipBuy_df = chip_sort( ChipBuy_df, stock, date, price, chip_20.top15_buy, 20, chip_20.sum_vol )
            ChipBuy_df = chip_sort( ChipBuy_df, stock, date, price, chip_60.top15_buy, 60, chip_60.sum_vol )

            ChipSell_df = chip_sort( ChipSell_df, stock, date, price, chip_01.top15_sell, 1, chip_01.sum_vol )
            ChipSell_df = chip_sort( ChipSell_df, stock, date, price, chip_03.top15_sell, 3, chip_03.sum_vol )
            ChipSell_df = chip_sort( ChipSell_df, stock, date, price, chip_05.top15_sell, 5, chip_05.sum_vol )
            ChipSell_df = chip_sort( ChipSell_df, stock, date, price, chip_10.top15_sell, 10, chip_10.sum_vol )
            ChipSell_df = chip_sort( ChipSell_df, stock, date, price, chip_20.top15_sell, 20, chip_20.sum_vol )
            ChipSell_df = chip_sort( ChipSell_df, stock, date, price, chip_60.top15_sell, 60, chip_60.sum_vol )
            
            # end = datetime.now( )
            # print( 'chip_sort it took: ', end-start )
            

    if os.path.isfile( './籌碼集中/ChipBuy.csv' ) is False:
        with open( './籌碼集中/ChipBuy.csv', 'w', newline = '\n', encoding = 'utf8' ) as f:
            w = csv.writer( f )
        ChipBuySrc_df = pd.DataFrame( columns = chip_cols )
    else:
        ChipBuySrc_df = pd.read_csv( './籌碼集中/ChipBuy.csv', sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '股號': str, '日期': str } )
        del ChipBuySrc_df[ 'Unnamed: 0' ]

    if os.path.isfile( './籌碼集中/ChipSell.csv' ) is False:
        with open( './籌碼集中/ChipSell.csv', 'w', newline = '\n', encoding = 'utf8' ) as f:
            w = csv.writer( f )
        ChipSellSrc_df = pd.DataFrame( columns = chip_cols )
    else:
        ChipSellSrc_df = pd.read_csv( './籌碼集中/ChipSell.csv', sep = ',', encoding = 'utf8', false_values = 'NA', dtype = { '股號': str, '日期': str } )
        del ChipSellSrc_df[ 'Unnamed: 0' ]
    # --------------------------------------

    ChipBuy_df  = ChipBuy_df.append( ChipBuySrc_df )
    ChipSell_df = ChipSell_df.append( ChipSellSrc_df )

    ChipBuy_df  = ChipBuy_df[ chip_cols ]
    ChipSell_df = ChipSell_df[ chip_cols ]

    df.sort_values( by = [ '日期', '股號' ], ascending = [ False, True ], inplace = True )
    df.reset_index( drop = True, inplace = True )
    ChipBuy_df.reset_index( drop = True, inplace =  True )
    ChipSell_df.reset_index( drop = True, inplace =  True )

    df.to_csv( tmp, encoding = 'utf-8' )
    ChipBuy_df.to_csv( './籌碼集中/ChipBuy.csv', encoding = 'utf-8' )
    ChipSell_df.to_csv( './籌碼集中/ChipSell.csv', encoding = 'utf-8' )
    # --------------------------------------

    df = df[ df[ '日期' ] >  db.date_lst[ 5 ] ]
    ChipBuy_df = ChipBuy_df[ ChipBuy_df[ '日期'] > db.date_lst[ 5 ] ]
    ChipSell_df = ChipSell_df[ ChipSell_df[ '日期'] > db.date_lst[ 5 ] ]

    df_writer = pd.ExcelWriter( tar_file )
    df.to_excel( df_writer, sheet_name = '籌碼分析' )
    ChipBuy_df.to_excel( df_writer, sheet_name = '卷商買超' )
    ChipSell_df.to_excel( df_writer, sheet_name = '卷商賣超' )
    df_writer.save()
    print( tar_file )

if __name__ == '__main__':

    start_tmr = datetime.now( )

    capital = goodinfo.capital( path = './StockList_股本.csv' )
    condition = capital.df[ '股本(億)' ] >= 20
    capital.df = capital.df[ condition ]

    stock_lst = sorted( capital.df[ 'stock' ].tolist( ) )
    # stock_lst = [ '2317' ]

    tmr = datetime.now( ).strftime( '%y%m%d_%H%M' )
    unit( './籌碼集中/籌碼集中_{}.xls'.format( tmr ), stock_lst, capital.df )

    print( datetime.now( ) - start_tmr )

