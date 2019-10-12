# -*- coding: utf-8 -*-

from datetime import datetime
import pandas as pd
import re
import numpy as np
from sqlalchemy import create_engine
import mysql.connector
import re
import csv
import time
import shutil
import os
import MySQLdb 
import io
from pandas import isnull
from glob import glob

class DB_Investors :

    def __init__( self, server, database, username, password ):

        self.df = pd.DataFrame( )
        self.src_df = pd.DataFrame( )

        self.datelst = [ ]
        print( "Initial Database connection..." + database )
        
        self.dbname = database
        
        self.con_db = mysql.connector.connect( host     = server,
                                               user     = username,
                                               passwd   = password,
                                               database = database,
                                               charset  = "utf8"
                                              )

        self.cur_db = self.con_db.cursor( buffered = True )
        self.con_db.commit( )
        
        self.stock = ''

    def Reset_Table( self ):
        
        # Do some setup
        self.cur_db.execute( '''DROP TABLE IF EXISTS INVESTORS;''' )
        
        print( 'Successfuly Deleter INVESTORS' )

    def CreatDB( self ):

        self.cur_db.execute( '''

            CREATE TABLE StockDB.INVESTORS 
        	(
                stock varchar( 10 ) COLLATE utf8_bin NOT NULL,
                date DATE NOT NULL,
                
                foreign_sell        int NULL,
                investment_sell     int NULL,
                dealer_sell         int NULL,
                single_day_sell     int NULL, 
                 
                foreign_estimate    int NULL,
                investment_estimate int NULL,
                dealer_estimate     int NULL,
                single_day_estimate int NULL,
                
                foreign_ratio       decimal( 5, 2 ) NULL,
                investment_ratio    decimal( 5, 2 ) NULL,
                
                INDEX name ( stock, date ),
                
                INDEX idx_stock ( stock ),
                INDEX idx_date ( date )
                
        	)''' )
            
        print( 'Successfuly Create 3大法人' )
        
    def FindDuplicate( self, data ):

        # 尋找重覆資料
        cmd = '''SELECT stock, date from StockDB.INVESTORS where stock = \'{}\' and date = \'{}\';'''.format( self.stock, data )
        
        # print( cmd )
        
        self.cur_db.execute( cmd )
        
        ft = self.cur_db.fetchone( )
        
        # print( '比對資料庫{0:>10} {1}'.format( self.stock, data ) )

        if ft is not None:
        
            cmd = '''DELETE FROM StockDB.INVESTORS where stock = \'{}\' and date = \'{}\';'''.format( self.stock, data )
            
            # print( cmd )
            
            self.cur_db.execute( cmd )
            
            print( '刪除重覆資料{0:>10} {1}'.format( self.stock, data ) )
            
            self.con_db.commit( )

    def CompareDB( self ):

        cmd = 'SELECT date, foreign_sell FROM INVESTORS WHERE stock = \'{}\''.format( self.stock )

        self.cur_db.execute( cmd )
        
        ft = self.cur_db.fetchall( )

        lst = [ ]

        for val in ft:

            date = val[ 0 ].strftime( '%y%m%d' )

            foreign_sell = val[ 1 ]

            lst.append( ( date, foreign_sell ) )

        df_db = pd.DataFrame( lst, columns = [ '日期', 'foreign_sell_FromDB' ] )

        left = pd.merge( self.df, df_db, on = [ '日期' ], how = 'left' )

        left = left[ left[ 'foreign_sell_FromDB' ] != left[ '外資買賣超' ] ]

        del left[ 'foreign_sell_FromDB' ]

        self.df = left
        
        for index, row in self.df.iterrows( ):
            # print( self.stock, row[ '日期' ] )
            self.FindDuplicate( row[ '日期' ] )
        # print( self.df )
        # print( stock_num, self.src_df.iloc[ 0 ] )
        # print( stock_num, self.df.iloc[ 0 ] )

    def ReadCSV( self, file ):

        self.df = pd.read_csv( file, 
                               sep = ',', 
                               encoding = 'utf8', 
                               false_values = 'NA', 
                               dtype = { '日期': str } )

        # self.df[ '日期' ] = pd.to_datetime( self.df[ '日期' ], format = "%y%m%d" )
        # print( self.df )

    def WriteDB( self ):

        self.df = self.df.astype( object ).where( pd.notnull( self.df ), None )

        lst = self.df.values.tolist( )

        if len( lst ) == 0:
            # print( '資料庫比對CSV無新資料 {}'.format( self.stock ) )
            return

        for val in lst:

            val[ 0 ] = self.stock
            dt = datetime.strptime( val[ 1 ], '%y%m%d' )
            val[ 1 ] = dt.strftime( "%y-%m-%d" )

            # var_string = ', '.join( '%s' * ( len( val )  ) )
            
            var_string = '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s'
            query_string = 'INSERT INTO INVESTORS VALUES ( {} );'.format( var_string )

            # print( query_string )
            # print( '取出{}'.format( val ) )

            self.cur_db.execute( query_string, val )
            
            print( '寫入資料庫 {} {}'.format( val[ 0 ], val[ 1 ] ) )

def main( ):


    db = DB_Investors( '127.0.0.1', 'StockDB', 'root', '28wC75#D' )

    # db.Reset_Table( )
    # db.CreatDB( )
    
    path = '/home/wenwei/下載/Investors/3大法人/'

    # 讀取資料夾
    for file in os.listdir( path ):

        if file.endswith( ".csv" ) != 1:
            continue
            
        db.stock = file.replace( '_3大法人持股.csv', '' )
        
        # print( db.stock )
        
        db.ReadCSV( '{}{}'.format( path, file ) )
        db.CompareDB( )
        db.WriteDB( )
        
        # exit( )
        
    db.con_db.commit( )
   

if __name__ == '__main__':

    start_tmr = time.time( )
    main( )
    print( 'The script took {:06.1f} minute !'.format( (time.time( ) - start_tmr) / 60 ) )