# -*- coding: utf-8 -*-
# import pyodbc
import mysql.connector
import re
import csv
from datetime import datetime
import glob
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import shutil
import os
# import MySQLdb 
import io
from pandas import isnull
from glob import glob

# - Create the sqlalchemy engine using the pyodbc connection
# engine = create_engine( "mysql+mysqlconnector://root:pRegaine@localhost/StockDB", convert_unicode = True, echo=False )

engine = create_engine( "mysql+mysqlconnector://root:28wC75#D@127.0.0.1/StockDB?charset=utf8" )

# mysqlconnector
# engine = create_engine( "mysql+mysqlconnector://root:pRegaine@localhost/StockDB", echo=False )

# PyMySQL
# engine = create_engine(  'mysql+pymysql://root:pRegaine@localhost/StockDB'  )

def sorter(x):
    return int( os.path.basename(x).split('_')[ 0 ] )

def db_colname(pandas_colname):
    '''convert pandas column name to a DBMS column name
        TODO: deal with name length restrictions, esp for Oracle
    '''
    colname =  pandas_colname.replace(' ','_').strip()                  
    return colname

class dbHandle:

    def __init__( self, server, database, username, password ):

        print( "Initial Database connection..." + database )

        self.con_db = mysql.connector.connect( host     = server,
                                               user     = username,
                                               passwd   = password,
                                               database = database,
                                               charset  = "utf8"
                                              )
                                               
        self.cur_db = self.con_db.cursor( )
        self.date = ''
        self.stock = None
        
        self.df = pd.DataFrame()
    
        self.con_db.commit( )

    def ResetTable( self, table ):

        cmd = 'DROP TABLE IF EXISTS {}'.format( table )

        # Do some setup
        self.cur_db.execute( cmd )
        
        print( 'Successfully Del {}'.format( table ) )

    def CreateTable( self ):

        """確認一定長度，且只會有英數字：char
                        確認一定長度，且可能會用非英數以外的字元：nchar
                        長度可變動，且只會有英數字：varchar
                        長度可變動，且可能會用非英數以外的字元：nvarchar
                        price decimal( 8, 2 ) NOT NULL
                        最大位數8位數包含小數點前6位，小數點後2位，或小數點前8位，小數點無"""

        cmd = '''
            CREATE TABLE BROKERAGE
            (
            stock          varchar( 10 ) COLLATE utf8_bin NOT NULL,
            date           DATE NOT NULL,
            brokerage      char( 4 ) COLLATE utf8_bin NOT NULL,
            brokerage_name varchar( 10 ) COLLATE utf8_bin NOT NULL,
            price          DECIMAL( 8, 2 ) NOT NULL,
            buy_volume     DECIMAL( 16, 0 ) DEFAULT 0,
            sell_volume    DECIMAL( 16, 0 ) DEFAULT 0,
            INDEX name ( stock, date )
            );
        '''

        self.cur_db.execute( cmd )
        
        print( 'Successfully Create BROKERAGE' )


    def InsertCSV2DB( self, filename ):

        f = open( filename, 'r', encoding = 'utf8', errors = 'ignore' )

        for row in csv.reader( f ):
            if row[ 0 ] == '':
                return

            brokerage_symbol = row[ 1 ][ 0:4 ]
            brokerage_name = row[ 1 ][ 4:len( row[ 1 ] ) ].replace( ' ', '' ).replace( '\u3000', '' )
            price       = row[ 2 ]
            buy_volume  = row[ 3 ]
            sell_volume = row[ 4 ]

            cmd = 'INSERT INTO BROKERAGE ( stock, date, brokerage, brokerage_name, price, buy_volume, sell_volume ) \
                   VALUES ( ?, ?, ?, ?, ?, ?, ? )'
            try:
                row = ( self.stock, self.date, brokerage_symbol, brokerage_name, price, buy_volume, sell_volume )
                self.cur_db.execute( cmd, row )
                print( '寫入成功', row )
            except:
                print( '寫入失敗', row )

    def InsertDF2DB( self, filename ):

        # f = open( filename, 'r', encoding = 'utf8', errors = 'ignore' )
        lst = [ 'index', 'brokerage', 'price', 'buy_volume', 'sell_volume' ]

        df = pd.read_csv( filename, lineterminator = '\n', encoding = 'utf8', names = lst, sep = ',',
                          index_col = False,
                          na_filter=False,
                        )
                           
        del df[ 'index' ]
        df[ 'brokerage' ].replace( '\s+', '', inplace = True, regex = True )
        df[ 'brokerage_name' ] = df[ 'brokerage' ].str[ 4: ]
        df[ 'brokerage'] = df[ 'brokerage' ].str[ 0:4 ]
        df[ 'stock' ] = self.stock
        df[ 'date' ] = self.date

        self.df = self.df.append( df, ignore_index=True )
        
        # print( self.df )
        
        # df.to_sql( name = 'BROKERAGE', 
                   # con = engine, 
                   # index = False, 
                   # if_exists = 'append', 
                   # index_label = None, 
                   # dtype={ 
                        # 'stock'          : sqlalchemy.types.VARCHAR( length=10 ),
                        # 'date'           : sqlalchemy.types.Date( ),
                        # 'brokerage'      : sqlalchemy.types.CHAR( length=4 ),
                        # 'brokerage_name' : sqlalchemy.types.VARCHAR( length=10 ),
                        # 'price'          : sqlalchemy.types.DECIMAL( precision=8, scale=2 ),
                        # 'buy_volume'     : sqlalchemy.types.DECIMAL( precision=16, scale=0 ),
                        # 'sell_volume'    : sqlalchemy.types.DECIMAL( precision=16, scale=0 )
                   # }
        # )
        
        # frame = df
        
        # wildcards = ','.join(['%s'] * len(frame.columns))
        # cols=[ db_colname(k) for k in frame.dtypes.index]
        # colnames = ','.join(cols)
        # insert_sql = 'INSERT INTO BROKERAGE (%s) VALUES (%s)' % ( colnames, wildcards)
        # print( insert_sql )
        # # data = [tuple(x) for x in frame.values]
        # data= [ tuple([ None if isnull(v) else v for v in rw]) for rw in frame.values ] 
        # print( data[0] )
        
        # self.cur_db.executemany(insert_sql, data)
        
        # self.con_db.commit()
        

    def InsertDB( self ):

        dst_path  = './Broker/AlreadyWritten/'
        
        paths = [ p for p in glob('./Broker/券商分點/*[0-9]/') ]
        
        for path in paths:
        
            dstFolderPath = "{}{}".format( dst_path, path.replace( "./Broker/", "" ) )
                
            print( dstFolderPath )    
            
            if not os.path.exists( dstFolderPath ):
                os.makedirs( dstFolderPath )
                
            self.df = pd.DataFrame()
            
            files = [ f for f in glob( path + '*.csv' ) ]
            
            for filename in files:
        
                self.date  = filename[ -10:-4 ]
                self.stock = re.sub( "[0-9]{8}", '', filename.split( '_' )[ 1 ] )
                self.stock = self.stock[1:]
                stock_name = filename.split( '_' )[ 2 ]
            
                cmd = 'SELECT stock, date FROM BROKERAGE WHERE stock = \'{}\' and date = \'{}\' LIMIT 1;'.format( self.stock, self.date )
            
                self.cur_db.execute( cmd )
                ft = self.cur_db.fetchone( )

                if ft is None:
                    print( '{0:<5} {1:<7} {2:<7}'.format( self.date, self.stock, stock_name ) )
                    self.InsertDF2DB( filename )
                else:
                    print( '資料已存在 {} '.format( filename )  )
                    
                root, file = os.path.split( filename )    
                                       
                shutil.move( filename, '{}{}'.format( dstFolderPath, file ) )
                
            
            shutil.rmtree( path, ignore_errors=True )
    
            frame = self.df
    
            wildcards = ','.join(['%s'] * len(frame.columns))
            cols=[ db_colname(k) for k in frame.dtypes.index]
            colnames = ','.join(cols)
            insert_sql = 'INSERT INTO BROKERAGE (%s) VALUES (%s)' % ( colnames, wildcards)
            # print( insert_sql )
            # # data = [tuple(x) for x in frame.values]
            data= [ tuple([ None if isnull(v) else v for v in rw]) for rw in frame.values ] 
            # print( data[0] )
            
            self.cur_db.executemany(insert_sql, data)       
                    
            self.con_db.commit() 
            
        self.cur_db.close()

def main( ):

    db = dbHandle( '127.0.0.1', 'StockDB', 'root', "28wC75#D" )
    # db.ResetTable( 'BROKERAGE' )
    # db.CreateTable(  )
    db.InsertDB( )

if __name__ == '__main__':

    start_tmr = time.time( )
    main( )
    print( 'The script took {:06.1f} minute !'.format( (time.time( ) - start_tmr) / 60 ) )
