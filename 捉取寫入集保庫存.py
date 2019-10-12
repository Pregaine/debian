# coding=utf8

import requests
from datetime import datetime
from bs4 import BeautifulSoup as BS
import module.db.stock_db as DB
# import module.inquire.GetStockNum as GetStockNum
import pandas as pd
import codes.codes as TWSE
import sys
import time

class TDCC_Handle:

    def __init__(self):

        # self.soup = BS( res.text, "html5lib" )
        self.datelst = [ ]
        self.d = { '1': '1_999',
                   '2': '1000_5000',
                   '3': '5001_10000',
                   '4': '10001_15000',
                   '5': '15001_20000',
                   '6': '20001_30000',
                   '7': '30001_40000',
                   '8': '40001_50000',
                   '9': '50001_100000',
                   '10': '100001_200000',
                   '11': '200001_400000',
                   '12': '400001_600000',
                   '13': '600001_800000',
                   '14': '800001_1000000',
                   '15': 'Up_1000001' }

    def qrerry_date(self):

        for date in self.soup.find_all( 'option' ):
            date = '{}'.format( date.text )
            self.datelst.append( date )

        return self.datelst

    def querry_stock( self, stock, date, res ):

        df = pd.DataFrame( )
        soup = BS( res.text, "html5lib" )

        try:
            tb = soup.select( '.mt' )[ 1 ]
        except Exception:
            print( stock, date, soup.findAll( 'script' )[ 2 ].text )
            return

        df[ 'stock' ] = [ stock ]
        df[ 'date'] = [ date ]

        for tr in tb.select( 'tr' ):
            if tr.select( 'td' )[ 0 ].text in self.d.keys():

                val = tr.select( 'td' )[ 0 ].text
                people = int( tr.select( 'td' )[ 2 ].text.replace( ',', '' ) )
                unit = int( tr.select( 'td' )[ 3 ].text.replace( ',', '' ) )
                proportion = float( tr.select( 'td' )[ 4 ].text.replace( ',', '' ) )

                df[ 'Share_Rating_People_' + self.d[ val ] ] = [ people ]
                df[ 'Share_Rating_Unit_' + self.d[ val ] ] = [ unit ]
                df[ 'Share_Rating_Proportion_' + self.d[ val ] ] = [ proportion ]
                
        # print( df )

        return df

def main( ):

    DB_OBJ = DB.Handle( '127.0.0.1', 'StockDB', 'TDCC', 'root', "28wC75#D" )

    # DB_OBJ.ResetTable( 'TDCC' )
    # DB_OBJ.CreateTable_TDCC( )
    
    TDCC_OBJ = TDCC_Handle( )

    stock_lst = list( TWSE.codes.keys() )
    # stock_lst = [ '2316' ]
    # print( stock_lst )

    # 查詢網路集保庫存日期
    # date_lst = TDCC_OBJ.qrerry_date( )

    date_lst = sys.argv[ 1: ]
    # date_lst = [ "20180921", "20181019", "20180907", "20180914", "20180928", "20181005", "20181012" ]
    # print( stock_lst )

    url = "https://www.tdcc.com.tw/smWeb/QryStockAjax.do"
    headers = { 'content-type'  : "application/x-www-form-urlencoded" }

    while len( stock_lst ) != 0:

        stock = stock_lst.pop( 0 )

        stock_db_date_lst = DB_OBJ.GetAllData( 'date', "stock = \'{}\' ".format( stock ) )
        
        # stock_db_date_lst = []
        
        stock_db_date_lst = list( set( date_lst ) - set( stock_db_date_lst ) )

        print( '{} 資料庫日期筆數 {}'.format( stock, len( stock_db_date_lst ) ) )

        while len(  stock_db_date_lst  ) != 0:

            date = stock_db_date_lst.pop( 0 )

            payload = {
                "REQ_OPR": "SELECT",
                'SqlMethod': 'StockNo',
                'StockName': '',
                'StockNo': '{}'.format( stock ),
                'clkStockName'   : '',
                'clkStockNo': '{}'.format( stock ),
                'scaDate' : '{}'.format( date ),
                'scaDates': '{}'.format( date ), }

            try:
                response = requests.request( "POST", url, data = payload, headers = headers, timeout = 2 )

            except Exception as e:
                print( e )
                time.sleep( 1 )
                print( 'ConnectionError {} {}'.format( stock, date ) )
                stock_db_date_lst.append( date )
                stock_lst.append( stock )
                continue
                
            print( 'Web Return Status', response.status_code, stock, len(  stock_lst  ) )

            time.sleep( 1 )

            if response.status_code != 200:
                stock_db_date_lst.append( date )
                continue

            # 捉取資料根據日期

            soup = BS( response.text, "html5lib" )
            try:
                if soup.find( 'td', align = "center", colspan = "5" ).text == '無此資料':
                    print( '{} {} 無此資料'.format( stock, date ) )
                    continue
            except:
                pass

            df = TDCC_OBJ.querry_stock( stock, date, response )
            # print( df.head() )
            if df.empty:
                continue

            data = df.iloc[ 0 ].tolist( )
            data = data[ 0 : 2 ] + [ float( i ) for i in data[ 2: ] ]
            
            
            DB_OBJ.WriteData( data )

            # print( df )
            # stock_db_date_lst.append( date )
            # print( '{} {}寫入資料庫失敗'.format( stock, date ) )

if __name__ == '__main__':

    start_tmr = datetime.now( )
    main( )
    print( datetime.now( ) - start_tmr )