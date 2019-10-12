# coding: utf-8
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import sys
import glob
import os
from zipfile import ZipFile, ZIP_LZMA
import datetime
import shutil

def Achive_Folder_To_ZIP( sFilePath, DstZipPath ):
    
    """
    input : Folder path and name
    output: using zipfile to ZIP folder
    """
    # zf = zipfile.ZipFile( sFilePath + '.zip', mode='w' )#只儲存不壓縮
    
    zf = ZipFile( DstZipPath, mode='w', compression = ZIP_LZMA )
    
    for root, folders, files in os.walk( sFilePath ):
        for sfile in files:
            aFile = os.path.join(root, sfile)
            zf.write(aFile)
            
    zf.close()
    
def BakupBrokerFolder( SrcFolderPath ):

    for dirname, dirnames, filenames in os.walk( SrcFolderPath ):
        
        for dirname in dirnames:
            
            # print( '{}/{}.zip'.format( SrcFolderPath, dirname )  )
      
            DstZipPath = '{}/{}.zip'.format( SrcFolderPath, dirname )
            
            if os.path.isfile( DstZipPath ):
                print( "{} 已存在 Raspberry Pi".format( DstZipPath ) )
            else:
                SrcPath = '{}/{}'.format( SrcFolderPath, dirname )
                
                print( '壓縮資料夾 {}'.format( SrcPath ) )
                
                Achive_Folder_To_ZIP( SrcPath, DstZipPath )
    
def BakupCorporationFolder( SrcFolderPath ):
    
    # 檢查檔案是否存在
    now = datetime.datetime.now( )
    
    DstZipPath = "{}_{}.zip".format( SrcFolderPath, now.strftime( "%y%m%d") )

    # print( DstZipPath )
    
    if os.path.isfile( DstZipPath ):
        print( "{} 已存在 Raspberry Pi".format( DstZipPath ) )
    else:
        Achive_Folder_To_ZIP( SrcFolderPath, DstZipPath )
# -----------------------------------------------------

def BakupTechFolder( SrcFolderPath ):
    
    filePath = './技術指標/2330_日線技術指標.csv'
    modTimesinceEpoc = os.path.getmtime(filePath)
    modificationTime = datetime.datetime.fromtimestamp(modTimesinceEpoc).strftime( '%y%m%d' )
    print( "Tech Last Modified Time : ", modificationTime )
    
    # 檢查檔案是否存在
    DstZipPath = "{}_{}.zip".format( SrcFolderPath, modificationTime )

    # print( DstZipPath )
    
    if os.path.isfile( DstZipPath ):
        print( "{} 已存在 Raspberry Pi".format( DstZipPath ) )
    else:
        Achive_Folder_To_ZIP( SrcFolderPath, DstZipPath )

def BakupCorporationZipAppend( SrcZipPath ):

    L = []
    
    for ZipFile in glob.glob( SrcZipPath + '*.zip' ):
 
        L.append( ZipFile[ 2: ] )

    return L
# -----------------------------------------------------

def BakupBrokerZipAppend( SrcZipPath ):

    L = []
    
    for ZipFile in glob.glob( SrcZipPath + '*.zip' ):
 
        L.append( ZipFile[ 7: ] )

    return L

# gauth = GoogleAuth()
# gauth.LocalWebserverAuth() # Creates local webserver and auto handles authentication.
# drive = GoogleDrive(gauth)

# exit( )

def DownloadFile( dst, includeStr, obj ):

    if includeStr in obj[ 'title' ]:
    
        src = obj[ 'title' ]
    
        dstFilePath = "{}/{}".format( dst, obj[ 'title' ] )
    
        if os.path.isfile( dstFilePath ) is False:
    
            print( 'Download {}'.format( obj[ 'title' ] ) )
    
            obj.GetContentFile( obj[ 'title' ] )

            shutil.move( src, dstFilePath )
            
            with ZipFile( dstFilePath, 'r' ) as zip_ref:
                
                zip_ref.extractall( "{}/".format( dst ) )
        
        else:
            
            print( '{} is exist'.format( obj[ 'title' ] ) )


gauth = GoogleAuth()
gauth.CommandLineAuth() #透過授權碼認證
drive = GoogleDrive(gauth)
# -------------------------------------------------------

UploadZipList = [ ]
UploadMarginZipList = [ ]
UploadBrokerZipList = [ ]

# BakupCorporationFolder( './3大法人' )
# BakupCorporationFolder( './融資融卷' )
# BakupTechFolder( './技術指標' )
# BakupBrokerFolder( './券商分點' )


# UploadZipList = BakupCorporationZipAppend( './3大法人' )
# UploadTechZipList = BakupCorporationZipAppend( './技術指標' )
# UploadMarginZipList = BakupCorporationZipAppend( './融資融卷' )
# UploadBrokerZipList = BakupBrokerZipAppend( './券商分點/全台卷商交易資料' )

# UploadZipList = UploadMarginZipList + UploadZipList + UploadBrokerZipList + UploadTechZipList

# print( UploadZipList )
# ----------------------------------------------------

# Google Drive Stock Folder
parent_id = '1W6AF_pa3v3jYqw7aT4H0xFmuIRDmiAhC'

file_list = drive.ListFile( { 'q': "'1W6AF_pa3v3jYqw7aT4H0xFmuIRDmiAhC' in parents and trashed=false" } ).GetList()

fileTitleList = [ ]

for file in file_list:

    # fileTitleList.append( file[ 'title' ] )
    
    DownloadFile( "./Broker", "全台卷商交易資料", file )
    
    DownloadFile( "./Tech", "技術指標", file )        
    
    DownloadFile( "./Investors", "3大法人", file )
    
    DownloadFile( "./Margin", "融資融卷", file )
    # -------------------------------------------------------

    
exit( )    
    
print( fileTitleList )

for uploadZipStr in UploadZipList:

    if uploadZipStr not in fileTitleList:
    
        # print( '{} 上傳中'.format( uploadZipStr ) )
        
        # child_folder = drive.CreateFile( { 'title': uploadDirStr, 'parents':[ {'id':parent_id} ],
                                           # "mimeType": "application/vnd.google-apps.folder" } )

        file = drive.CreateFile( { "parents": [ { "kind": "drive#fileLink", "id": parent_id } ] } )    
              
        uploadZipPath = uploadZipStr 

        file[ 'title' ] = uploadZipStr
              
        if "全台卷商交易資料" in uploadZipStr:
        
            uploadZipPath = "券商分點/{}".format( uploadZipStr )
                                
        # print( uploadZipStr )
        
        file.SetContentFile( './{}'.format( uploadZipPath ) )
        
        file.Upload( )
        
        print( '{} 上傳成功'.format( uploadZipStr ) )
    
    else:
        
        print( '{} 已存在 GoogleDrive'.format( uploadZipStr ) )
# -------------------------------------------------------
    
# file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
# for file1 in file_list:       
# print ('title: %s, id: %s' % (file1['title'], file1['id']))
