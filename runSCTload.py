import pandas as pd
import pyodbc
import glob
import os
from fnmatch import fnmatch
from datetime import date

root = os.path.dirname(os.path.realpath(__file__))
pattern = "sct*.csv"

#csvfiles = []
#for file in glob.glob("*.csv"):
#csvfiles.append(file)

for path, subdirs, files in os.walk(root):
    for name in files:
        if fnmatch(name, pattern):
            SCTFile = (os.path.join(path, name))
            print(SCTFile)

            p1 = SCTFile.index('reports')
            p2 = SCTFile.index('csv')
            sysname = (SCTFile[p1:100])
            nametab = sysname.split('/')
            dbname=nametab[1]
            schemaname=nametab[2]

            # Search MSSQL features
            feature_1 = 'Compression'
            feature_2 = 'Partitioning'
            feature_3 = 'Encryption'
            source_db = 'Source database'
            SQLversion = 'Microsoft SQL Server'
            SQLEdition = ''
            OSVersion = 'Windows'
            today = date.today()
            ReportDate = today.strftime("%m/%d/%y")

            feature_1_sts = 'False'
            feature_2_sts = 'False'
            feature_3_sts = 'False'

            inFile = open(SCTFile, "rb")
            #reader = csv.reader(codecs.iterdecode(inFile, 'utf-8'))
            reader = inFile.readlines()

            for row in reader:
                #print (row)

                str = row.decode()
                if feature_1 in str:
                   feature_1_sts = 'True'

                if feature_2 in str:
                   feature_2_sts = 'True'

                if feature_3 in str:
                   feature_3_sts = 'True'

                if source_db in str:
                   #print(str)
                   pos = str.index('\\')
                   Instance = (str[17:pos])
                   print (Instance)

                if SQLversion in str:
                   pos = str.index(',,')
                   Version = (str[21:pos])
                   #print (Version)

                if OSVersion in str:
                   pos0 = str.index(OSVersion)
                   pos1 = str.index('<')
                   OSVer = (str[pos0:pos1])
                   #print (OSVer)

                if 'Datacenter Edition' in str:
                   SQLEdition = 'Datacenter Edition'
                   #print (SQLEdition)

                if 'Enterprise Edition' in str:
                   SQLEdition = 'Enterprise Edition'
                   #print (SQLEdition)

                if 'Standard Edition' in str:
                   SQLEdition = 'Standard Edition'
                   #print (SQLEdition)

                if 'Developer Edition' in str:
                   SQLEdition = 'Developer Edition'
                   #print (SQLEdition)

                if 'Web Edition' in str:
                   SQLEdition = 'Web Edition'
                   #print (SQLEdition)

            inFile.close()

            #######################################################################
            # Connect to the database and record SCT information collected
            #######################################################################

            data = pd.read_csv (SCTFile,nrows=12)
            df = pd.DataFrame(data, columns= ['Category','Number of objects','Objects automatically converted','Objects with simple actions','Objects with medium-complexity actions','Objects with complex actions','Total lines of code'])
            df.columns = ['Category','Numberofobjects','ObjAutoConverted','ObjSimpleActions','ObjWithMediumActions','ObjComplexActions','TotalLinesCode']

            df = df.fillna(value=0)

            #print(df)

            # Connect to SQL Server
            conn = pyodbc.connect('Driver={MySQL};'
                                  'Server=mysql-bb8.ccuwkrymeppv.us-east-1.rds.amazonaws.com;'
                                  'Database=sctdb;'
                                  'UID=admin;'
                                  'PWD=Admin123;')

            # Delete if Instance already exists
            curdel = conn.cursor()
            curdel.execute('''
                           DELETE sct_reports WHERE InstanceName = ?
                           ''',
                           Instance
                           )

            # Insert into Table
            cursor = conn.cursor()

            for row in df.itertuples():
                cursor.execute('''
                            INSERT INTO sct_reports 
                            (InstanceName,InstanceVersion,SQLEdition,OSVersion,DBName,SchemaName,Category,Numberofobjects,ObjAutoConverted,
                            ObjSimpleActions,ObjWithMediumActions,ObjComplexActions,TotalLinesCode,
                            CompressionEnabled,PartitionEnabled,EncryptionEnabled,ReportDate)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            ''',
                            Instance,
                            Version,
                            SQLEdition,
                            OSVer,
                            dbname,
                            schemaname,
                            row.Category,
                            row.Numberofobjects,
                            row.ObjAutoConverted,
                            row.ObjSimpleActions,
                            row.ObjWithMediumActions,
                            row.ObjComplexActions,
                            row.TotalLinesCode,
                            feature_1_sts, feature_2_sts, feature_3_sts,
                            ReportDate
                            )
            conn.commit()

