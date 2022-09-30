#!/usr/bin/python3

import pandas as pd
import pyodbc
import glob
import os
from fnmatch import fnmatch
from datetime import date
import mysql.connector
from mysql.connector import errorcode

root = os.path.dirname(os.path.realpath(__file__))
print(root)
cfile = root + "/cfg/config"
cfg = open(cfile)

for line in cfg:
    f = line.find("db_host")
    if f == 0:
        vHost = line[line.index(":") + 1:100]
    f = line.find("db_name:")
    if f == 0:
        vDBName = line[line.index(":") + 1:30]
    f = line.find("userid:")
    if f == 0:
        vUserID = line[line.index(":") + 1:30]
    f = line.find("pwd:")
    if f == 0:
        vPWD = line[line.index(":") + 1:20]
    f = line.find("pattern:")
    if f == 0:
        vPattern = line[line.index(":") + 1:20]

Pattern1 = "Csv-report_Summary.csv"
Pattern2 = "Csv-report.csv"
Pattern3 = "Csv-report-Action-Items-Summary.csv"
Pattern4 = "Aggregated_report.csv"
sourceengine = "MSSQL"
targetengine = "MSSQL"


# Connect to RDS instance
try:
    # Connect to SQL Server
    connDB = pyodbc.connect('Driver={SQL Server};'
                            'Server=sqlserver-bb8.ccuwkrymeppv.us-east-1.rds.amazonaws.com;'
                            'Database=sctdb;'
                            'UID=admin;'
                            'PWD=Aws123$;')

except pyodbc.Error as err:
    print("Unable to connect to SCT database!")


for path, subdirs, files in os.walk(root):
    for name in files:
        if fnmatch(name, Pattern1):
            SCTFile = (os.path.join(path, name))
            print(SCTFile)

            p1 = SCTFile.index('reports')
            p2 = SCTFile.index('csv')
            sysname = (SCTFile[p1:100])
            nametab = sysname.split('\\')
            hostname = nametab[2]
            # vEnd = instance.index('-')
            # instance=instance[0:vEnd]
            # print (hostname)
            dbname = nametab[4]
            vEnd = dbname.index('.')
            dbname = dbname[0:vEnd]
            schemaname = nametab[4]
            vStart = schemaname.index('.') + 1
            schemaname = schemaname[vStart:]

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
            # print(ReportDate)

            feature_1_sts = 'False'
            feature_2_sts = 'False'
            feature_3_sts = 'False'

            inFile = open(SCTFile, "rb")
            # reader = csv.reader(codecs.iterdecode(inFile, 'utf-8'))
            reader = inFile.readlines()

            for row in reader:

                str = row.decode()
                if feature_1 in str:
                    feature_1_sts = 'True'

                if feature_2 in str:
                    feature_2_sts = 'True'

                if feature_3 in str:
                    feature_3_sts = 'True'

                if source_db in str:
                    # print(str)
                    pos0 = str.index('@') + 1
                    pos1 = str.index('"')
                    instance = (str[pos0:-1])
                    instance = instance.translate({ord('"'): None})
                    print("Instance/DBName/Schema: ", (instance, dbname, schemaname))
                # hostname=instance

                if SQLversion in str:
                    pos = str.index(' - ')
                    Version = (str[21:pos])
                    # print (Version)

                if OSVersion in str:
                    pos0 = str.index(OSVersion)
                    pos1 = str.index('<')
                    OSVer = (str[pos0:pos1])
                    # print(OSVer)
                    if 'Datacenter Edition' in str:
                        SQLEdition = 'Datacenter Edition'

                    if 'Enterprise Edition' in str:
                        SQLEdition = 'Enterprise Edition'

                    if 'Standard Edition' in str:
                        SQLEdition = 'Standard Edition'

                    if 'Developer Edition' in str:
                        SQLEdition = 'Developer Edition'

                    if 'Web Edition' in str:
                        SQLEdition = 'Web Edition'

            inFile.close()


#######################################################################
# Read Report Summary - Csv-report_Summary
#######################################################################

            data = pd.read_csv(SCTFile, nrows=8, header=0)
            df = pd.DataFrame(data, columns=['Category', 'Number of objects', 'Objects automatically converted',
                                             'Objects with simple actions', 'Objects with medium-complexity actions',
                                             'Objects with complex actions', 'Total lines of code'])
            df.columns = ['Category', 'Numberofobjects', 'ObjAutoConverted', 'ObjSimpleActions', 'ObjWithMediumActions',
                          'ObjComplexActions', 'TotalLinesCode']

            df = df.fillna(value=0)

            # Delete if Instance already exists
            cursor = connDB.cursor()
            try:
                cursor.execute('''
                               DELETE FROM sct_report_summary WHERE Hostname= ? and InstanceName = ? and SchemaName = ?
                               ''',
                               hostname, instance, schemaname
                               )

                connDB.commit()
            except pyodbc.Error as err:
                print("Unable to delete from sct_report_summary!")

            # Insert into the RDS database

            for row in df.itertuples():

                # Insert new report

                try:

                    cursor.execute('''INSERT INTO sct_report_summary
                    (InstanceName,InstanceVersion,SQLEdition,OSVersion,Hostname,
                    DBName,SchemaName,Category,Numberofobjects,ObjAutoConverted,
                    ObjSimpleActions,ObjWithMediumActions,ObjComplexActions,
                    TotalLinesCode,CompressionEnabled,PartitionEnabled,EncryptionEnabled,ReportDate)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                                   instance, Version, SQLEdition, OSVer, hostname,
                                   dbname, schemaname, row.Category, row.Numberofobjects, row.ObjAutoConverted,
                                   row.ObjSimpleActions, row.ObjWithMediumActions, row.ObjComplexActions,
                                   row.TotalLinesCode, feature_1_sts, feature_2_sts, feature_3_sts, ReportDate)

                    connDB.commit()

                except pyodbc.Error as err:
                    print(err)
                    # print("Error inserting new SCT reports into sct_report_summary!")

                # Check Server Inconsistencies file
                for path, subdirs, files in os.walk(root):
                    for name in files:
                        if fnmatch(name, Pattern2):
                            SCTFile = (os.path.join(path, name))
                            # print(SCTFile)
                            data = pd.read_csv(SCTFile, header=0)
                            df = pd.DataFrame(data,
                                              columns=["Category", "Occurrence", "Action item", "Subject", "Group",
                                                       "Description",
                                                       "Documentation references", "Recommended action", "Filtered",
                                                       "Estimated complexity"])
                            df.columns = ['Category', 'Ocurrence', 'ActionItem', 'Subject', 'Group1', 'Description',
                                          'DocumentationReferences', 'RecommendedAction', 'Filtered',
                                          'EstimatedComplexity']

                            df = df.fillna(value=0)

                            for row in df.itertuples():

                                # Delete if Instance already exists
                                try:
                                    cursor = connDB.cursor()

                                    cursor.execute('''
                                                   DELETE FROM sct_report_server_level WHERE Hostname= ? and InstanceName = ?
                                                   ''',
                                                   hostname, instance
                                                   )

                                    connDB.commit()
                                except pyodbc.Error as err:
                                    print("Unable to delete previous reports from sct_report_server_level.")

                                # Insert server level informaiton
                                try:
                                    cursor.execute('SET ANSI_WARNINGS  OFF')
                                    cursor.execute('''INSERT INTO sct_report_server_level
                                                  (InstanceName,Hostname,Category,Ocurrence,
                                                   ActionItem,Subject,Group1,Description,
                                                   DocumentationReferences,RecommendedAction,Filtered,EstimatedComplexity)
                                                  values
                                                  (?,?,?,?,?,?,?,?,?,?,?,?)''',
                                                   instance, hostname, row.Category, row.Ocurrence,
                                                   row.ActionItem, row.Subject, row.Group1,
                                                   row.Description, row.DocumentationReferences,
                                                   row.RecommendedAction, row.Filtered,
                                                   row.EstimatedComplexity)

                                    connDB.commit()

                                except pyodbc.Error as err:
                                    print("Error inserting new SCT reports into RDS database!")
                                    # print (err)


##################################################################################################################
    # Action Items Report  -Csv-report_Action_Items_Summary
##################################################################################################################

            for path, subdirs, files in os.walk(root):
                for name in files:
                    if fnmatch(name, Pattern3):
                        SCTFile = (os.path.join(path, name))
                        print(SCTFile)
                        data = pd.read_csv(SCTFile, header=0)

                        df = pd.DataFrame(data, columns=['Schema','Action item','Number of occurrences',
                                                             'Learning curve efforts',
                                                             'Efforts to convert an occurrence of the action item'])

                        df.columns = ["fSchema","ActionItem","NumberofOccurences",
                                      "LearningCurveEfforts",
                                      "EffortsToConvert"]

                        df = df.fillna(value=0)


                        for row in df.itertuples():

                            schema = row.fSchema
                            pos1 = schema.index('.')
                            dbname = schema[0:pos1]
                            schema = schema[pos1+1:]
                            actionitem = row.ActionItem

                            # Delete if Instance already exists
                            try:
                                cursor = connDB.cursor()

                                cursor.execute('''
                                                   DELETE FROM sct_report_action_items
                                                   WHERE
                                                         DBName = ? AND
                                                         SchemaName = ? AND
                                                         ActionItem = ?
                                                   ''',
                                               dbname, schema, actionitem
                                               )
                                connDB.commit()
                            except pyodbc.Error as err:
                                print("Unable to delete previous reports from sct_aggregated_report!")
                                connDB.commit()

                            # Insert server level information
                            try:
                                cursor.execute('''INSERT INTO sct_report_action_items
                                               (SourceEngine, TargetEngine, InstanceName,DBName, SchemaName,
                                                ActionItem,NumberofOcurrence,
                                                LearningCurveEfforts,
                                                EffortsToConvert)
                                              values
                                              (?,?,?,?,?,?,?,?,?)''',
                                                'MSSQL','AURORA_POSTGRSQL', instance, dbname, schema,
                                                row.ActionItem,
                                                row.NumberofOccurences,
                                                row.LearningCurveEfforts,
                                                row.EffortsToConvert)

                                connDB.commit()

                            except pyodbc.Error as err:
                                print("Unable to insert into sct_report_action_items!")


##################################################################################################################
    # Aggregated Report
##################################################################################################################

for path, subdirs, files in os.walk(root):
    for name in files:
        if fnmatch(name, Pattern4):
            SCTFile = (os.path.join(path, name))
            print(SCTFile)
            data = pd.read_csv(SCTFile, header=0)

            df = pd.DataFrame(data, columns=['Server Ip','Name','Description','Schema name',
                                             'Code object conversion % for "Amazon RDS for Microsoft SQL Server"','Storage object conversion % for "Amazon RDS for Microsoft SQL Server"',
                                             'Syntax Elements conversion % for "Amazon RDS for Microsoft SQL Server"','Conversion Complexity "Amazon RDS for Microsoft SQL Server"'])

            df.columns = ["ServerIp","Name","Description","a_SchemaName",
                          "CodeObjectConversion","StorageObjectConversion",
                          "SyntaxElementsConversion","ConversionComplexity"]

            df = df.fillna(value=0)

            for row in df.itertuples():

                schema = row.a_SchemaName
                pos1 = schema.index('.')
                a_dbname = schema[0:pos1]
                schema = schema[pos1+1:]
                #print(schema)
                #print(a_dbname)

                # Delete if Instance already exists
                try:
                    cursor = connDB.cursor()

                    cursor.execute('''
                                       DELETE FROM sct_aggregated_report
                                       WHERE InstanceName = ? AND
                                             DBName = ? AND
                                             SchemaName = ? AND
                                             TargetEngine = ?
                                       ''',
                    row.ServerIp, a_dbname, schema, targetengine
                    )
                    connDB.commit()

                except pyodbc.Error as err:
                    print("Unable to delete previous reports from sct_aggregated_report!")
                    connDB.commit()

                try:
                    cursor.execute('''INSERT INTO sct_aggregated_report
                                   (SourceEngine, TargetEngine, InstanceName,DBName, SchemaName,
                                    Name,Description,
                                    CodeObjectConversion,StorageObjectConversion,
                                    SyntaxElementsConversion,ConversionComplexity)
                                  values
                                  (?,?,?,?,?,?,?,?,?,?,?)''',
                                    sourceengine, targetengine, row.ServerIp, a_dbname, schema, '', '',
                                    row.CodeObjectConversion, row.StorageObjectConversion,
                                    row.SyntaxElementsConversion, row.ConversionComplexity)

                    connDB.commit()

                except pyodbc.Error as err:
                    print("Unable to insert into  sct_aggregated_report!")
