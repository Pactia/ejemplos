import boto3
from dotenv import load_dotenv
from os.path import join, dirname
import pandas as pd
import time 
import csv
from athena_s3 import query_results
from clean_s3 import clean_up
from mysql_con import create_conn



dotenv_path = join(dirname(__file__), ".env")
load_dotenv(dotenv_path)  

athena = boto3.client("athena")
data_catalogs = [cata["CatalogName"] for cata  in 
                 athena.list_data_catalogs()["DataCatalogsSummary"]]
print(data_catalogs)

databases_names = athena.list_databases(CatalogName = data_catalogs[0])
print(databases_names)

tables = athena.list_table_metadata(CatalogName = data_catalogs[0], 
                                          DatabaseName = databases_names["DatabaseList"][0]["Name"])

print(tables)
tables_names = [table["Name"] for table in tables["TableMetadataList"]]
print("Nombre tablas:")
print(tables_names)




BUCKET_NAME = "pactia-out-athena"

params = {
    'region': 'us-east-1',
    'database': "datalake_pactia",
    'bucket': BUCKET_NAME,
    'path': 'temp/athena/output',
    'query': """SELECT * FROM "datalake_pactia"."ve_inflacion" \
        where fecha = CAST('2020-10-01' as date)  limit 10;"""
}


### Consumo Athena

session = boto3.Session()
location, data = query_results(session, params)
#df = pd.read_json(data)
print("Locations: ",location)
print("Result Data: ")
print("Consumo desde Athena")
print(data)
print(pd.read_csv(location))
#print(df)

clean_up(BUCKET_NAME)

#### Mysql consumo 

QUERY = "select * from ve_inflacion where fecha ='2020-10-01';"
data2 = pd.read_sql(QUERY, con = create_conn())
print("Consumo desde SQL")
print(data2)

#### S3 puro 

s3 = boto3.client("s3")
s3.download_file("pactia-datalake-dwh", "variables_externas/inflacion/2021-01-15.parquet", 
                 "inflacion.parquet")
data_3 = pd.read_parquet("inflacion.parquet")
print("Consumo usando api S3")
print(data_3)



