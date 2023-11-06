import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base import BaseHook
import pandas as pd
from sqlalchemy import create_engine

#extract tasks
@task()
def get_src_tables():
    hook = MySqlHook(mysql_conn_id="sqlserver1")
    sql = "SELECT * FROM 'lkpaccount'"
    df = hook.get_pandas_df(sql)
    print(df)
    tbl_dict = df.to_dict('dict')
    return tbl_dict

#Transformation tasks
@task()
def transform_1(): #srcProduct
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pdf = pd.read_sql_query('SELECT * FROM public."src_DimProduct" ', engine)
    #drop columns
    revised = pdf[['ProductKey', 'ProductAlternateKey', 'ProductSubcategoryKey','WeightUnitMeasureCode', 'SizeUnitMeasureCode', 'EnglishProductName',
                   'StandardCost','FinishedGoodsFlag', 'Color', 'SafetyStockLevel', 'ReorderPoint','ListPrice', 'Size', 'SizeRange', 'Weight',
                   'DaysToManufacture','ProductLine', 'DealerPrice', 'Class', 'Style', 'ModelName', 'EnglishDescription', 'StartDate','EndDate', 'Status']]
    #replace nulls
    revised['WeightUnitMeasureCode'].fillna('0', inplace=True)
    revised['ProductSubcategoryKey'].fillna('0', inplace=True)
    revised['SizeUnitMeasureCode'].fillna('0', inplace=True)
    revised['StandardCost'].fillna('0', inplace=True)
    revised['ListPrice'].fillna('0', inplace=True)
    revised['ProductLine'].fillna('NA', inplace=True)
    revised['Class'].fillna('NA', inplace=True)
    revised['Style'].fillna('NA', inplace=True)
    revised['Size'].fillna('NA', inplace=True)
    revised['ModelName'].fillna('NA', inplace=True)
    revised['EnglishDescription'].fillna('NA', inplace=True)
    revised['DealerPrice'].fillna('0', inplace=True)
    revised['Weight'].fillna('0', inplace=True)
    # Rename columns with rename function
    revised = revised.rename(columns={"EnglishDescription": "Description", "EnglishProductName":"ProductName"})
    revised.to_sql(f'stg_DimProduct', engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported successful"}

#
@task()
def load_src_data(tbl_dict: dict):
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    all_tbl_name = []
    start_time = time.time()
    #access the table_name element in dictionaries
    for k, v in tbl_dict['table_name'].items():
        #print(v)
        all_tbl_name.append(v)
        rows_imported = 0
        sql = f'select * FROM {v}'
        hook = MySqlHook(mssql_conn_id="sqlserver")
        df = hook.get_pandas_df(sql)
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {v} ')
        df.to_sql(f'src_{v}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print(f'Done. {str(round(time.time() - start_time, 2))} total seconds elapsed')
    print("Data imported successful")
    return all_tbl_name

#load
@task()
def model(): #prdProduct
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pc = pd.read_sql_query('SELECT * FROM public."stg_DimProductCategory" ', engine)
    p = pd.read_sql_query('SELECT * FROM public."stg_DimProduct" ', engine)
    p['ProductSubcategoryKey'] = p.ProductSubcategoryKey.astype(float)
    p['ProductSubcategoryKey'] = p.ProductSubcategoryKey.astype(int)
    ps = pd.read_sql_query('SELECT * FROM public."stg_DimProductSubcategory" ', engine)
    #join all three
    merged = p.merge(ps, on='ProductSubcategoryKey').merge(pc, on='ProductCategoryKey')
    merged.to_sql(f'prd_DimProductCategory', engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported successful"}


# [START]
with DAG(dag_id="etl_dag",schedule_interval="0 9 * * *", start_date=datetime(2023, 10, 31),catchup=False,  tags=["product_model"]) as dag:

    with TaskGroup("extract_load", tooltip="Extract and load source data") as extract_load_src:
        src_product_tbls = get_src_tables()
        src_product_tbls

    with TaskGroup("transform_src", tooltip="Transform and stage data") as transform_src_product:
        transform_1 = transform_1()
        #define task order
        transform_1

    with TaskGroup("load_model", tooltip="Final Product model") as load_product_model:
        prd_Product_model = model()
        #define order
        prd_Product_model

    with TaskGroup("load_hdfs", tooltip="Final model") as load_hdfs:
        prd_Product_model = model()
        #define order
        prd_Product_model

    extract_load_src >> transform_src_product >> load_product_model >> load_hdfs