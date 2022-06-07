def run_full_load():
    from datetime import datetime, timedelta

    #from custom.MySqlToPostgreOperator import MySqlToPostgreOperator
    import json
    from os import environ

    from base64 import b64decode
    from datetime import datetime
    from logging import getLogger, INFO, WARN
    from os import environ
    from sys import exit as sys_exit
    from sqlalchemy import create_engine
    import io
    import psycopg2
    import csv
    from pandas import read_sql_table

    from psycopg2.extensions import register_adapter
    from psycopg2.extras import Json
    from psycopg2 import connect
    import pandas as pd
    import warnings

    import psycopg2
    import csv
    import io
    #from tkinter.messagebox import QUESTION
    import mysql.connector
    import pandas as pd
    import os
    import numpy as np
    import time
    import io
    import csv

    import requests
    import os 
    from dotenv import load_dotenv
    # Logging




    # os.chdir('include')
    
    # load_dotenv('enviroment_variables.env')
    warnings.filterwarnings("ignore")
    # Logging
    logger = getLogger()
    logger.setLevel('INFO')


    
    
    pg_host =  os.getenv('PG_HOST_STAGING')
    pg_user = os.getenv('PG_USERNAME_WRITE_STAGING')
    pg_password = os.getenv('PG_PASSWORD_WRITE_STAGING')
    # pg_database =os.getenv('pg_connect_string')


    pg_database = os.getenv('PG_DATABASE')
    pg_schema = os.getenv('PG_RAW_SCHEMA')
    pg_tables_to_use = os.getenv('PG_ALL_SKUS')
   
    pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}/{pg_schema}"
    #print(pg_connect_string)
    pg_engine = create_engine(f"{pg_connect_string}", echo=False)
    chunk_size = 1000  # os.getenv('CHUNK_SIZE')
    

    mysql_host =  os.getenv('MYSQL_HOST')
    mysql_port =  os.getenv('MYSQL_PORT')
    mysql_schema = os.getenv('MYSQL_DATABASE_akeneo')
    mysql_user = os.getenv('MYSQL_USERNAME')

    mysql_password = os.getenv('MYSQL_PASSWORD')


    mysql_connect_string = f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_schema}"
    mysql_engine = create_engine(f"{mysql_connect_string}", echo=False)



    ########################################################################
    ################################ Reading the product tables from Akeneo
    df_product = pd.read_sql("""
                                    select distinct pcp.*
                                                , pcp.raw_values                                     raw_values_product
                                                , pcpm.raw_values                                    raw_values_model
                                                , pcft.label         as                family
                                                , sku
                                                , base_unit_content
                                                , base_unit_content_uom
                                                , no_of_base_units
                                                , gtin
                                                , kollex_active
                                                , manufacturer
                                                , sales_unit_pkgg
                                                , name
                                                 ,pcpm.code as l1_code
                                                , case when active is null then 0 else active end as "active"


                                    from akeneo.pim_catalog_product pcp
                                            left join akeneo.pim_catalog_product_model pcpm
                                                    on pcp.product_model_id = pcpm.id
                                            left join (select max(sku)                   as sku,
                                                            MAX(base_unit_content)     as base_unit_content,
                                                            MAX(base_unit_content_uom) as base_unit_content_uom,
                                                            MAX(no_of_base_units)         no_of_base_units,
                                                            MAX(gtin)                  as gtin,

                                                            MAX(kollex_active)         as kollex_active,
                                                            MAX(manufacturer)          as manufacturer,


                                                            MAX(refund_value)          as refund_value,


                                                            MAX(sales_unit_pkgg)       as sales_unit_pkgg,
                                                            MAX(name)                  as name,
                                                            MAX(active)                as active,
                                                            MAX(category_code)            category_code,

                                                            MAX(direct_shop_release)      direct_shop_release

                                                        from gfghdata.product
                                                        where sku is not null
                                                        group by sku) as gfghproduct on gfghproduct.sku = pcp.identifier
                                    left join akeneo.pim_catalog_family_translation pcft on pcp.family_id =  pcft.foreign_key

                                    """
                                            , con=mysql_engine
                                            , chunksize=chunk_size)
    chunk = pd.DataFrame()
    #print("finished reading Akeneo Data and will Start processing now")





    ################################################################
    ######################### dropping table of all skus for cleanup




    # pg_host = os.getenv('PG_HOST')
    pg_database = os.getenv('PG_DATABASE')
    #pg_schema = os.getenv('PG_SCHEMA_Junk')  # os.getenv('PG_SCHEMA')
    # pg_user = os.getenv('PG_USERNAME_WRITE')

    # pg_password = os.getenv('PG_PASSWORD_WRITE')s
    # pg_tables_to_use = 'all_skus_2'

    pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    pg_engine = create_engine(f"{pg_connect_string}", echo=False)

    connection = pg_engine.connect()

    connection.execute(f"drop table if exists {pg_schema}.{pg_tables_to_use};")
    #print("Finished cleaning the table in the DWH")
    #df_product = pd.read_sql("select  * from akeneo.pim_catalog_product limit 100", con=mysql_engine)
    count = 0

    ###################################################
    ###################### Extracting Active Merchants  

    pg_connect_string = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    pg_engine = create_engine(f"{pg_connect_string}", echo=False)


    merchants_active= pd.read_sql_table('merchants_all', con=pg_engine,schema=os.getenv('PG_RAW_SCHEMA'))
    merchants_active = merchants_active[~merchants_active["merchant_key"].str.contains('test',na=False)]
    merchants_active = merchants_active[merchants_active["merchant_key"]!='trinkkontor']
    merchants_active = merchants_active[merchants_active["merchant_key"]!='trinkkontor_trr']

    for df_chunk in df_product:
        chunk = df_chunk
        print("######################################Processing Chunk Number "+str(count))
        
        


        
        #chunk['raw_values_json'] =  chunk['raw_values'].apply(json.dumps)


        ####################################################################################################
        # extracting values from the JSON for each SKU from product and product model JSON fields raw_values


        chunk['manufacturer_name'] = chunk['raw_values_model'].apply(lambda x: '|'+json.loads(x)
                                                                                    .get('manufacturer_name')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>')+"|" if x is not None and json.loads(x)
                                                                                                                .get('manufacturer_name') is not None 
                                                                                    else None).astype(str)
        chunk['manufacturer_name_2'] = chunk['raw_values_product'].apply(lambda x: '|'+json.loads(x)
                                                                                    .get('manufacturer_name')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>')+"|" if x is not None and json.loads(x)
                                                                                                                .get('manufacturer_name') is not None 
                                                                                    else None).astype(str)
        chunk['manufacturer_name_fixtest'] = chunk['raw_values_model'].apply(lambda x: '|'+json.loads(x)
                                                                                    .get('manufacturer_name')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>')+"|" if x is not None and json.loads(x)
                                                                                                                .get('manufacturer_name') is not None 
                                                                                    else None).astype(str)
        chunk['amount_single_unit'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('amount_single_unit')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('amount_single_unit') is not None 
                                                                                    else None).astype(str)
        chunk['type_single_unit'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('type_single_unit')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('type_single_unit') is not None 
                                                                                    else None).astype(str)
        chunk['golden_record_level1'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('golden_record_level1')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('golden_record_level1') is not None 
                                                                                    else None).astype(str)
        chunk['shop_enabled'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                                    .get('shop_enabled')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('shop_enabled') is not None 
                                                                                    else None).astype(str)

        chunk['gtin_single_unit'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('gtin_single_unit')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('gtin_single_unit') is not None 
                                                                                    else None).astype(str)
        chunk['gtin_packaging_unit'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                                    .get('gtin_packaging_unit')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('gtin_packaging_unit') is not None 
                                                                                    else None).astype(str)
        chunk['detail_type_single_unit'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('detail_type_single_unit')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('detail_type_single_unit') is not None 
                                                                                    else None).astype(str)
        chunk['detail_type_packaging_unit'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('detail_type_packaging_unit')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('detail_type_packaging_unit') is not None 
                                                                                    else None).astype(str)

        chunk['release_l1'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('release_l1')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('release_l1') is not None 
                                                                                    else None).astype(str)
        chunk['foto_release_hash'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('foto_release_hash')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('foto_release_hash') is not None 
                                                                                    else None).astype(str)


        chunk['shop_enabled'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                                    .get('shop_enabled')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('shop_enabled') is not None 
                                                                                    else None).astype(str)


        chunk['status_base'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('status_base')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('status_base') is not None 
                                                                                    else None).astype(str)
        chunk['is_manual'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('code')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('code') is not None 
                                                                                    else None).astype(str)



        chunk['is_manual'] = chunk['is_manual'].apply(lambda x: True if x is not None and 'm-' in x else False)



        chunk['contact_info'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('contact_info')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('contact_info') is not None 
                                                                                    else None).astype(str)

        chunk['net_content_uom'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                                    .get('net_content_uom')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('net_content_uom') is not None 
                                                                                    else None).astype(str)
        chunk['structure_packaging_unit'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('structure_packaging_unit')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('structure_packaging_unit') is not None 
                                                                                    else None).astype(str)

        chunk['title'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                                    .get('brand_and_title')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('brand_and_title') is not None 
                                                                                    else None).astype(str)

        chunk['brand'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                                    .get('brand_and_title')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('brand_and_title') is not None 
                                                                                    else None).astype(str)
        chunk['net_content'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                                    .get('net_content')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('net_content') is not None 
                                                                                    else None).astype(str)


        chunk['net_content_liter'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                                    .get('net_content_liter')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('net_content_liter') is not None 
                                                                                    else None).astype(str)


        chunk['contact_info_2'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                                    .get('contact_info')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('contact_info') is not None 
                                                                                    else None).astype(str)


        chunk['net_content_uom_2'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('net_content_uom')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('net_content_uom') is not None 
                                                                                    else None).astype(str)


        chunk['structure_packaging_unit_2'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                                    .get('structure_packaging_unit')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('structure_packaging_unit') is not None 
                                                                                    else None).astype(str)
        chunk['title_2'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                                    .get('title')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('title') is not None 
                                                                                    else None).astype(str)
        chunk['base_code'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                            .get('shop_enabled')
                                                                            .get('<all_channels>')
                                                                            .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                        .get('code') is not None 
                                                                            else None)

        chunk['brand_2'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('brand')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('brand') is not None 
                                                                                    else None).astype(str)
        chunk['net_content_2'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('net_content')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('net_content') is not None 
                                                                                    else None).astype(str)
        chunk['net_content_liter_2'] = chunk['raw_values_model'].apply(lambda x: json.loads(x)
                                                                                    .get('net_content_liter')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if x is not None and json.loads(x)
                                                                                                                .get('net_content_liter') is not None 
                                                                                    else None).astype(str)                                                                            




        chunk['type_packaging_unit'] = chunk['raw_values_product'].apply(lambda x: json.loads(x)
                                                                                    .get('type_packaging_unit')
                                                                                    .get('<all_channels>')
                                                                                    .get('<all_locales>') if json.loads(x)
                                                                                                                .get('type_packaging_unit') is not None 
                                                                                    else None).astype(str)
        #print("Finished Extracting JSON Values from table")
        # print(chunk['type_packaging_unit'])
        # chunk.to_excel('chunk_dump.xlsx')




        ###################################################################################################################
        ############################## Consolidating Values of Title,Brand,Net_content_liter,Contact_info into one column
        print("Consolidating Those columns")
        chunk['title'] = chunk['title'].combine_first(chunk['title_2'])
        chunk['net_content'] = chunk['net_content'].combine_first(chunk['net_content_2'])
        chunk['brand'] = chunk['brand'].combine_first(chunk['brand_2'])
        chunk['net_content_liter'] = chunk['net_content_liter'].combine_first(chunk['net_content_liter_2'])
        chunk['contact_info'] = chunk['contact_info'].combine_first(chunk['contact_info_2'])

        chunk.drop(['title_2','net_content_2','brand_2','net_content_liter_2','contact_info_2'],axis=1,inplace=True)
        ##print("fnished Consolidating Those columns")


        ###############################################
        ######################### adding Pim categories
        print("extracting SKU Fact Consolidating Those columns")
        
        pg_connect_string = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
        pg_engine = create_engine(f"{pg_connect_string}", echo=False)
        # chunk_size = 1000  # os.getenv('CHUNK_SIZE')


        sku_category_fact = pd.read_sql_table(
            'sku_category_fact', con=pg_engine, schema=os.getenv('PG_INFO_SCHEMA'))
        chunk = chunk.merge(sku_category_fact, how='inner',
                        left_on='identifier', right_on='sku',suffixes=('', '_y'))
        chunk.drop(chunk.filter(regex='_y$').columns.tolist(),axis=1, inplace=True)
        #print("finished extracting SKU Fact Consolidating Those columns")



        #####################################################################################
        ######################### Translating attributes through attribute  values  in Akeneo
        ##print("Translating values from Attribute options")

        attribute_options = pd.read_sql(
            """select distinct code,value from pim_catalog_attribute_option 
                left join pim_catalog_attribute_option_value   
                    on pim_catalog_attribute_option.id = pim_catalog_attribute_option_value.option_id""", con=mysql_engine)


        chunk = chunk.merge(
            attribute_options, left_on='net_content_liter', right_on='code', how='left')
        chunk['net_content_liter'] = chunk['value'].combine_first(chunk['net_content_liter']).astype(str)
        chunk.drop(attribute_options.columns, inplace=True, axis=1)


        chunk = chunk.merge(
            attribute_options, left_on='net_content_uom', right_on='code', how='left')
        chunk['net_content_uom'] =  chunk['value'].combine_first(chunk['net_content_uom']).astype(str)
        chunk.drop(attribute_options.columns, inplace=True, axis=1)


        chunk = chunk.merge(
            attribute_options, left_on='amount_single_unit', right_on='code', how='left',)
        chunk['amount_single_unit'] = chunk['value'].combine_first(chunk['amount_single_unit']).astype(str)
        chunk.drop(attribute_options.columns, inplace=True, axis=1)


        chunk = chunk.merge(
            attribute_options, left_on='structure_packaging_unit', right_on='code', how='left')
        chunk['structure_packaging_unit'] = chunk['value'].combine_first(chunk['structure_packaging_unit']).astype(str)
        chunk.drop(attribute_options.columns, inplace=True, axis=1)


        chunk = chunk.merge(
            attribute_options, left_on='type_packaging_unit', right_on='code', how='left')
        chunk['type_packaging_unit'] = chunk['value'].combine_first(chunk['type_packaging_unit']).astype(str)
        chunk.drop(attribute_options.columns, inplace=True, axis=1)


        chunk = chunk.merge(
            attribute_options, left_on='golden_record_level1', right_on='code', how='left')
        chunk['golden_record_level1'] = chunk['value'].combine_first(chunk['golden_record_level1']).astype(str)
        chunk.drop(attribute_options.columns, inplace=True, axis=1)


        chunk = chunk.merge(
            attribute_options, left_on='status_base', right_on='code', how='left')
        chunk['status_base'] = chunk['value'].combine_first(chunk['status_base']).astype(str)
        chunk.drop(attribute_options.columns, inplace=True, axis=1)


        chunk = chunk.merge(
            attribute_options, left_on='brand', right_on='code', how='left')
        chunk['brand'] = chunk['value'].combine_first(chunk['brand']).astype(str)
        chunk.drop(attribute_options.columns, inplace=True, axis=1)


        chunk = chunk.merge(
            attribute_options, left_on='amount_single_unit', right_on='code', how='left')
        chunk['amount_single_unit'] = chunk['value'].combine_first(chunk['amount_single_unit']).astype(str)
        chunk.drop(attribute_options.columns, inplace=True, axis=1)


        chunk = chunk.merge(
            attribute_options, left_on='net_content_uom', right_on='code', how='left')
        chunk['net_content_uom'] = chunk['value'].combine_first(chunk['net_content_uom']).astype(str)
        chunk.drop(attribute_options.columns, inplace=True, axis=1)


        #print("Finished Translating values from Attribute options")




        ###################################################
        ###################### Extracting Merchant Info 

        #print("creating merchant Columns")
        for merchant in merchants_active['merchant_key']:
            #chunk[str(merchant)] = chunk['raw_values_product'].apply(lambda x :json.loads(x)['gfgh_'+str(merchant)+'_enabled']['<all_channels>']['<all_locales>'] if 'gfgh_'+str(merchant)+'_id' in json.dumps(x) else False).astype(str)
            chunk[str(merchant)+'_id'] = chunk['raw_values_product'].apply(lambda x :json.loads(x)['gfgh_'+str(merchant)+'_id']['<all_channels>']['<all_locales>'] if 'gfgh_'+str(merchant)+'_id' in json.dumps(x) else None).astype(str)
            chunk[str(merchant)+'_enabled'] = chunk['raw_values_product'].apply(lambda x :json.loads(x)['freigabe_'+str(merchant)+'_id']['<all_channels>']['<all_locales>'] if 'freigabe_'+str(merchant)+'_id' in json.dumps(x) else None).astype(str)
            ##print(merchant)


        #################################    
        ##### Counting Enabled SKUs
        number_of_merchants = merchants_active['merchant_key'].size
        enabelment_columns  = [col for col in chunk.columns if '_enabled' in col]
        enabled_df = chunk[enabelment_columns]
        enabled_df['enablement']=enabled_df[enabled_df == 'True'].count(axis=1)-2
        chunk['enablement'] = enabled_df['enablement']
        #print("finished creating merchant Columns")

        #################################
        ######## Droping the JSON columns
        print("Droping JSON Columns")
        chunk.drop(['raw_values_model','raw_values','raw_values_product'],axis=1,inplace=True)
        chunk.drop([ 'family_id', 'product_model_id', 'family_variant_id'],axis=1,inplace=True)



        # chunk.to_csv('dump_chunk.csv')
        ####################################################
        ######################### Writing the results in DWH 
        #print("Writing to the DWH")

        chunk.drop('is_enabled',axis=1,inplace=True)
        # pg_schema = os.getenv('PG_SCHEMA_Junk')
        pg_tables_to_use =os.getenv('PG_ALL_SKUS')


        # if count==0 :
        #     postgres_conn = psycopg2.connect(   host=pg_host,
        #                                     user=pg_user
        #                                 ,   password=pg_password
        #                                     ,database=pg_database
        #                                     ,options="-c search_path=prod_raw_layer")
        #     Postgres_cursor = postgres_conn.cursor()
        #      #drops old table and creates new empty table
        #     Postgres_cursor.execute("DROP TABLE IF EXISTS {}.{}".format(pg_schema,pg_tables_to_use))
        chunk.to_sql(pg_tables_to_use, pg_engine,schema=pg_schema, if_exists='append',index=False)
        
        print("##############Finished Writing to the DWH#############")
        count+=1