

def Name_matching():


  from pandas import read_sql_table
  from sqlalchemy import create_engine, types
  import pandas as pd
  from airflow.models import Variable
  # Logging


  pg_host =  Variable.get("PG_HOST")
  pg_user =  Variable.get("PG_USERNAME_WRITE")
  pg_password =  Variable.get("PG_PASSWORD_WRITE")



  pg_database = Variable.get("PG_DATABASE")
  pg_schema = Variable.get("PG_RAW_SCHEMA")

  pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
  pg_engine = create_engine(f"{pg_connect_string}", echo=False, pool_pre_ping=True, pool_recycle=36000)
  chunk_size = 2000  # environ.get('CHUNK_SIZE')


  connection = pg_engine.connect()

  connection.execute(f"drop table if exists {pg_schema}.result_name_matching;")



  df_product = pd.read_sql("""
                                    select concat( title , ' ' , brand , ' ' , amount_single_unit , 'x' , net_content_liter ) as name_allskus
                                          , identifier

                                          , base_unit_content::text         as "base_unit_content_allskus"
                                          , no_of_base_units::text          as "no_of_base_units_allskus"

                                        from prod_raw_layer.all_skus
                                        
                           """
 , con=pg_engine)


  products_to_identify = pd.read_sql_table('input_data_for_name_matching',schema='sheet_loader', con=pg_engine)





  from fuzzywuzzy import fuzz
  from fuzzywuzzy import process






  CHUNK_SIZE = 50
  df_to_write = pd.DataFrame()
  for chunk_num in range(len(products_to_identify) // CHUNK_SIZE + 1):
    start_index = chunk_num*CHUNK_SIZE
    end_index = min(chunk_num*CHUNK_SIZE + CHUNK_SIZE, len(df_product))
    chunk = products_to_identify[start_index:end_index]


    # .. do calculaton on chunk here ..
    print(str(start_index)+"  "+str(end_index))
    df_product_joined = df_product.merge(chunk,how='cross')


    print("finished getting all SKUs from DB")



    df_product_joined['name_similarity'] = df_product_joined.apply(lambda x: \
                                                    fuzz.ratio \
                                                            (x['name_allskus'] \
                                                          ,  x['name']), axis=1)
    df_product_joined = df_product_joined[(df_product_joined['name_similarity']>= 70)]

    print("finished name_similarity and size is "+str(len(df_product_joined.index)))
    if (len(df_product_joined.index)==0):
      continue
    df_product_joined['base_unit_content_similarity'] = df_product_joined.apply(lambda x: \
                                                                  fuzz.token_set_ratio
                                                                  ( 
                                                                    str(x['base_unit_content_allskus'])\
                                                                  ,str( x['base_unit_content']))\
                                                                  , axis=1)

    print("finished base_unit_content_similarity and size is "+str(len(df_product_joined.index)))
    if (len(df_product_joined.index)==0):
      continue
    df_product_joined['no_base_units_similarity'] = df_product_joined.apply(lambda x: \
                                                              fuzz.token_set_ratio(\
                                                                str(x['no_of_base_units_allskus'])\
                                                              ,str (x['no_of_base_units']))\
                                                              , axis=1)
    print("finished no_base_units_similarity and size is "+str(len(df_product_joined.index)))

    if (len(df_product_joined.index)==0):
      
      continue
    df_product_joined = df_product_joined[    (df_product_joined['base_unit_content_similarity']>= 100) \
                            &   (df_product_joined['no_base_units_similarity']>= 100) ]

    df_product_joined.drop_duplicates(subset=['name_allskus'
                                    , 'identifier'
                                    , 'gfgh_id'
                                    , 'name'
                                    , 'name_similarity'],inplace=True)
    df_product_joined.columns = ['name_all_skus'
                      ,'Golden_record'
                      ,'base_unit_content_allskus'
                      ,'no_of_base_units_allskus'
                      ,'gfgh_id'
                      ,'name_gfgh'
                      ,'no_of_base_units_gfgh'
                      ,'base_unit_content_gfgh'
                      ,'structure_packaging_unit_gfgh'
                      ,'name_similarity'
                      ,'base_unit_content_similarity'
                      ,'no_base_units_similarity']

    if (len(df_product_joined.index)==0):
      continue


    df_product_joined.to_sql('result_name_matching',pg_engine,schema='prod_raw_layer', if_exists='append',index=False)

    print(df_product_joined.shape[0])

    print("finished writing to the Database ")



  