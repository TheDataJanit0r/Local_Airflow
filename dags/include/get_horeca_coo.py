import logging
import os
import requests
import sys

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from tqdm import tqdm

load_dotenv()

logging.getLogger().setLevel(logging.INFO)  # disable prophet seasonality logs

DWH_HOST = os.environ.get("PG_HOST")
user = os.environ.get("PG_USERNAME_WRITE")
secret = os.environ.get("PG_PASSWORD_WRITE")
db_name = "dwh"
api_key = os.environ.get("GEOAPIFY_KEY")
focus_var = "onboarding_id"

all_results = []


def get_lat_lon_from_geoapify(key: str, address: str) -> (float, float):
    url = f"https://api.geoapify.com/v1/geocode/search?text={address}&apiKey={api_key}"
    r = requests.get(url)
    results = r.json().get("features")
    if len(results) >= 1:
        logging.info(f"Found {len(results)} coordinates for {merchant_key} at {address}..")
        # take just first result
        lat = results[0]["properties"]["lat"]
        lon = results[0]["properties"]["lon"]

        full_result_df = pd.DataFrame([g["properties"] for g in results])
        full_result_df[focus_var] = key

        all_results.append(full_result_df)
    else:
        return "", ""

    return lat, lon


just_locs = """
with locs as (select distinct md5(concat(merchant_key, merchant_company_identifier)) onboarding_id, merchant_key, merchant_company_identifier, address1, city, zip_code from prod_info_layer.customer_location_fact)

,coo as (select * from prod_reporting_layer.company_coordinates
union
select * from prod_reporting_layer.horeca_coordinates_geoapify)

select * from locs
left join coo using (onboarding_id)
where coo.onboarding_id is null
and merchant_key not in ('sondermann', 'lennards', 'test-fabian', 'test-wesley', 'kollex-erp', 'kollex-csv', 'liquisales',
'dicomputer', 'orgasoft', 'fallback', 'gvs_test', 'hendricks', 'testmerchant', 'jv_kollex_testmerchant',
'kollex-s')
and trim(address1) not similar to '(Teststr.*|Torstra(ss|ß)e 1(1|5)5|Genthiner (Str.|Straße) 32)'
and merchant_company_identifier is not null and address1 <> ''
and left(merchant_key, 4) <> 'test'
"""


logging.info(f"Creating DWH engine..")
engine = create_engine(f"postgresql://{user}:{secret}@{DWH_HOST}:{5432}/{db_name}")

logging.info(f"Reading from DWH with SQL query..")
df_locs = pd.read_sql_query(just_locs, engine)

engine.dispose()

df_merchants = df_locs.copy()
if len(df_merchants) < 1:
    logging.info(f"No new locations found. Exiting..")
    sys.exit()

df_merchants["addr_long"] = (
    df_merchants["address1"]
    + " "
    + df_merchants["zip_code"].astype("str")
    + " "
    + df_merchants["city"]
)

logging.info(
    f"Found {len(df_merchants)} horecas without coordinates. Fetching them now.."
)

loc_dict = {}
for i in tqdm(range(0, len(df_merchants))):
    row = df_merchants.iloc[i]

    if row[focus_var] in loc_dict.keys():
        continue
    merchant_key = row[focus_var]
    addr = row["addr_long"]
    if pd.isna(addr):
        continue
    lat, lon = get_lat_lon_from_geoapify(merchant_key, addr)
    if lat:
        loc_dict[merchant_key] = (lat, lon)


df_merchants["la"] = df_merchants[focus_var].apply(
    lambda x: loc_dict[x][0] if x in loc_dict else np.NaN
)
df_merchants["lo"] = df_merchants[focus_var].apply(
    lambda x: loc_dict[x][1] if x in loc_dict else np.NaN
)
last_idx = df_merchants.loc[df_merchants[focus_var] == merchant_key].index.values.item()
df_up = df_merchants[: last_idx + 1][[focus_var, "la", "lo"]].drop_duplicates()

logging.info(f"Creating DWH engine..")
engine = create_engine(f"postgresql://{user}:{secret}@{DWH_HOST}:{5432}/{db_name}")

logging.info(f"Now uploading to DWH...")
df_up.to_sql(
    "horeca_coordinates_geoapify",
    con=engine,
    schema="prod_reporting_layer",
    method="multi",
    chunksize=500,
    if_exists="append",
    index=False,
)

logging.info(f"Finished uploading {len(df_up)} rows, closing connection")
engine.dispose()
