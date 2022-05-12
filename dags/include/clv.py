import logging
import pandas as pd
import os

from datetime import datetime, timedelta
from dotenv import load_dotenv
from lifetimes import BetaGeoFitter, GammaGammaFitter
from lifetimes.utils import (calculate_alive_path,
                             calibration_and_holdout_data,
                             summary_data_from_transaction_data)

from matplotlib import pyplot as plt
from sqlalchemy import create_engine

load_dotenv()
logging.getLogger().setLevel(logging.INFO)  # disable prophet seasonality logs

plt.figure(figsize=(15, 10))
pd.options.display.float_format = "{:.7f}".format


DWH_HOST = os.environ.get("PG_HOST")
user = os.environ.get("PG_USERNAME_WRITE")
secret = os.environ.get("PG_PASSWORD_WRITE")

db_name = "dwh"

today = datetime.today()
today_str = str(today.date())

stop_at_covid = False
if stop_at_covid:
    corona_stop_date = "2020-03-13"
    name_suffix = "_before_covid"
else:
    corona_stop_date = today_str
    name_suffix = ""
date_end = (datetime.today().date() - timedelta(days=50)).strftime("%Y-%m-%d")

query = """
    select 
        md5(concat(merchant_key, merchant_company_identifier)) as onboarding_id,
        created_at,
        total_item_quantity,
        liters_sold,
        estimated_order_value
    from prod_info_layer.customer_orders_fact
    left join prod_info_layer.estimated_order_values using (id_sales_order)
    where estimated_order_value is not null
    order by 1,2
"""

q2 = """
    select 
        onboarding_id,
        first_order_date::date,
        extract(days from (now()-first_order_date)) days_alive,
        total_orders from prod_reporting_layer.company_reg_agg_fact 
    where total_orders > 1;
"""


logging.info(f"Creating DWH engine..")
engine = create_engine(
    f"postgresql://{user}:{secret}@{DWH_HOST}:{5432}/{db_name}",
    pool_size=20,
    max_overflow=30,
    pool_recycle=50,
)

logging.info(f"Reading from DWH with SQL query..")
df = pd.read_sql_query(query, engine)
df_times = pd.read_sql_query(q2, engine)
engine.dispose()
summary = summary_data_from_transaction_data(
    df, "onboarding_id", "created_at", observation_period_end=today_str
)

# similar API to scikit-learn and lifelines.
bgf = BetaGeoFitter(penalizer_coef=0.05)
bgf.fit(summary["frequency"], summary["recency"], summary["T"])

# predict purchase in one period
t = 1
summary[
    "predicted_purchases"
] = bgf.conditional_expected_number_of_purchases_up_to_time(
    t, summary["frequency"], summary["recency"], summary["T"]
)
summary.sort_values(by="predicted_purchases").tail(5)

t = 10  # predict purchases in 10 periods
individual = summary.loc[summary.index == "78a7617337a46e1f6c1ca9841fc43792"]
# The below function is an alias to `bfg.conditional_expected_number_of_purchases_up_to_time`
bgf.predict(t, individual["frequency"], individual["recency"], individual["T"])
# 0.0576511

big_list = []

for idx, row in df_times.iterrows():
    id = row["onboarding_id"]
    days_since_birth = int(row["days_alive"])
    sp_trans = df.loc[df["onboarding_id"] == id]
    arr = calculate_alive_path(bgf, sp_trans, "created_at", t=days_since_birth)
    arr2 = [x[0] for x in reversed(arr)]

    days = []
    for x in range(0, days_since_birth):
        day = datetime.today().date() - timedelta(days=x)
        days.append(day.strftime("%Y-%m-%d"))

    res_dict = dict(zip(days, arr2))
    df_t = pd.DataFrame(res_dict, index=[0]).T.reset_index()
    df_t["onboarding_id"] = id
    df_t.rename(columns={"index": "date_at", 0: "p_alive"}, inplace=True)
    big_list.append(df_t)
final = pd.concat(big_list)

logging.info(f"Creating DWH engine..")
logging.info(f"Now uploading to DWH...")

final.to_sql(
    "clv_prob_alive",
    con=engine,
    schema="prod_reporting_layer",
    method="multi",
    index=False,
    chunksize=500,
    if_exists="replace",
)

with engine.connect() as connection:
    connection.execute(
        "GRANT SELECT ON ALL TABLES IN SCHEMA prod_reporting_layer TO dwh_readonly"
    )

logging.info(f"Finished uploading, closing connection")
engine.dispose()

summary_cal_holdout = calibration_and_holdout_data(
    df,
    "onboarding_id",
    "created_at",
    calibration_period_end=date_end,
    observation_period_end=today_str,
)
logging.info(summary_cal_holdout.head())

bgf.fit(
    summary_cal_holdout["frequency_cal"],
    summary_cal_holdout["recency_cal"],
    summary_cal_holdout["T_cal"],
)

focus_variable = "estimated_order_value"
bf = df.groupby("onboarding_id").mean()
xd = pd.DataFrame.merge(summary, bf, how="left", on="onboarding_id")
xd = xd[xd["frequency"] > 0]
xd[[focus_variable, "frequency"]].corr()

ggf = GammaGammaFitter(penalizer_coef=0.05)
ggf.fit(xd["frequency"], xd[focus_variable])
logging.info(ggf)

exp_profit = ggf.conditional_expected_average_profit(
    xd["frequency"], xd[focus_variable]
)
profit_df = pd.DataFrame(exp_profit)
profit_df.columns = ["conditional_expected_average_profit"]

logging.info(
    "Expected conditional average profit: %s, Average profit: %s"
    % (
        ggf.conditional_expected_average_profit(
            xd["frequency"], xd[focus_variable]
        ).mean(),
        xd[xd["frequency"] > 0][focus_variable].mean(),
    )
)

bgf.fit(xd["frequency"], xd["recency"], xd["T"])

x = (
    ggf.customer_lifetime_value(
        bgf,  # the model to use to predict the number of future transactions
        xd["frequency"],
        xd["recency"],
        xd["T"],
        xd[focus_variable],
        time=3,  # months
        discount_rate=0.01,  # monthly discount rate ~ 12.7% annually
    )
).sort_values()
final_df = pd.DataFrame(x)


logging.info(f"Creating DWH engine..")
engine = create_engine(
    f"postgresql://{user}:{secret}@{DWH_HOST}:{5432}/{db_name}",
    pool_size=20,
    max_overflow=30,
    pool_recycle=50,
)

logging.info(f"Now uploading to DWH...")
profit_table_name = "clv_profits" + name_suffix
estimates_table_name = "clv_estimates_3mo" + name_suffix
profit_df.to_sql(
    profit_table_name,
    con=engine,
    schema="prod_reporting_layer",
    method="multi",
    chunksize=700,
    if_exists="replace",
)
final_df.to_sql(
    estimates_table_name,
    con=engine,
    schema="prod_reporting_layer",
    method="multi",
    chunksize=700,
    if_exists="replace",
)
with engine.connect() as connection:
    connection.execute(
        "GRANT SELECT ON ALL TABLES IN SCHEMA prod_reporting_layer TO dwh_readonly"
    )
logging.info(f"Finished uploading, closing connection")
engine.dispose()
