import argparse
from datetime import datetime, date
from dotenv import load_dotenv
import joblib
import logging
import numpy as np
import os
import pandas as pd
from utils.google import df_to_google_sheet
from utils.db import postgres_engine_factory
from utils.classification import (
    predict,
    predict_base_code,
    clean_text,
    get_specs,
    get_single_unit_specs,
    models_preparation)
from utils.generic import (
    read_temp_file,
    write_temp_file,
    get_model_name_by_category_name,
    get_models_path,
    load_model_by_category_name,
    get_name_matching_path
)

load_dotenv()
logging.getLogger().setLevel(logging.INFO)


def data_preparation():
    logging.info(f'Creating DWH engine..')
    engine = postgres_engine_factory()

    logging.info('Preparing data...')
    query = """
    select 
       product_uuid,
       name,
       product_category from prod_info_layer.kxe_products_to_match
    where pim_matched = FALSE
       and pim_not_matchable = FALSE
       and supplier_duplicate = FALSE
       and product_category like '%%Getr%%'
    """

    base = """
        select 
            sku,
            name_from_merchant,
            base_unit_content,
            no_of_base_units,
            sales_unit_pkgg,
            base_code,
            l1_code,
            sku_title,
            pim_category,
            structure_packaging_unit,
            type_packaging_unit,
            size,
            type_single_unit
        from prod_info_layer.pim_classifier_base
        union
        select 
            sku,
            sku_title,
            max(base_unit_content),
            max(no_of_base_units),
            max(sales_unit_pkgg),
            max(base_code),
            max(l1_code),
            max(sku_title),
            max(pim_category),
            max(structure_packaging_unit),
            max(type_packaging_unit),
            max(size),
            max(type_single_unit)
        from prod_info_layer.pim_classifier_base
        group by 1,2;
     """

    single_base = """
        select 
            base_code,
            l1_code,
            enablement,
            CONCAT(brand, ' ', title,' ',  amount_single_unit, 'x', net_content)::text as sku_title,
            amount_single_unit,
            net_content,
            structure_packaging_unit,
            type_packaging_unit,
            concat(amount_single_unit, ' x ', net_content) as size,
            type_single_unit,
            identifier as sku,
            id as akeneo_id
        from prod_raw_layer.all_skus
        left join prod_raw_layer.pim_catalog_product using (identifier)
        where enablement > 0 
            and type_single_unit <> 'FREAK (abweichendes Design, Rücksprache halten)' 
            and net_content is not null
            and base_code is not null
    """

    logging.info("Reading queries...")
    df = pd.read_sql_query(query, engine)
    df_base = pd.read_sql_query(base, engine)
    df_singles = pd.read_sql_query(single_base, engine)
    logging.info('Data prepared')
    return df, df_singles, df_base


def run(load_models_from_s3, use_temp_files):
    logging.info("Start runnig...")
    categories_list = [
        'Alkoholfreies Bier',
        'Bier',
        'Saft',
        'Spirituosen',
        'Softdrinks',
        'Wasser',
        'Wein & Prickelndes',
    ]
    log_reg_model_name = 'log_reg_classifier.joblib'

    # prepare models

    required_model_files = [get_model_name_by_category_name(cat) for cat in categories_list]
    required_model_files.append(log_reg_model_name)
    models_preparation(
        models_path=get_models_path(),
        required_model_files=required_model_files,
        load_models_from_s3=load_models_from_s3
    )

    # load models
    category_model_map = {
        cat: load_model_by_category_name(cat) for cat in categories_list
    }

    # prepare data
    df, df_singles, df_base = data_preparation()

    # generate category_predicted

    log_reg_model = joblib.load(os.path.join(get_models_path(), log_reg_model_name))
    df['category_predicted'] = df['name'].apply(predict_base_code, args=(log_reg_model,))
    df['clean_name'] = df['name'].apply(lambda x: clean_text(text=x))

    # process categories
    to_join = list()
    for category, model in category_model_map.items():
        logging.info('\n{} Predicting category: {}'.format(datetime.now(), category))
        if use_temp_files:
            # create output directory structure
            out_dir = get_name_matching_path('pim_classification_kxe')
            # read or generate prediction
            prediction = read_temp_file(category, out_dir)
            if prediction is False:
                prediction = predict(df, category, model)
                write_temp_file(prediction, category, out_dir)
        else:
            prediction = predict(df, category, model)
        to_join.append(prediction)
    df = pd.concat(to_join)

    logging.info('Processing base')
    df = df[['product_uuid',
             'name',
             'product_category',
             'clean_name',
             'category_predicted',
             'base_code',
             'base_code_proba']]
    df['amount_single_units'] = df['name'].apply(
        lambda x: get_specs(x)[0]
        if get_specs(x)
        else get_single_unit_specs(x, 'kxe')[0]
        if get_single_unit_specs(x, 'kxe')
        else np.NaN
    )
    logging.info("Applying base unit content")
    df['base_unit_content'] = df['name'].apply(
        lambda x:
        get_specs(x)[1]
        if get_specs(x)
        else get_single_unit_specs(x, 'kxe')[1]
        if get_single_unit_specs(x, 'kxe')
        else np.NaN)
    logging("Applying base unit net_content_uom")
    df['net_content_uom'] = df['name'].apply(
        lambda x:
        get_specs(x)[2]
        if get_specs(x)
        else get_single_unit_specs(x, 'kxe')[2]
        if get_single_unit_specs(x, 'kxe')
        else np.NaN)
    # process/clean df
    logging.info("Cleaning df")
    df_cleaned = df.dropna(inplace=False)
    df_cleaned['base_unit_content'].dropna(inplace=True)
    df_cleaned = df_cleaned.loc[df_cleaned.base_unit_content != ''].copy()
    df_cleaned['base_unit_content'] = df_cleaned['base_unit_content'].astype(str)

    # df_cleaned['base_unit_content'] = df_cleaned['base_unit_content'].str.replace('033', '0.33').\
    #     str.replace(',', '.').astype(float)
    # bug 0.0.33 occured so replace 033 -> 0.33 is removed
    df_cleaned['base_unit_content'] = df_cleaned['base_unit_content'].str.replace(',', '.').astype(float)
    df_cleaned['amount_single_units'] = df_cleaned['amount_single_units'].astype(int)
    df_cleaned['base_code_proba'] = df_cleaned['base_code_proba'].astype(float)
    df_cleaned = df_cleaned.loc[df_cleaned.base_code_proba > 0.5]

    # missed items
    df_missed = df.loc[~df.index.isin(df_cleaned.index)]

    # process singles
    logging.info("Processing singles")
    df_singles['net_content'] = df_singles['net_content'].astype(str).str.replace(',', '.').astype(float)
    df_singles['amount_single_unit'] = df_singles['amount_single_unit'].astype(int)

    # merge cleaned and singles
    logging.info("Merging cleaned and singles.")
    merged_df = pd.merge(
        df_cleaned,
        df_singles,
        how='left',
        left_on=['base_code', 'amount_single_units', 'base_unit_content'],
        right_on=['base_code', 'amount_single_unit', 'net_content']
    )

    logging.info('Finalizing data')
    final_new_df = pd.concat([merged_df, df_missed])
    final_new_df['more_than_one_match'] = final_new_df[['product_uuid']].duplicated(keep=False)
    final_new_df = final_new_df[[
        'product_uuid', 'sku', 'base_code', 'more_than_one_match', 'name', 'product_category', 'clean_name',
        'category_predicted', 'base_code_proba', 'amount_single_units', 'base_unit_content',
        'net_content_uom', 'l1_code', 'enablement', 'sku_title', 'amount_single_unit',
        'net_content', 'structure_packaging_unit', 'type_packaging_unit', 'size', 'type_single_unit', 'akeneo_id'
    ]]
    final_new_df['base_code_proba'] = final_new_df['base_code_proba'].astype(float)
    final_new_df = final_new_df.loc[final_new_df.base_code_proba > 0.5]

    logging.info('Sharing via google sheet')
    spreadsheet = 'kxe_pim_matches'
    sheet = 'kxe_matched_{}'.format(date.today().strftime("%d_%m_%Y"))
    df_to_google_sheet(df=final_new_df, spreadsheet_name=spreadsheet, sheet=sheet)


if __name__ == '__main__':
    """
        Usage: 
        python pim_classification_kxe.py --merchant frauendorf 
            --no-including-sixpacks --base-units-exist --no-load-models-from-s3
    """

    parser = argparse.ArgumentParser(description='PIM Classifier for KXE')
    parser.add_argument(
        '--load-models-from-s3',
        dest='load_models_from_s3',
        help='Load Models From S3',
        action='store_true'
    )
    parser.add_argument(
        '--use-temp-files',
        dest='use_temp_files',
        help='Use Temporary Files',
        action='store_true'
    )
    args = parser.parse_args()
    run(load_models_from_s3=args.load_models_from_s3,
        use_temp_files=args.use_temp_files)
