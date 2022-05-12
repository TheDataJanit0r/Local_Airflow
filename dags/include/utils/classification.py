import logging
import os
import re
import pandas as pd
from datetime import datetime
from utils.aws import download_file_from_s3
from dotenv import load_dotenv

load_dotenv()


def models_preparation(models_path, required_model_files, load_models_from_s3):
    logging.info('Preparing models')
    # load models from s3
    if load_models_from_s3:
        for model in required_model_files:
            download_file_from_s3(
                output_dir=models_path,
                filename=model,
                bucket_name=os.environ.get('AWS_BUCKET'),
                bucket_object=os.environ.get('AWS_BUCKET_OBJECT')
            )

    # validate required_models are in models_path
    existing_models = os.listdir(models_path)
    missing_models = [model for model in required_model_files if model not in existing_models]
    if missing_models:
        raise ValueError(
            'Missing models {}. Set flag load_models_from_s3'.format(
                missing_models)
        )
    else:
        logging.info('Models prepared')


def clean_text(text):
    """
        text: a string

        return: modified initial string
    """
    re_special_chars = re.compile('[/(){}\[\]\|@;]')
    re_packaging = re.compile('\d+ ?(x|/) ?\d+,?(\d+)? ?l?\.?')
    text = re_special_chars.sub(' ', text)
    text = re_packaging.sub('', text)
    text = text.replace('ADELH.', 'Adelholzener ')
    text = text.replace('Becks.', 'Beckschulte')
    text = text.replace('Gran.', 'Granini')
    text = text.replace('Bitburg.', 'Bitburger')
    text = text.replace('Radl.', 'Radler')
    text = text.replace('Erdinger KW', 'Erdinger Kristallweizen')
    text = text.replace('Bened.', 'Benediktiner')
    text = text.replace('A-Fr0,0', 'Alkoholfrei 0,0%')
    text = text.replace('Gerolst. Gourm.', 'Gerolsteiner Gourmet')
    text = text.replace('Köstr. Schwarzb.', 'Köstritzer Schwarzbier')
    text = text.lower()
    return text


def clean_text_simple(text):
    re_special_chars = re.compile('[/(){}\[\]\|@;]')
    text = text.replace('Becks.', 'Beckschulte')
    text = text.lower()
    text = re_special_chars.sub(' ', text)
    return text


def predict_base_code(text, model):
    pred_code = model.predict(([text]))[0]
    return pred_code


def predict_proba(model, text, pred_code=None):
    if pred_code is None:
        pred_code = model.predict(([text]))[0]
    probability = model.predict_proba([text])
    dict_of_words = {model.classes_[i]: probability[0][i] for i in range(0, len(probability[0]))}
    result_df = pd.DataFrame.from_dict(dict_of_words, orient='index')
    result_df = result_df.reset_index()
    result_df.rename(columns=({'index': 'category', 0: 'precision'}), inplace=True)
    result_df.precision = result_df.precision.apply('{:0.5f}'.format)
    return result_df.loc[result_df.category == pred_code]['precision'].values.item()


def predict(df, cat, model, clean_text_func=clean_text_simple):
    logging.info('Generating prediction for {}'.format(cat))
    bf = df[df['category_predicted'] == cat].copy()
    if not bf.empty:
        logging.info('{} Predicting base code'.format(datetime.now()))
        bf['name'] = bf['name'].apply(clean_text_func)
        bf['base_code'] = bf['name'].apply(predict_base_code, args=(model,))
        logging.info('{} Predicting base code proba'.format(datetime.now()))
        bf['base_code_proba'] = bf.apply(
            lambda row: predict_proba(
                model=model, text=row['name'], pred_code=row['base_code']),
            axis=1)
    else:
        logging.info("Skipping prediction as there is no data for {}".format(cat))
    return bf


def get_single_unit_specs(string, app):
    if app == 'kxe':
        pattern = '(?P<base_unit_content>\d*(,|\.)\d+)(?P<net_content_uom>[A-Za-z]\.?$)?'
        title_search = re.search(pattern, string, re.IGNORECASE)
        if title_search:
            base_unit_content = title_search.group('base_unit_content')
            net_content_uom = title_search.group('net_content_uom')
            if not net_content_uom:
                net_content_uom = 'l'
            return '1', base_unit_content.strip(','), net_content_uom
    else:
        pattern = '(?P<base_unit_content>\d*(,|\.)\d+) ?(?P<net_content_uom>[A-Za-z]\.?$)?'

        title_search = re.search(pattern, string, re.IGNORECASE)

        if string.endswith("er"):
            pattern = "(?P<base_unit_content>\d+)(er)?"
            title_search = re.search(pattern, string, re.IGNORECASE)

            if title_search:
                base_unit_content = title_search.group('base_unit_content')
                net_content_uom = 'l'

                return '1', base_unit_content.strip(','), net_content_uom

        if string.endswith(" L"):
            pattern = "(?P<base_unit_content>\d*(,|\.)\d+) L"
            title_search = re.search(pattern, string, re.IGNORECASE)

            if title_search:
                base_unit_content = title_search.group('base_unit_content')
                net_content_uom = 'l'

                return '1', base_unit_content.strip(','), net_content_uom

        if title_search:
            base_unit_content = title_search.group('base_unit_content')
            net_content_uom = title_search.group('net_content_uom')
            if not net_content_uom:
                net_content_uom = 'l'

            return '1', base_unit_content.strip(','), net_content_uom


def get_specs(string):
    pattern = '(?P<no_of_base_units>\d+) ?(x|/) ?(?P<base_unit_content>\d*((,|\.)\d+)?)?(?P<net_content_uom>[A-Za-z]$)?'
    title_search = re.search(pattern, string, re.IGNORECASE)
    if title_search:
        amount_single_unit = title_search.group('no_of_base_units')
        base_unit_content = title_search.group('base_unit_content')
        net_content_uom = title_search.group('net_content_uom')
        if base_unit_content.startswith(','):
            base_unit_content = f"0{base_unit_content}"
        if ',' in base_unit_content or '.' in base_unit_content:
            base_unit_content = base_unit_content.rstrip('0')
        if not net_content_uom:
            net_content_uom = 'l'
        return amount_single_unit, base_unit_content.strip(','), net_content_uom
