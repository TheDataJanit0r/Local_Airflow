import joblib
import logging
import os
import pandas as pd


logging.getLogger(__file__).setLevel(logging.INFO)


# source paths
def get_src_path():
    src_path = os.path.join(os.getcwd(), 'src')
    os.makedirs(src_path, exist_ok=True)
    return src_path


def get_akquise_src_path():
    akquise_src_path = os.path.join(get_src_path(), 'akquise')
    os.makedirs(akquise_src_path, exist_ok=True)
    return akquise_src_path


def get_gedat_path():
    gedat_path = os.path.join(get_src_path(), "gedat")
    os.makedirs(gedat_path, exist_ok=True)
    return gedat_path


def get_models_path():
    models_path = os.path.join(get_src_path(), 'models')
    os.makedirs(models_path, exist_ok=True)
    return models_path


def get_model_name_by_category_name(category_name):
    logging.info(f"Getting model by category: {category_name}")
    cat = "_".join(category_name.split(' ')).lower()
    model_name = '{}_classifier.joblib'.format(cat)
    return model_name


def get_model_path_by_category_name(category_name):
    models_path = get_models_path()
    model_name = get_model_name_by_category_name(category_name)
    return os.path.join(models_path, model_name)


def load_model_by_category_name(category_name):
    logging.info(f"Loading model by category: {category_name}")
    model_path = get_model_path_by_category_name(category_name=category_name)
    return joblib.load(model_path)


def get_txt_path():
    txt_path = os.path.join(get_src_path(), 'txt')
    os.makedirs(txt_path, exist_ok=True)
    return txt_path


# output paths
def get_out_path():
    out_path = os.path.join(os.getcwd(), 'out')
    os.makedirs(out_path, exist_ok=True)
    return out_path


def get_name_matching_path(merchant):
    name_matching_path = os.path.join(get_out_path(), 'name_matching', merchant)
    os.makedirs(name_matching_path, exist_ok=True)
    return name_matching_path


def get_akquise_out_path():
    akquise_out_path = os.path.join(get_out_path(), 'akquise')
    os.makedirs(akquise_out_path, exist_ok=True)
    return akquise_out_path


def get_data_out_path():
    data_out_path = os.path.join(get_out_path(), 'data')
    os.makedirs(data_out_path, exist_ok=True)
    return data_out_path


def get_gedat_out_path():
    gedat_out_path = os.path.join(get_out_path(), 'gedat')
    os.makedirs(gedat_out_path, exist_ok=True)
    return gedat_out_path


# images upload automation
def get_images_src_path():
    src_path = get_src_path()
    images_src_path = os.path.join(src_path, 'images')
    os.makedirs(images_src_path, exist_ok=True)
    return images_src_path


def get_images_out_path():
    out_path = get_out_path()
    images_out_path = os.path.join(out_path, 'images')
    os.makedirs(images_out_path, exist_ok=True)
    return images_out_path


def get_images_processed_out_path():
    out_path = get_out_path()
    images_processed_out_path = os.path.join(out_path, 'processed')
    os.makedirs(images_processed_out_path, exist_ok=True)
    return images_processed_out_path


# classification temp files
def _generate_temp_file_name(cat):
    import re
    cat = re.sub(r'\W+', '', cat).lower().replace(' ', '_')
    return '{}_name_match.csv'.format(cat)


def _generate_temp_file_path(cat, out_dir):
    return os.path.join(out_dir, _generate_temp_file_name(cat))


def write_temp_file(df, cat, out_dir):
    logging.info(f"Writing prediction file: {out_dir} ")
    temp_file_path = _generate_temp_file_path(cat, out_dir)
    df.to_csv(temp_file_path, index=True, index_label='index')


def read_temp_file(cat, out_dir):
    temp_file_path = _generate_temp_file_path(cat, out_dir)
    if os.path.exists(temp_file_path):
        logging.info('Reading prediction file {}'.format(temp_file_path))
        return pd.read_csv(temp_file_path, index_col='index')
    else:
        logging.info('Prediction file not found {}'.format(temp_file_path))
        return False

