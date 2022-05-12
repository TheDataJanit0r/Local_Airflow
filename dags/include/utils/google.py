import gspread
import gspread_pandas
import gspread_pandas.conf
import gspread_dataframe as gd
import logging

from datetime import datetime
from gspread import SpreadsheetNotFound
from gspread_pandas import Spread
from oauth2client.service_account import ServiceAccountCredentials

logging.getLogger().setLevel(logging.INFO)


def df_to_google_sheet(df, spreadsheet_name, sheet):
    gapi_keyfile = "google_secret.json"
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    google_creds = gspread.authorize(
        ServiceAccountCredentials.from_json_keyfile_name(gapi_keyfile, scope)
    )
    # fix format in order not to remove leading zeros
    df['gfgh_product_id'] = '\'' + df['gfgh_product_id'].astype(str)

    try:
        configuration = gspread_pandas.conf.get_config()
        spread = gspread_pandas.Spread(spreadsheet_name, config=configuration)
        timestamp = datetime.now()
        sheet = f'{sheet}_{timestamp.strftime("%m-%d-%Y %H:%M")}'
        spread.df_to_sheet(df, index=False, sheet=sheet, start='A1',
                           replace=True)
        logging.info('Found Existing Spreadsheet')
        logging.info(spread.url)
    except SpreadsheetNotFound:
        logging.info('Creating New Spreadsheet')
        spreadsheet = google_creds.create(spreadsheet_name)
        spread = Spread(spreadsheet_name)
        spread.df_to_sheet(df,
                           raw_columns=['gfgh_product_id'],
                           index=False,
                           sheet=sheet,
                           start='A1',
                           replace=True)
        spreadsheet.share('kollex.de',
                          perm_type='domain',
                          role='writer')
        spreadsheet.share('pim@kollex.de',
                          perm_type='user',
                          role='writer')
        spreadsheet.share('kovacicek@hotmail.com',
                          perm_type='user',
                          role='writer')
        logging.info(spread.url)


def gedat_to_sheet(df, spreadsheet_name, sheet):
    gapi_keyfile = "google_secret.json"
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    google_creds = gspread.authorize(
        ServiceAccountCredentials.from_json_keyfile_name(gapi_keyfile, scope)
    )

    try:
        configuration = gspread_pandas.conf.get_config()
        spread = gspread_pandas.Spread(spreadsheet_name, config=configuration)
        spread.df_to_sheet(df, index=False, sheet=sheet, start='A1',
                           replace=True)
        logging.info('Found Existing Spreadsheet')
        logging.info(spread.url)
    except SpreadsheetNotFound:
        logging.info('Creating New Spreadsheet')
        spreadsheet = google_creds.create(spreadsheet_name)
        spread = Spread(spreadsheet_name)
        spread.df_to_sheet(df,
                           raw_columns=['KOLLEX_ID'],
                           index=False,
                           sheet=sheet,
                           start='A1',
                           replace=True)
        spreadsheet.share('kollex.de',
                          perm_type='domain',
                          role='writer')
        spreadsheet.share('pim@kollex.de',
                          perm_type='user',
                          role='writer')
        spreadsheet.share('kovacicek@hotmail.com',
                          perm_type='user',
                          role='writer')


def append_to_existing_sheet(sheet_name, worksheet_name, df, col_names):
    logging.info("Getting credentials...")
    gapi_keyfile = "google_secret.json"
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    google_creds = gspread.authorize(
        ServiceAccountCredentials.from_json_keyfile_name(gapi_keyfile, scope)
    )

    existing_sheet = google_creds.open(sheet_name) \
        .worksheet(worksheet_name)
    sheet_as_df = gd.get_as_dataframe(existing_sheet, usecols=col_names)
    logging.info("Found existing sheet.")
    current_dates = sheet_as_df['EXPDATE'].tolist()
    new_dates = df['EXPDATE'].tolist()
    missing_dates = list(set(new_dates) - set(current_dates))
    filter_df = df[df['EXPDATE'].isin(missing_dates)]
    logging.info("Appending dataframe...")
    sheet_as_df = sheet_as_df.append(filter_df)
    configuration = gspread_pandas.conf.get_config()
    gedat_to_sheet(sheet_as_df, sheet_name, worksheet_name + "_backup")
    spread = gspread_pandas.Spread(sheet_name, config=configuration)
    logging.info("Share df to google sheet.")
    spread.df_to_sheet(sheet_as_df,
                       index=False,
                       sheet=worksheet_name,
                       start='A1',
                       replace=False)
