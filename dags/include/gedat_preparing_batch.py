import argparse
import csv
import fnmatch
import logging
import os
import pandas as pd
import paramiko
import shutil
import datetime
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd

load_dotenv('enviroment_variables.env')

pg_host =  os.getenv('PG_HOST_STAGING')
pg_user = os.getenv('PG_USERNAME_WRITE_STAGING')
pg_password = os.getenv('PG_PASSWORD_WRITE_STAGING')
pg_database = os.getenv('PG_DATABASE')
pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
pg_engine = create_engine(f"{pg_connect_string}", echo=False, pool_pre_ping=True, pool_recycle=800)

############ Create list of customers based on agreed format
############ with gedat

query = """
with merchants as (
		select '01'                                as "Satzart"
			, '00000001'                          as "Empfänger"
			, '43999023'                          as "Absender"
			, '01'                                as "Versionsnummer"
			, concat( 'M_', id_merchant )         as "Hersteller-Kunden-Nr."
			, NULL                                as "GEDAT-Adressnummer"
			, NULL                                as "GLN"
			, NULL                                as "Leerfeld1"
			, NULL                                as "Leerfeld2"
			, NULL                                as "Gemeindekennziffer"
			, 'Getränkefachgroßhandel'            as "Geschäftstyp"
			, merchant.name                       as "Name-1 (Bezeichnung)"
			, NULL                                as "Name-2 (Inhaber)"
			, merchant.name                       as "Kurzbezeichnung"
			, street                              as "Straße u. Hausnummer"
			, zip                                 as "Postleitzahl"
			, city                                as "Ort"
			, 'Deutschland'                       as "Land"
			, contact.mobile_phone                as "Telefon-1"
			, NULL                                as "Telefon-2"
			, NULL                                as "Leerfeld2"
			, NULL                                as "Telefax"
			, NULL                                as "Leerfeld3"
			, to_char( now( )::date, 'YYYYMMDD' ) as "Übertragungsdatum"
			, 'N'                                 as "Status"
			, '10000004'                          as "Übertragungsnummer"
		from fdw_customer_service.merchant
				left join fdw_customer_service.merchant_has_contacts
							on merchant.id_merchant = merchant_has_contacts.fk_merchant
				left join fdw_customer_service.contact on merchant_has_contacts.fk_contact
			= contact.id_contact
				left join fdw_customer_service.address on address.id_address = merchant.fk_address

	)


, customers as (
				select '01'                                as "Satzart"
					, '00000001'                          as "Empfänger"
					, '43999023'                          as "Absender"
					, '01'                                as "Versionsnummer"
					, concat( 'O_', id_customer )         as "Hersteller-Kunden-Nr."
					, NULL                                as "GEDAT-Adressnummer"
					, NULL                                as "GLN"
					, NULL                                as "Leerfeld"
					, NULL                                as "Leerfeld1"
					, NULL                                as "Gemeindekennziffer"
					, NULL                                as "Geschäftstyp"
					, customer.name                       as "Name-1 (Bezeichnung)"
					, customer.name2                      as "Name-2 (Inhaber)"
					, customer.short_name                 as "Kurzbezeichnung"
					, address.street                      as "Straße u. Hausnummer"
					, address.zip                         as "Postleitzahl"
					, address.city                        as "Ort"
					, address.country                     as "Land"
					, contact.mobile_phone                as "Telefon-1"
					, NULL                                as "Telefon-2"
					, NULL                                as "Leerfeld2"
					, NULL                                as "Telefax"
					, NULL                                as "Leerfeld3"
					, to_char( now( )::date, 'YYYYMMDD' ) as "Übertragungsdatum"
					, 'N'                                 as "Status"
					, '10000004'                          as "Übertragungsnummer"
				from fdw_customer_service.customer
						left join fdw_customer_service.address
									on customer.fk_address = address.id_address
						left join fdw_customer_service.customer_has_contacts
									on customer.id_customer = customer_has_contacts.fk_customer
						left join fdw_customer_service.contact
									on customer_has_contacts.fk_contact = contact.id_contact


			)


, final as (
					select *
					from customers

					union
					select *
					from merchants
				)

select *
from final
where "Straße u. Hausnummer" <> ''
                                """

df = pd.read_sql_query(query, pg_engine)
logging.info(f'Data loaded, closing connection and tunnel')



dt_string = (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d-%H-%M-00")



filename = f"Kunden_{dt_string}.txt"
df.to_csv(filename, sep="@", index=False
     #     , encoding="windows-1252"
#           , encoding="iso8859-15"
#                     , encoding="iso8859-15"
          
                     , encoding="latin9",errors='replace'
          , header=False,quoting=csv.QUOTE_ALL,
          escapechar='"',
          line_terminator='\r\n')

########## Upload List of customers to SFTP

path = '/kollex-transfer/gfgh/gedat/kollex'
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASS = os.getenv("SFTP_PASS")


########## Upload the table to the SFTP
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(SFTP_HOST, username=SFTP_USER, password=SFTP_PASS)

sftp = client.open_sftp()
sftp.put(f"{filename}", f"{path}/{filename}")
logging.info(f'Uploaded: {filename} to {path}')

sftp.close()


########## Download all files from SFTP

path = '/kollex-transfer/gfgh/gedat/GEDAT/history/'
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(SFTP_HOST, username=SFTP_USER, password=SFTP_PASS)



sftp = client.open_sftp()


count=0
files= list(sftp.listdir(path))
for filename in sftp.listdir(path):
   
	files.insert(count,filename)
	count=count+1


for file in files:
    sftp.get(f"{path}/{file}", f"{file}")
    logging.info(f'Downloaded locally: {file}')
    
    sftp.rename(f"{path}/{file}", f"{path}/history/{file}")
    logging.info(f'Moved {file} to /history folder on remote')

import glob 

Downloaded_files =glob.glob('Kunden_result*.txt')

df_to_upload = pd.DataFrame()



########### Appending these TXT files into one Dataframe

for file in Downloaded_files:
   df_to_upload= df_to_upload.append(pd.read_table(file))


df_to_upload=df_to_upload.rename(columns=str.lower)

########## Upload results to DWH
df_to_upload.to_sql( con = pg_engine
                    ,name='gedat_results'
                    ,schema='sheet_loader'
                    ,if_exists='replace'
                    ,index=False)
pg_engine.dispose()

### remove all trash files

files_to_remove =glob.glob('Kunden_*.txt')
import os

[os.remove(file) for file in files_to_remove]