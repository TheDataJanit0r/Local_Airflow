

def dbt_run_diffs():
    import os
    from dotenv import load_dotenv
    import requests
    import json
    # os.chdir('include')
    from airflow.models import Variable
    # load_dotenv('enviroment_variables.env')

    myToken = Variable.get("dbt_token")
    myUrl =  Variable.get("diffs_url")

    #string  = {'Authorization': 'token {}'.format(myToken),'cause' :'Kick Off From Testing Script'}
    head ={'Authorization': 'token {}'.format(myToken)}
    body ={'cause' :'Kicked Off From ALL_SKUS Airflow Diffs Pipeline'}
    r = requests.post(myUrl, headers=head,data=body)
    r_dictionary= r.json()
    print(r.text)