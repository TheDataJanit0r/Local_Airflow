

# load_dotenv('enviroment_variables.env')

def dbt_run_all_layers():
    import os
    from dotenv import load_dotenv
    import requests
    import json
# os.chdir('incl
    myToken = os.getenv('dbt_token')
    myUrl = os.getenv('all_layers_url')

    #string  = {'Authorization': 'token {}'.format(myToken),'cause' :'Kick Off From Testing Script'}
    head ={'Authorization': 'token {}'.format(myToken)}
    body ={'cause' :'Kicked Off From ALL_SKUS Airflow'}
    r = requests.post(myUrl, headers=head,data=body)
    r_dictionary= r.json()
    print(r.text)