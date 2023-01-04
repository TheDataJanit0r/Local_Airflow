

# load_dotenv('enviroment_variables.env')

def dbt_run_all_layers():
    import os
    import requests
    import json
    from airflow.models import Variable
    # load_dotenv('enviroment_variables.env')

    myToken = Variable.get("dbt_token")
    myUrl =  Variable.get("Matching_url")

    #string  = {'Authorization': 'token {}'.format(myToken),'cause' :'Kick Off From Testing Script'}
    head ={'Authorization': 'token {}'.format(myToken)}
    body ={'cause' :'Kicked Off From Airflow Diffs Pipeline'}
    r = requests.post(myUrl, headers=head,data=body)
    r_dictionary= r.json()
    print(r.text)


