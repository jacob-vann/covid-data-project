from configparser import ConfigParser
from datetime import date
import os.path

today = str(date.today()).replace('-', '')
file = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config.ini')
config = ConfigParser()
config.read(file)

# covid api
covid_api_url = config['covid-api']['url']
covid_api_headers = {'x-rapidapi-key': config['covid-api']['key'], 'x-rapidapi-host': config['covid-api']['host']}
covid_api_conn_id = config['covid-api']['conn']

# s3
covid_data_bucket = config['s3']['bucket']
s3_key = '{0}_covid_data.csv'.format(today)
s3_conn_id = config['s3']['conn']

# email
email = config['other']['email']

# files
file_name = 'dags/files/{0}_covid_data.csv'.format(today)

