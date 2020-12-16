from configparser import ConfigParser
from datetime import date
import os.path

today = str(date.today()).replace('-', '')
config_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config.cfg')

config = ConfigParser()
config.read(config_file)

# covid api
COVID_API_URL = config.get('COVID-API', 'URL')
COVID_API_KEY = config.get('COVID-API', 'KEY')
COVID_API_KEY_LABEL = config.get('COVID-API', 'KEY_LABEL')
COVID_API_HOST = config.get('COVID-API', 'HOST')
COVID_API_HOST_LABEL = config.get('COVID-API', 'HOST_LABEL')
COVID_API_CONN_ID = config.get('COVID-API', 'CONN')
COVID_API_HEADERS = {COVID_API_KEY_LABEL: COVID_API_KEY, COVID_API_HOST_LABEL: COVID_API_HOST}

# files
COVID_DATA = 'dags/files/{0}_{1}'.format(today, config.get('FILE', 'COVID_DATA'))

# s3
COVID_DATA_BUCKET = config.get('S3', 'BUCKET')
S3_KEY = '{0}_{1}'.format(today, config.get('FILE', 'COVID_DATA'))
S3_CONN_ID = config.get('S3', 'CONN')

# email
AIRFLOW_EMAIL = config.get('OTHER', 'EMAIL')



