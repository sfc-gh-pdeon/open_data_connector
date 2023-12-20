CREATE OR REPLACE FUNCTION config.get_orgs()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'get_orgs'
EXTERNAL_ACCESS_INTEGRATIONS = ({0})
PACKAGES = ('snowflake-snowpark-python','requests')
SECRETS = ('cred' = core.ckan_api_key )
AS
$$
import _snowflake
import requests
import json
import logging
logger = logging.getLogger("python_logger")
session = requests.Session()

def get_orgs():
  try:
    token = _snowflake.get_generic_secret_string('cred')
    url = "https://{1}/api/action/organization_list_for_user"
    response = session.get(url, headers = {{"X-CKAN-API-Key": token}})
    return json.dumps(response.json()['result'])
  except Exception as ex:
    logger.error(ex)
    return []
$$;
  
GRANT USAGE ON FUNCTION config.get_orgs() TO APPLICATION ROLE ckan_app_role;