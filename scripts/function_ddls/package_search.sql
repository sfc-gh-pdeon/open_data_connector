CREATE OR REPLACE FUNCTION config.package_search(org_id string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'package_search'
EXTERNAL_ACCESS_INTEGRATIONS = ({0})
PACKAGES = ('snowflake-snowpark-python','requests')
SECRETS = ('cred' = core.ckan_api_key )
AS
$$
import _snowflake
import requests
import json
import logging
session = requests.Session()
logger = logging.getLogger("python_logger")

def package_search(org_id):
  try:
    token = _snowflake.get_generic_secret_string('cred')
    url = "https://{1}/api/action/package_search?fq=organization:" + org_id + "&include_private=true&rows=1000"
    response = session.get(url, headers = {{"X-CKAN-API-Key": token}})
    return json.dumps(response.json()['result'])
  except Exception as ex:
    logger.error(ex)
    return []
$$;
  
GRANT USAGE ON FUNCTION config.package_search(string) TO APPLICATION ROLE ckan_app_role;