CREATE OR REPLACE FUNCTION config.resource_update(resource_id string, extension string, presigned_url string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'resource_update'
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

def resource_update(resource_id, extension, presigned_url):
  try:
    token = _snowflake.get_generic_secret_string('cred')
    url = "https://{1}/api/action/resource_update"
    json_options = {{'id':resource_id,'format':extension ,'url':presigned_url, 'clear_upload':'true'}}
    response = session.post(url, headers = {{"X-CKAN-API-Key": token}}, json = json_options)
    return json.dumps(response.json()['result'])
  except Exception as ex:
    logger.error(ex)
    return []
$$;
  
GRANT USAGE ON FUNCTION config.package_search(string) TO APPLICATION ROLE ckan_app_role;