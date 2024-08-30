--Step 1
--Create the application pacakge and a stage to store the code. 
USE ROLE accountadmin;
GRANT CREATE APPLICATION PACKAGE ON ACCOUNT TO ROLE OPEN_DATA_ROLE;
use role open_data_role;
CREATE APPLICATION PACKAGE CKAN_OPEN_DATA_CONNECTOR;
CREATE OR REPLACE SCHEMA SHARED_CONTENT;
CREATE OR REPLACE STAGE SHARED_CONTENT.CODE_STAGE FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1);

--Upload the code assets using SnowSQL
/*
PUT file://../OPEN_DATA_CONNECTOR/manifest.yml @CKAN_OPEN_DATA_CONNECTOR.shared_content.CODE_STAGE overwrite=true auto_compress=false;
PUT file://../OPEN_DATA_CONNECTOR/scripts/setup.sql @CKAN_OPEN_DATA_CONNECTOR.shared_content.CODE_STAGE/scripts overwrite=true auto_compress=false;
PUT file://../OPEN_DATA_CONNECTOR/readme.md @CKAN_OPEN_DATA_CONNECTOR.shared_content.CODE_STAGE overwrite=true auto_compress=false;
PUT file://../OPEN_DATA_CONNECTOR/streamlit/environment.yml @CKAN_OPEN_DATA_CONNECTOR.shared_content.CODE_STAGE/streamlit overwrite=true auto_compress=false;
PUT file://../OPEN_DATA_CONNECTOR/streamlit/main.py @CKAN_OPEN_DATA_CONNECTOR.shared_content.CODE_STAGE/streamlit overwrite=true auto_compress=false;
PUT file://../OPEN_DATA_CONNECTOR/streamlit/util.py @CKAN_OPEN_DATA_CONNECTOR.shared_content.CODE_STAGE/streamlit overwrite=true auto_compress=false;
PUT file://../OPEN_DATA_CONNECTOR/streamlit/pages/* @CKAN_OPEN_DATA_CONNECTOR.shared_content.CODE_STAGE/streamlit/pages overwrite=true auto_compress=false;
PUT file://../OPEN_DATA_CONNECTOR/scripts/function_ddls/* @CKAN_OPEN_DATA_CONNECTOR.shared_content.CODE_STAGE/scripts/function_ddls/ overwrite=true auto_compress=false;
*/

--For Initial deployment, set the version
ALTER APPLICATION PACKAGE CKAN_OPEN_DATA_CONNECTOR
  ADD VERSION V1_0 USING '@CKAN_OPEN_DATA_CONNECTOR.shared_content.CODE_STAGE';

  --For minor upgrades, add a patch
ALTER APPLICATION PACKAGE CKAN_OPEN_DATA_CONNECTOR ADD PATCH
FOR VERSION v1_0
USING '@CKAN_OPEN_DATA_CONNECTOR.shared_content.CODE_STAGE'
LABEL = '1.0 - init';

--To release a version to your consumers, create a default release directive
ALTER APPLICATION PACKAGE CKAN_OPEN_DATA_CONNECTOR
SET DEFAULT RELEASE DIRECTIVE
VERSION = v1_0
patch=0
;

--If you have downstream consumer accounts that you want to use for testing, you can create custom release directives
ALTER APPLICATION PACKAGE CKAN_OPEN_DATA_CONNECTOR_TEST SET RELEASE DIRECTIVE TEST ACCOUNTS = ( org-account) VERSION = V1_0 PATCH = 0;
 
--Once we set the package for "external" consumption, we cannot unset this. it is permanent for a package. This mandates an automated security scan by Snowflake and possible manual review of the code upon failure of the initial scan. Work with you Sales Engineer if this fails or you have problems.
ALTER APPLICATION PACKAGE CKAN_OPEN_DATA_CONNECTOR
  SET DISTRIBUTION = EXTERNAL;
   
SHOW VERSIONS IN APPLICATION PACKAGE CKAN_OPEN_DATA_CONNECTOR ;
select "version", "patch", "review_status" from table(result_scan(last_query_id()));

-- TESTING ZONE ---
--This creates the application in your own account for testing
CREATE APPLICATION CKAN_Open_Data_Connector
  FROM APPLICATION PACKAGE CKAN_OPEN_DATA_CONNECTOR
  USING '@CKAN_CONNECTOR.shared_content.CODE_STAGE';

--While upgrades are automatic in production, we can force an upgrade for our testing accounts if they are taking too long to perform troubleshooting. This is not used to force upgrades for consumers in production
alter application CKAN_Open_Data_Connector UPGRADE USING '@CKAN_CONNECTOR.shared_content.CODE_STAGE';

--by default, you cannot see any of the queries or data executed by a consumer of the native app. But for troubleshooting, we can debug a local instance and see queries
ALTER APPLICATION CKAN_Open_Data_Connector SET DEBUG_MODE = true;

--If we need to capture events from consumers we need to set the acount to process and store those events
CALL SYSTEM$SET_EVENT_SHARING_ACCOUNT_FOR_REGION('AWS_US_WEST_2', 'PUBLIC', '---');
CREATE DATABASE EVENTS;
CREATE EVENT TABLE EVENTS.PUBLIC.EVENTS;
-- TESTING ZONE ---
