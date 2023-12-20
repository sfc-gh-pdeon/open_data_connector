import streamlit as st
from snowflake.snowpark.context import get_active_session
import util as util
import logging

session = get_active_session()
logger = logging.getLogger("python_logger")
app_name=util.get_app_name()

st.header('Status and Logging')
st.info('Review Request Log to API')

try:
    logs = session.sql('select * from core.ckan_log order by 1 desc')
    lde = st.experimental_data_editor(logs,num_rows="fixed",use_container_width=True)
except Exception as ex:
            logger.error(ex)
            st.error(util.error_msg, icon='🚨')

st.info('Tasks')
try:
    tasks = session.sql(f"begin show tasks in {app_name}.core; let res RESULTSET := (select \"name\",\"warehouse\",\"schedule\",\"state\", IFF(\"condition\" IS NOT NULL,'Has Stream Condition','No Stream Condition') \"has_stream\" from table(result_scan(last_query_id()))); return table(res); end;").collect()
    tde = st.experimental_data_editor(tasks,num_rows="fixed",use_container_width=True)
except Exception as ex:
            logger.error(ex)
            st.error(util.error_msg, icon='🚨')