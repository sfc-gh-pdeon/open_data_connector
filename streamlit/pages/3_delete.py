import streamlit as st
from snowflake.snowpark.context import get_active_session
import util as util
import logging
import time

session = get_active_session()
logger = logging.getLogger("python_logger")
app_name=util.get_app_name()

def fix_date_cols(df, tz='UTC'):
    cols = df.select_dtypes(include=['datetime64[ns]']).columns
    for col in cols:
        df[col] = df[col].dt.tz_localize(tz)

st.header('Edit Published Resources')
st.info('1. To insert new records, hover on the final row then click on the plus button.')
st.info('2. To update records, click into a cell and make your changes, then hit return.')
st.info('3. To delete records, use the grey column on the left to select a row, or multiple rows with the shift or control/cmd key, then hit delete on your keyboard.')

tbls = session.table('core.resources')
edited = st.experimental_data_editor(tbls,num_rows="dynamic",use_container_width=True)

col1, col2, col3 = st.columns(3)
with col1: 
    btnSave = st.button('Save', key='save', type='primary')
with col2:
    btnRepublish = st.button('Re-Publish', key='republish', type='secondary')
with col3:
    btnRefresh = st.button('Refresh', key='refresh', type='secondary')

if btnSave:
    with st.spinner("Saving resources..."):
        time.sleep(.1)
        try:
            fix_date_cols(edited, tz='UTC')
            _ = session.sql('truncate table core.resources;').collect()#TODO: this line is a hack because the auto_create/overwrite combo still results in a drop of the table which invalidates the stream. So this will do for now, but should be fixed later.
            session.write_pandas(edited, "core.resources", auto_create_table=False, overwrite=False, quote_identifiers=False)                        
            st.success('Saved!', icon="✅")
        except Exception as ex:
            logger.error(ex)
            st.error(util.error_msg, icon='🚨')
if btnRepublish:
    with st.spinner("Updating resources..."):
        time.sleep(.1)
        try:
            result = session.sql("call CONFIG.SP_UPDATE_RESOURCES()").collect()[0][0]
            if result == 'FAILURE':
                st.error(util.error_msg, icon='🚨')
            else: 
                st.success('Saved!', icon="✅")
        except Exception as ex:
            logger.error(ex)
            st.error(util.error_msg, icon='🚨')

if btnRefresh:
    st.experimental_rerun()       


