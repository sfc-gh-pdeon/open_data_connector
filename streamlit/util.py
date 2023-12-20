import streamlit as st
from snowflake.snowpark.context import get_active_session
import time
session = get_active_session()
error_msg = 'An error has occurred. Create an [Event Table](https://other-docs.snowflake.com/en/native-apps/consumer-enable-logging) in your account and enable Event Sharing to share the error with the provider'
@st.cache_data
def get_app_name() -> str:
    with st.spinner('Getting App Name...'):
        time.sleep(.1)
        return session.sql("""
            select '"'||current_database()||'"' DB
        """).collect()[0]["DB"]

def is_task_configured() -> bool:
    app_name = get_app_name()
    if is_task_configured not in st.session_state or not st.session_state.is_task_configured:
        df = session.sql(f"show tasks in schema {app_name}.core;").collect()
        st.session_state.is_task_configured = len(df) >= 2 #this should be the total number of tasks deployed to make sure we got them all
        if st.session_state.is_task_configured:
            st.session_state.vwh_submitted = True
    return st.session_state.is_task_configured

def is_key_configured() -> bool:
    app_name = get_app_name()
    if is_key_configured not in st.session_state or not st.session_state.is_key_configured:
        df = session.sql(f"show secrets like 'ckan_api_key' in schema {app_name}.core;").collect()
        st.session_state.is_key_configured = len(df) > 0
    return st.session_state.is_key_configured

def is_url_configured() -> bool: 
    if is_url_configured not in st.session_state or not st.session_state.is_url_configured:         
        df2 = get_ckan_url()
        if len(df2) > 0:
            return True            
    return False

def get_ckan_url() -> str:    
    app_name = get_app_name()  
    df = session.sql(f"show user functions like 'ckan_url_fn' in schema {app_name}.core;").collect()
    if len(df) > 0:
        return session.sql('SELECT core.ckan_url_fn();').collect()[0][0]
    return ''.strip()

def is_external_access_configured() -> bool:
    app_name = get_app_name()
    if is_external_access_configured not in st.session_state or not st.session_state.is_external_access_configured:
        df = session.sql(f"show user functions in schema {app_name}.config;").collect()
        st.session_state.is_external_access_configured = len(df) >= 3 #this should be the total number off UDFs deployed so we make sure they are all successfully deployed
    return st.session_state.is_external_access_configured
