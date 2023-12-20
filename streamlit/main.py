import streamlit as st
from snowflake.snowpark.context import get_active_session
session = get_active_session()
st.set_page_config(layout="wide")

st.header('How to use the Open Data Portal Connector')
st.warning('The Open Data Connector publishes data to a public portal. You are responsible for only publishing data that is meant for public distribution.')
st.info('Step 1: Initialize the App by performing some one-time setup steps')
st.info('Step 2: Select and publish a table using a listing from the Open Data Portal')
st.info('Step 3: Remove any tables that should no longer receive updates')
st.info('Step 4: Review and track the status of jobs')