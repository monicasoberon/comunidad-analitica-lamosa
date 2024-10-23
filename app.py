import os
import streamlit as st
from snowflake.snowpark.functions import col
import pandas as pd
import time
from snowflake.snowpark.session import Session
import snowflake.snowpark as snowpark
from snowflake.snowpark.dataframe_reader import *
from snowflake.snowpark.functions import *
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Set key vault URL
key_vault_url = os.getenv("AZURE_KEYVAULT_RESOURCEENDPOINT")

# Create a credential, specifying the user-assigned managed identity (optional)
credential = DefaultAzureCredential(managed_identity_client_id=os.getenv("AZURE_KEYVAULT_CLIENTID"))

# Create a client to access Azure Key Vault
client = SecretClient(vault_url=key_vault_url, credential=credential)

# Fetch Snowflake connection parameters from Azure Key Vault
snowflake_connection_parameters = {
    "account": client.get_secret("ACCOUNT").value,
    "user": "MONICA_SOBERON",
    "password": client.get_secret("PASSWORD").value,
    "role": "PRACTICANTE_ROLE",
    "warehouse": "PRACTICANTE_WH",
    "database": "LABORATORIO",
    "schema": "MONICA_SOBERON",
    "client_session_keep_alive": True
}

# Function to establish a Snowflake session
def get_snowflake_session(snowflake_connection_parameters):
    try:
        session = Session.builder.configs(snowflake_connection_parameters).create()
        # Optionally, print version to verify connection
        version = session.sql("SELECT CURRENT_VERSION()").collect()
        print(f"Connected to Snowflake version: {version[0][0]}")
        return session
    except Exception as e:
        raise Exception(f"Error connecting to Snowflake: {e}")

# Check if Snowflake session is in the session state
if 'snowflake_session' not in st.session_state:
    try:
        st.session_state['snowflake_session'] = get_snowflake_session(snowflake_connection_parameters)
    except Exception as e:
        st.error(str(e))
        st.stop()

# Set up the UI with columns and images
col1, col2 = st.columns(2)
with col1:
    st.image("Imagenes/logo-lamosa.png", width=300)
with col2:
    st.image("Imagenes/LogoOficial.png", width=350)

st.markdown(
    "<h2 style='text-align: center;'>Bienvenido a la página de Gestiones y Estadísticas de la Comunidad de Analítica.</h2>",
    unsafe_allow_html=True
)

# Sidebar with images
with st.sidebar:
    st.image("Imagenes/logo-lamosa.png", width=150)
    st.image("Imagenes/LogoOficial.png", width=150)

st.markdown("**¡Bienvenid@ a la Plataforma de Gestión y Reportes!**")

# Define your pages
sesiones = st.Page("Gestiones/Sesiones.py", title="Sesiones")
cursos = st.Page("Gestiones/Cursos.py", title="Cursos")
clases = st.Page("Gestiones/Clases.py", title="Clases")
usuarios = st.Page("Gestiones/Usuarios.py", title="Usuarios")
individual = st.Page("Reportes/Individual.py", title="Individual")
comunidad = st.Page("Reportes/Comunidad.py", title="Comunidad")
estadisticas = st.Page("Reportes/Estadisticas.py", title="Estadísticas")

pg = st.navigation(
    {
        "Gestiones": [sesiones, cursos, clases, usuarios],
        "Reportes": [individual, comunidad, estadisticas],
    }
)
pg.run()
