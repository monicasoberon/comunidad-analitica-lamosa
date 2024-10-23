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


# Fetch the Key Vault URL and Managed Identity Client ID from environment variables
key_vault_url = os.getenv("AZURE_KEYVAULT_RESOURCEENDPOINT")
user_assigned_identity_client_id = os.getenv("AZURE_KEYVAULT_CLIENTID")

if not key_vault_url or not user_assigned_identity_client_id:
    raise Exception("Key Vault URL or Client ID not set in environment variables")

# Use DefaultAzureCredential with the user-assigned managed identity's client ID
credential = DefaultAzureCredential(managed_identity_client_id=user_assigned_identity_client_id)

# Create a client to access Azure Key Vault
client = SecretClient(vault_url=key_vault_url, credential=credential)

# Retrieve secrets from Key Vault
try:
    account = client.get_secret("ACCOUNT").value
    password = client.get_secret("PASSWORD").value
    print(f"Account: {account}, Password: {password}")
except Exception as e:
    print(f"Error retrieving secrets from Azure Key Vault: {e}")

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
        
# Check if 'content_visible' is in session state
if 'content_visible' not in st.session_state:
    st.session_state['content_visible'] = True

# Display the content if it's still marked as visible
if st.session_state['content_visible']:
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

    # Simulate delay to hide content after 10 seconds
    time.sleep(10)

    # Hide the content after 10 seconds
    st.session_state['content_visible'] = False
    st.experimental_rerun()

# Sidebar with images
with st.sidebar:
    st.image("Imagenes/logo-lamosa.png", width=150)
    st.image("Imagenes/LogoOficial.png", width=150)

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
