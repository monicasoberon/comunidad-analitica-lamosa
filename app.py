import os
import streamlit as st
from dotenv import load_dotenv  # Import the dotenv library
from snowflake.snowpark.functions import col
import pandas as pd
import time
from snowflake.snowpark.session import Session
import snowflake.snowpark as snowpark
from snowflake.snowpark.dataframe_reader import *
from snowflake.snowpark.functions import *
from dotenv import load_dotenv

load_dotenv()

# Snowflake connection parameters
snowflake_connection_parameters = {
    "account": os.environ.get("SNOWACCOUNT"),
    "user": os.environ.get("USER"),
    "password": os.environ.get("PASSWORD"),
    "role": os.environ.get("ROLE"),
    "warehouse": os.environ.get("WAREHOUSE"),
    "database": os.environ.get("DATABASE"),
    "schema": os.environ.get("SCHEMA"),
    "client_session_keep_alive": True
}

def get_snowflake_session(snowflake_connection_parameters):

    try:
        session = Session.builder.configs(snowflake_connection_parameters).create()
        # Optionally, print version to verify connection
        version = session.sql("SELECT CURRENT_VERSION()").collect()
        print(f"Connected to Snowflake version: {version[0][0]}")
        return session
    except Exception as e:
        raise Exception(f"Error connecting to Snowflake: {e}")
    
if 'snowflake_session' not in st.session_state:
    try:
        st.session_state['snowflake_session'] = get_snowflake_session(snowflake_connection_parameters)
    except Exception as e:
        st.error(str(e))
        st.stop()

col1, col2 = st.columns(2)
with col1:
    st.image("Imagenes/logo-lamosa.png", width=300)
with col2:
    st.image("Imagenes/LogoOficial.png", width=350)

st.markdown(
    "<h2 style='text-align: center;'>Bienvenido a la página de Gestiones y Estadísticas de la Comunidad de Analítica.</h2>",
    unsafe_allow_html=True
)

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

# Set up navigation
pg = st.navigation(
    {
        "Gestiones": [sesiones, cursos, clases, usuarios],
        "Reportes": [individual, comunidad, estadisticas],
    }
)
pg.run()
