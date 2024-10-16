import os
import streamlit as st
from snowflake.snowpark.functions import col  # Re-added this import
import pandas as pd
import time
import snowflake.snowpark.session as snow_session


# Retrieve Snowflake connection parameters from environment variables
snowflake_connection_parameters = {
    'account': os.environ.get('ACCOUNT'),
    'user': os.environ.get('USER'),
    'password': os.environ.get('PASSWORD'),
    'role': os.environ.get('ROLE'),
    'warehouse': os.environ.get('WAREHOUSE'),
    'database': os.environ.get('DATABASE'),
    'schema': os.environ.get('SCHEMA'),
    'client_session_keep_alive': True
}

# Initialize Snowflake session
session = snow_session.Session.builder.configs(snowflake_connection_parameters).create()

col1, col2 = st.columns(2)
with col1:
    st.image("Imagenes/logo-lamosa.png", width=300)
with col2:
    st.image("Imagenes/LogoOficial.png", width=350)
st.markdown("<h2 style='text-align: center;'>Bienvenido a la página de Gestiones y Estadísticas de la Comunidad de Analítica.</h2>", unsafe_allow_html=True)


with st.sidebar:
    st.image("Imagenes/logo-lamosa.png", width=150)
    st.image("Imagenes/LogoOficial.png", width=150)

st.markdown(f"**¡Bienvenid@ a la Plataforma de Gestión y Reportes!**")

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




