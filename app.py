import os
import streamlit as st
from snowflake.snowpark.functions import col  # Re-added this import
import pandas as pd
import time

# Initialize connection to Snowflake
cnx = st.connection("snowflake")
session = cnx.session()

# Get the user's name from Azure App Service authentication
user_name = os.environ.get('X-MS-CLIENT-PRINCIPAL-NAME')

if user_name:
    # User is authenticated
    with st.sidebar:
        st.image("Imagenes/logo-lamosa.png", width=150)
        st.image("Imagenes/LogoOficial.png", width=150)
    
    st.markdown(f"**¡Bienvenid@, {user_name}, a la Plataforma de Gestión y Reportes!**")
    
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
else:
    # User is not authenticated
    col1, col2 = st.columns(2)
    with col1:
        st.image("Imagenes/logo-lamosa.png", width=300)
    with col2:
        st.image("Imagenes/LogoOficial.png", width=350)
    st.markdown("<h2 style='text-align: center;'>Bienvenido a la página de Gestiones y Estadísticas de la Comunidad de Analítica.</h2>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center;'>Por favor, autentícate con tu cuenta de Microsoft empresarial.</p>", unsafe_allow_html=True)
    st.stop()
