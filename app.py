import os
import streamlit as st
from snowflake.snowpark.functions import col
import pandas as pd
import time

def authenticate_with_azure():
    if "auth_data" not in st.session_state:
        # Check if the user is authenticated via Azure
        # Assuming Azure handles the authentication outside of this app
        col1, col2 = st.columns(2)
        with col1:
            st.image("Imagenes/logo-lamosa.png", width=300)
        with col2:
            st.image("Imagenes/LogoOficial.png", width=350)
        st.markdown("<h2 style='text-align: center;'>Bienvenido a la página de Gestiones y Estadísticas de la Comunidad de Analítica.</h2>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center;'>Por favor autenticate con tu cuenta de microsoft empresarial.</p>", unsafe_allow_html=True)
        st.stop()

    # If authenticated, get the authenticated user's information
    return st.session_state.get("auth_data")

cnx = st.connection("snowflake")
session = cnx.session()

auth_data = authenticate_with_azure()
if auth_data:
    account = auth_data["account"]
    name = account["name"]

    with st.sidebar:
        st.image("Imagenes/logo-lamosa.png", width=150)
        st.image("Imagenes/LogoOficial.png", width=150)

    st.markdown(f"**¡Bienvenid@, {name},  a la Plataforma de Gestión y Reportes!**")
    
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

