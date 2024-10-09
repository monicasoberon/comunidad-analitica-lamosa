import os
import streamlit as st
from snowflake.snowpark.functions import col
import pandas as pd
from streamlit_msal import Msal
import time

# Authentication
def authenticate():
    auth_data = None

    with st.sidebar:
        try:
            auth_data = Msal.initialize_ui(
                client_id=st.secrets["client_id"],
                authority=st.secrets["authority"],
                scopes=["User.Read", "User.ReadBasic.All"],
                connecting_label="Connecting...",
                disconnected_label="Disconnected",
                sign_in_label="Sign in",
                sign_out_label="Sign out"
            )
        except Exception as e:
            st.write(f"Error during MSAL initialization: {e}")

    if auth_data:
        st.session_state["auth_data"] = auth_data
    else:
        if "auth_data" not in st.session_state:
            col1, col2 = st.columns(2)
            with col1:
                st.image("Imagenes/logo-lamosa.png", width=300)
            with col2:
                st.image("Imagenes/LogoOficial.png", width=350)
            st.markdown("<h2 style='text-align: center;'>Bienvenido a la página de Gestiones y Estadísticas de la Comunidad de Analítica.</h2>", unsafe_allow_html=True)
            st.markdown("<p style='text-align: center;'>Por favor autenticate con tu cuenta de microsoft empresarial.</p>", unsafe_allow_html=True)
            st.stop()

    return st.session_state.get("auth_data")

cnx = st.connection("snowflake")
session = cnx.session()

auth_data = authenticate()
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

