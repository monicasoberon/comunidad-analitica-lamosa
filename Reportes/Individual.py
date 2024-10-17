import os
import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col

cnx = st.connection("snowflake")
session = cnx.session()

if "auth_data" not in st.session_state:
    st.write("Please authenticate to access this page.")
    st.stop()  # Stop the execution of this page

st.title("Reporte Actividad Individual")
st.write(
    """Este reporte permite consultar la actividad de los miembros de la 
    comunidad de forma individualizada, incluyendo sesiones y cursos en 
    los que han participado.
    """
)

# Query to get a list of community members
community_result = session.sql("SELECT CORREO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD;")
community_df = community_result.to_pandas()
community_emails = community_df['CORREO'].tolist()

# Display selectbox for individual member selection
selected_member = st.selectbox('Selecciona un miembro:', community_emails)
if selected_member:
    # Tabs for individual activity report
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Detalles del Miembro", 
        "Sesiones Asistidas", 
        "Cursos Inscritos", 
        "Sesiones Invitado No Asistió", 
        "Cursos Invitado No Registró"
    ])

    # Tab 1: Detalles del Miembro
    with tab1:
        member_result = session.sql(f"SELECT NOMBRE, APELLIDO, CORREO, STATUS FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{selected_member}';")
        member_df = member_result.to_pandas()

        st.write("**Detalles del Miembro:**")
        for index, row in member_df.iterrows():
            st.write(f" Nombre: {row['NOMBRE']}")
            st.write(f" Apellido: {row['APELLIDO']}")
            st.write(f" Correo: {row['CORREO']}")
            st.write(f" Estatus: {row['STATUS']}")

    # Tab 2: Sesiones Asistidas
    with tab2:
        member_sessions_result = session.sql(f"""
            SELECT S.NOMBRE_SESION, S.FECHA_SESION 
            FROM LABORATORIO.MONICA_SOBERON.SESION AS S 
            INNER JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS A 
            ON S.ID_SESION = A.ID_SESION 
            WHERE A.ID_USUARIO = (SELECT ID_USUARIO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{selected_member}');
        """)
        member_sessions_df = member_sessions_result.to_pandas()

        st.write("**Sesiones Asistidas por el Miembro:**")
        st.write(member_sessions_df)

    # Tab 3: Cursos Inscritos
    with tab3:
        member_courses_result = session.sql(f"""
            SELECT N.NOMBRE_CURSO, C.FECHA_INICIO, C.FECHA_FIN, R.SOLICITUD_APROBADA, R.CURSO_APROBADO 
            FROM LABORATORIO.MONICA_SOBERON.CURSO AS C 
            INNER JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R 
            ON C.ID_CURSO = R.ID_CURSO 
            INNER JOIN LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS AS N
            ON C.ID_CATALOGO = N.ID_CATALOGO
            WHERE R.ID_USUARIO = (SELECT ID_USUARIO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{selected_member}');
        """)
        member_courses_df = member_courses_result.to_pandas()

        st.write("**Cursos Inscritos por el Miembro:**")
        st.write(member_courses_df)

    # Tab 4: Sesiones a las que fue Invitado y No Asistió
    with tab4:
        invitado_no_asistio = session.sql(f"""
            SELECT S.NOMBRE_SESION, S.FECHA_SESION 
            FROM LABORATORIO.MONICA_SOBERON.SESION AS S 
            INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_SESION AS I 
            ON S.ID_SESION = I.ID_SESION 
            LEFT JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS A 
            ON S.ID_SESION = A.ID_SESION AND I.ID_USUARIO = A.ID_USUARIO 
            WHERE A.ID_USUARIO IS NULL 
            AND I.ID_USUARIO = (SELECT ID_USUARIO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{selected_member}');
        """)
        invitado_no_asistio_df = invitado_no_asistio.to_pandas()

        st.write("**Sesiones a las que fue Invitado y no Asistió:**")
        st.write(invitado_no_asistio_df)

    # Tab 5: Cursos a los que fue Invitado y No Registró
    with tab5:
        invitado_no_registro = session.sql(f"""
            SELECT N.NOMBRE_CURSO, S.FECHA_INICIO 
            FROM LABORATORIO.MONICA_SOBERON.CURSO AS S 
            INNER JOIN LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS AS N
            ON N.ID_CATALOGO = S.ID_CATALOGO
            INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_CURSO AS I 
            ON S.ID_CURSO = I.ID_CURSO 
            LEFT JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS A 
            ON S.ID_CURSO = A.ID_CURSO AND I.ID_USUARIO = A.ID_USUARIO 
            WHERE A.ID_USUARIO IS NULL 
            AND I.ID_USUARIO = (SELECT ID_USUARIO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{selected_member}');
        """)
        invitado_no_registro_df = invitado_no_registro.to_pandas()

        st.write("**Cursos a los que fue Invitado y no se Registró:**")
        st.write(invitado_no_registro_df)
