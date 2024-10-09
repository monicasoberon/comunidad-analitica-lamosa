import os
import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col

cnx = st.connection("snowflake")
session = cnx.session()

if "auth_data" not in st.session_state:
    st.write("Please authenticate to access this page.")
    st.stop()  # Stop the execution of this page
    
st.title("Datos de Invitados y Asistencias de Sesiones")
st.write(
"""Esta pantalla ayuda a registrar datos de las sesiones al igual que los invitados y los participantes.
"""
)

# Create tabs
tabs = st.tabs(["Crear Sesión", "Editar Sesión", "Registrar Invitados", "Registrar Asistentes", "Borrar Sesión"])

with tabs[0]:
    st.header("Crear Sesión")
    with st.form(key='new_session_form'):
        session_name = st.text_input("Nombre de la Sesión")
        session_date = st.date_input("Fecha de la Sesión")
        link_session_info = st.text_input("Enlace de Información de la Sesión (opcional)")
    
        submit_button = st.form_submit_button(label='Crear Sesión')
    
        if submit_button:
            if session_name and session_date:
                # Insert new session into the database
                query = f"""
                INSERT INTO SESION (NOMBRE_SESION, FECHA_SESION, LINK_SESION_INFORMATIVA)
                VALUES ('{session_name}', '{session_date}', '{link_session_info}');
                """
                session.sql(query).collect()
                st.success(f"Sesión '{session_name}' creada con éxito.")
            else:
                st.error("Por favor, ingrese el nombre y la fecha de la sesión.")

with tabs[1]:
    st.header("Editar Sesión")

    # Query for session information
    session_result = session.sql("SELECT NOMBRE_SESION FROM LABORATORIO.MONICA_SOBERON.SESION;")
    session_df = session_result.to_pandas()
    session_names = session_df['NOMBRE_SESION'].tolist()

    # Display session select box
    selected_session = st.selectbox('Selecciona una Sesión:', session_names, key="edit")
    if selected_session:
        # Query for session details based on the selected session
        session_details_result = session.sql(f"""
            SELECT NOMBRE_SESION, FECHA_SESION, LINK_SESION_INFORMATIVA 
            FROM LABORATORIO.MONICA_SOBERON.SESION 
            WHERE NOMBRE_SESION = '{selected_session}';
        """)
        session_details_df = session_details_result.to_pandas()
        
        # Query for the session ID
        session_id_result = session.sql(f"""
            SELECT ID_SESION 
            FROM LABORATORIO.MONICA_SOBERON.SESION 
            WHERE NOMBRE_SESION = '{selected_session}';
        """)
        session_id_df = session_id_result.to_pandas()
        id_sesion = session_id_df['ID_SESION'].iloc[0]
        
        # Display the session details in a form for editing
        with st.form(key='edit_session_form'):
            new_session_name = st.text_input("Nombre de la Sesión", session_details_df['NOMBRE_SESION'].iloc[0])
            new_session_date = st.date_input("Fecha de la Sesión", session_details_df['FECHA_SESION'].iloc[0])
            new_link_info = st.text_input("Enlace de Información de la Sesión", session_details_df['LINK_SESION_INFORMATIVA'].iloc[0])
            
            update_button = st.form_submit_button(label='Actualizar Sesión')
            
            if update_button:
                # Update the session details in the database
                update_query = f"""
                UPDATE LABORATORIO.MONICA_SOBERON.SESION 
                SET NOMBRE_SESION = '{new_session_name}', 
                    FECHA_SESION = '{new_session_date}', 
                    LINK_SESION_INFORMATIVA = '{new_link_info}'
                WHERE ID_SESION = {id_sesion};
                """
                session.sql(update_query).collect()
                st.success(f"Sesión '{new_session_name}' actualizada con éxito.")


with tabs[2]:
    st.header("Lista de Invitados")

    # Query for session information
    session_result = session.sql("SELECT NOMBRE_SESION FROM LABORATORIO.MONICA_SOBERON.SESION;")
    session_df = session_result.to_pandas()
    session_names = session_df['NOMBRE_SESION'].tolist()

        # Display session select box
    selected_session = st.selectbox('Selecciona una Sesión:', session_names)
    if selected_session:
        # Query for session details based on the selected session
        session_details_result = session.sql(f"SELECT NOMBRE_SESION, FECHA_SESION, LINK_SESION_INFORMATIVA FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{selected_session}';")
        id_sesion = session.sql(f"select id_sesion FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{selected_session}';")
    
        session_details_df = session_details_result.to_pandas()
        session_id_result = session.sql(f"""
            SELECT ID_SESION
            FROM LABORATORIO.MONICA_SOBERON.SESION
            WHERE NOMBRE_SESION = '{selected_session}';
                """)
        session_id_df = session_id_result.to_pandas()
        id_sesion = session_id_df['ID_SESION'].iloc[0]
            
            # Display the session details as a list
        st.write("**Detalles de la Sesión:**")
        for index, row in session_details_df.iterrows():
                st.write(f" Nombre de la Sesión: {row['NOMBRE_SESION']}")
                st.write(f" Fecha de la Sesión: {row['FECHA_SESION']}")

        invitado_result = session.sql(f"""
            SELECT c.NOMBRE, c.APELLIDO, c.CORREO 
            FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD c
            JOIN LABORATORIO.MONICA_SOBERON.INVITACION_SESION r
            ON c.ID_USUARIO = r.ID_USUARIO
            WHERE r.ID_SESION = {id_sesion};
        """)
        
        invi_df = invitado_result.to_pandas()
        
        # Display the registered users
        st.write("**Usuarios Invitados:**")
        st.dataframe(invi_df)

        st.write("Agregar usuarios invitados.")
        email_input = st.text_area(
            "Pega la lista de correos electrónicos aquí (uno por línea):",
            height=300
        )       

        if st.button("Procesar Correos", key= "process2"):
# Procesar los correos electrónicos
            assistant_email_list = [email.replace(chr(10), '').replace(chr(13), '').strip().lower() for email in email_input.split('\n') if email.strip()]
            
            # Remover duplicados
            assistant_email_list = list(set(assistant_email_list))
            
            # Consultar correos existentes en la tabla comunidad
            if assistant_email_list:
                email_list_str = ', '.join(f"'{email}'" for email in assistant_email_list)
                existing_emails_query = f"SELECT correo FROM LABORATORIO.MONICA_SOBERON.comunidad WHERE correo IN ({email_list_str})"
                existing_emails = session.sql(existing_emails_query).collect()
                existing_email_set = set(email['CORREO'] for email in existing_emails)  # Ajuste del nombre de la columna

                # Correos no encontrados
                not_found_emails = [email for email in assistant_email_list if email not in existing_email_set]

                # Si hay correos no encontrados, se muestra el formulario para registrar nuevos usuarios
                if not_found_emails:
                    st.session_state.not_found_emails = not_found_emails
                    st.session_state.show_popup = True
                    st.warning("Algunos correos no están registrados en la comunidad. Regístralos antes de continuar.")
                else:
                    # Insertar los correos que sí están en la comunidad
                    insert_query = f"""
                        INSERT INTO invitacion_sesion (id_sesion, id_usuario)
                        SELECT {id_sesion}, c.id_usuario
                        FROM LABORATORIO.MONICA_SOBERON.comunidad c
                        WHERE c.correo IN ({email_list_str})
                        AND NOT EXISTS (
                            SELECT 1
                            FROM invitacion_sesion i
                            WHERE i.id_sesion = {id_sesion} AND i.id_usuario = c.id_usuario
                        );
                    """

                    session.sql(insert_query).collect()
                    st.success("Correos electrónicos nuevos procesados y subidos a la base de datos.")

                # Mostrar correos procesados
                df_assistants = pd.DataFrame(assistant_email_list, columns=['Correo'])
                st.write("Correos electrónicos de asistentes procesados:")
                st.dataframe(df_assistants)

        # Mostrar formulario para registrar nuevos usuarios si hay correos no encontrados
        if st.session_state.get('show_popup', False):
            st.write("Los siguientes correos no se encontraron en la comunidad:")
            st.write(", ".join(st.session_state.not_found_emails))
            
            # Loop over not found emails
            for email in st.session_state.not_found_emails:
                with st.form(f"register_{email}"):
                    # Get the name and surname using text input fields
                    nombre = st.text_input(f"Nombre para {email}", key=f"nombre_{email}")
                    apellido = st.text_input(f"Apellido para {email}", key=f"apellido_{email}")
                    correo = email  

                    if st.form_submit_button(f"Registrar {email}"):
                        # Validate that both nombre and apellido are filled
                        if not nombre or not apellido:
                            st.warning(f"Por favor, completa todos los campos para {email}.")
                        else:
                            # Try inserting into the database
                            try:
                                insert_user_query = f"""
                                INSERT INTO LABORATORIO.MONICA_SOBERON.comunidad (nombre, apellido, correo, status)
                                VALUES ('{nombre}', '{apellido}', '{correo}', TRUE);
                                """
                                # Debug: Check the SQL query
                                st.write(f"SQL Query: {insert_user_query}")
                                
                                # Execute the query
                                session.sql(insert_user_query).collect()

                                # Debug: Display success message
                                st.success(f"Usuario {nombre} {apellido} registrado con éxito.")
                                
                            except Exception as e:
                                # Display error message if SQL fails
                                st.error(f"Error al registrar el usuario {correo}: {e}")
                            
                            # Reiniciar estado del popup después de procesar
                            st.session_state.show_popup = False

with tabs[3]:
    st.header("Lista de Usuarios que Asistieron")
    
    # Query for session information
    session_result = session.sql("SELECT NOMBRE_SESION FROM LABORATORIO.MONICA_SOBERON.SESION;")
    session_df = session_result.to_pandas()
    session_names = session_df['NOMBRE_SESION'].tolist()

    # Display session select box
    selected_session = st.selectbox('Selecciona una Sesión: ', session_names, key = 'names')
    if selected_session:
        # Query for session details based on the selected session
        session_details_result = session.sql(f"SELECT NOMBRE_SESION, FECHA_SESION, LINK_SESION_INFORMATIVA FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{selected_session}';")
        session_id_result = session.sql(f"SELECT ID_SESION FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{selected_session}';")
        
        session_details_df = session_details_result.to_pandas()
        session_id_df = session_id_result.to_pandas()
        id_sesion = session_id_df['ID_SESION'].iloc[0]
            
        # Display the session details as a list
        st.write("**Detalles de la Sesión:**")
        for index, row in session_details_df.iterrows():
            st.write(f" Nombre de la Sesión: {row['NOMBRE_SESION']}")
            st.write(f" Fecha de la Sesión: {row['FECHA_SESION']}")
            
        asis_result = session.sql(f"""
            SELECT c.NOMBRE, c.APELLIDO, c.CORREO 
            FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD c
            JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION r
            ON c.ID_USUARIO = r.ID_USUARIO
            WHERE r.ID_SESION = {id_sesion};
        """)
        
        asis_df = asis_result.to_pandas()
        
        # Display the registered users
        st.write("**Usuarios que Asistieron:**")
        st.dataframe(asis_df)

        st.write("Agregar usuarios que asistieron.")

        assistant_email_input = st.text_area(
            "Pega la lista de correos electrónicos de los usuarios que asistieron aquí (uno por línea):",
            height=300
        )

        if st.button("Procesar Correos Asistentes", key = "process3"):
            # Process the input emails
            assistant_email_list = [email.replace(chr(10), '').replace(chr(13), '').strip().lower() for email in assistant_email_input.split('\n') if email.strip()]

            # Remove duplicates
            assistant_email_list = list(set(assistant_email_list))
            
            df_assistants = pd.DataFrame(assistant_email_list, columns=['Correo'])

            # Display the processed emails
            st.write("Correos electrónicos de asistentes procesados:")
            st.dataframe(df_assistants)
            
            # Insert into the database
            insert_query = f"""
            INSERT INTO asistencia_sesion (id_sesion, id_usuario)
            SELECT {id_sesion}, c.id_usuario
            FROM comunidad c
            WHERE c.correo IN ({', '.join(f"'{email}'" for email in assistant_email_list)})
            AND NOT EXISTS (
                SELECT 1
                FROM asistencia_sesion a
                WHERE a.id_sesion = {id_sesion} AND a.id_usuario = c.id_usuario
            );
            """

            # Execute the query
            session.sql(insert_query).collect()
            st.success("Asistencias registradas con éxito.")    

with tabs[4]:
    st.header("Borrar Sesión")
    st.write("Solo se permite borrar sesiones de la base de datos si estos no tienen invitaciones o asistencias.")
    st.write("Borrar una sesion es algo definitivo.")

    session_result = session.sql("SELECT NOMBRE_SESION FROM LABORATORIO.MONICA_SOBERON.SESION;")
    session_df = session_result.to_pandas()
    session_names = session_df['NOMBRE_SESION'].tolist()

    # Display session select box
    selected_session = st.selectbox('Selecciona una Sesión: ', session_names, key ='names2')
    if selected_session:
        # Query for session details based on the selected session
        session_details_result = session.sql(f"SELECT NOMBRE_SESION, FECHA_SESION, LINK_SESION_INFORMATIVA FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{selected_session}';")
        session_id_result = session.sql(f"SELECT ID_SESION FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{selected_session}';")
        
        session_details_df = session_details_result.to_pandas()
        session_id_df = session_id_result.to_pandas()
        id_sesion = session_id_df['ID_SESION'].iloc[0]
            
        # Display the session details as a list
        st.write("**Detalles de la Sesión:**")
        for index, row in session_details_df.iterrows():
            st.write(f" Nombre de la Sesión: {row['NOMBRE_SESION']}")
            st.write(f" Fecha de la Sesión: {row['FECHA_SESION']}")

        seguro = st.checkbox("Estoy seguro de que quiero eliminar esta sesion.")

        checkdata = session.sql(f"""
        SELECT 
            (SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.INVITACION_SESION WHERE ID_SESION = '{id_sesion}') AS INVITADOS_COUNT,
            (SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION WHERE ID_sESION = '{id_sesion}') AS ASISTENTES_COUNT
        """)

        checkdata_df = checkdata.to_pandas()

        if seguro:
            if checkdata_df.iloc[0]['INVITADOS_COUNT'] == 0 and checkdata_df.iloc[0]['ASISTENTES_COUNT'] == 0:
                borrar = st.button('Eliminar Sesión', key="processS")
                if borrar:
                    session.sql(f"DELETE FROM LABORATORIO.MONICA_SOBERON.SESION WHERE ID_SESION = '{id_sesion}';").collect()
                    st.success(f"La sesion ha sido eliminado exitosamente.")
            else:
                st.error("Esta sesion no se puede eliminar porque tiene clases, invitados, o usuarios registrados asociados.")

