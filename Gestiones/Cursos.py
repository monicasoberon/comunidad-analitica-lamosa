import os
import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col

cnx = st.connection("snowflake")
session = cnx.session()

if "auth_data" not in st.session_state:
    st.write("Please authenticate to access this page.")
    st.stop()  # Stop the execution of this page
    
# Set up the Streamlit app
st.title("Gestión de Cursos")
st.write("Esta aplicación te ayuda a gestionar cursos, invitados y sesiones.")

@st.cache_data
def get_course_names():
    nombres_result = session.sql("""
        SELECT n.NOMBRE_CURSO, c.ID_CURSO, c.FECHA_INICIO, c.FECHA_FIN
        FROM LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS AS n 
        INNER JOIN LABORATORIO.MONICA_SOBERON.CURSO AS c ON n.ID_CATALOGO = c.ID_CATALOGO;
    """)
    nombres_df = nombres_result.to_pandas()

    nombres_df['FECHA_INICIO'] = pd.to_datetime(nombres_df['FECHA_INICIO'], errors='coerce').dt.strftime('%d/%m/%Y')
    nombres_df['FECHA_FIN'] = pd.to_datetime(nombres_df['FECHA_FIN'], errors='coerce').dt.strftime('%d/%m/%Y')

    # Combine course name with start and end dates for display
    nombres_df['course_name_with_dates'] = nombres_df.apply(
        lambda row: f"{row['NOMBRE_CURSO']} ({row['FECHA_INICIO']} - {row['FECHA_FIN']})" 
        if pd.notnull(row['FECHA_INICIO']) and pd.notnull(row['FECHA_FIN']) 
        else f"{row['NOMBRE_CURSO']} (Fecha no disponible)", axis=1
    )
    return nombres_df

# Create tabs
tabs = st.tabs(["Crear Curso", "Editar Curso", "Lista de Invitados", "Lista de Registrados", "Bajas y Finalizaciones", "Borrar Curso"])

with tabs[0]:
    st.header("Crear Nuevo Curso")

    with st.form(key='new_course_form'):

        nombres = session.sql("""SELECT NOMBRE_CURSO FROM LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS;""")
        course_name = st.selectbox("Nombre del Curso", nombres.to_pandas()['NOMBRE_CURSO'].tolist())
        course_name_id = session.sql(f"SELECT ID_CATALOGO FROM LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS WHERE NOMBRE_CURSO = '{course_name}';").to_pandas()['ID_CATALOGO'].iloc[0]
        course_start_date = st.date_input("Fecha de Inicio")
        course_end_date = st.date_input("Fecha de Fin")
        course_provider = st.text_input("Proveedor")
        requires_case = st.checkbox("¿Requiere Caso de Uso?")
        course_contact_email = st.text_input("Correo de Contacto")
        
        # Select multiple sessions for the course
        session_result = session.sql("SELECT ID_SESION, NOMBRE_SESION FROM LABORATORIO.MONICA_SOBERON.SESION;")
        session_df = session_result.to_pandas()
        session_names = session_df['NOMBRE_SESION'].tolist()
        selected_sessions = st.multiselect('Selecciona las Sesiones:', session_names)
        
        # Instructor dropdown
        instructor_result = session.sql("SELECT ID_INSTRUCTOR, NOMBRE_INSTRUCTOR, APELLIDO_INSTRUCTOR FROM LABORATORIO.MONICA_SOBERON.INSTRUCTOR;")
        instructor_df = instructor_result.to_pandas()
        instructor_names = [f"{row['NOMBRE_INSTRUCTOR']} {row['APELLIDO_INSTRUCTOR']}" for index, row in instructor_df.iterrows()]
        selected_instructor = st.selectbox("Selecciona el Instructor del Curso:", instructor_names)
        
        submit_button2 = st.form_submit_button(label='Crear Curso')
        
        if submit_button2:
            if course_name and course_start_date and course_end_date:
                # Insert new course into the CURSO table
                insert_course_query = f"""
                INSERT INTO LABORATORIO.MONICA_SOBERON.CURSO (ID_CATALOGO, FECHA_INICIO, FECHA_FIN, PROVEEDOR, REQUIERE_CASO_USO, CORREO_CONTACTO)
                VALUES ('{course_name_id}', '{course_start_date}', '{course_end_date}', '{course_provider}', {requires_case}, '{course_contact_email}');
                """
                session.sql(insert_course_query).collect()

                # Get the ID of the newly inserted course
                course_id_result = session.sql(f"""SELECT ID_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO as c
                                               INNER JOIN LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS as n
                                               ON n.ID_CATALOGO = c.ID_CATALOGO
                                               WHERE n.NOMBRE_CURSO = '{course_name}';""")
                course_id_df = course_id_result.to_pandas()
                course_id = course_id_df['ID_CURSO'].iloc[0]
                
                # Insert selected sessions into the TIENE_SESION table
                for session_name in selected_sessions:
                    session_id_result = session.sql(f"SELECT ID_SESION FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{session_name}';")
                    session_id_df = session_id_result.to_pandas()
                    session_id = session_id_df['ID_SESION'].iloc[0]  
                    insert_session_query = f"""
                    INSERT INTO LABORATORIO.MONICA_SOBERON.TIENE_SESION (ID_CURSO, ID_SESION)
                    VALUES ({course_id}, {session_id});
                    """
                    session.sql(insert_session_query).collect()

                # Insert the instructor-course relationship into the IMPARTE table
                instructor_id_result = session.sql(f"""
                SELECT ID_INSTRUCTOR FROM LABORATORIO.MONICA_SOBERON.INSTRUCTOR
                WHERE CONCAT(NOMBRE_INSTRUCTOR, ' ', APELLIDO_INSTRUCTOR) = '{selected_instructor}';
                """)
                instructor_id_df = instructor_id_result.to_pandas()
                instructor_id = instructor_id_df['ID_INSTRUCTOR'].iloc[0]
                
                insert_instructor_query = f"""
                INSERT INTO LABORATORIO.MONICA_SOBERON.IMPARTE (ID_INSTRUCTOR, ID_CURSO)
                VALUES ({instructor_id}, {course_id});
                """
                session.sql(insert_instructor_query).collect()

                st.success(f"Curso '{course_name}' creado con éxito y asociado a las sesiones seleccionadas.")
            else:
                st.error("Por favor, completa toda la información del curso.")

    with st.form(key='new_course'):
        st.write("Si el curso no esta listado como opción en los nombres, añadelo aquí.")
        course_name = st.text_input("Nombre del Curso Nuevo a Añadir a la Base de Datos")

        submit_button = st.form_submit_button(label='Crear Curso')
        
        if submit_button:
            if course_name:
                insert_course_name_query = f"""
                INSERT INTO LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS (NOMBRE_CURSO)
                VALUES ('{course_name}');
                """
                session.sql(insert_course_name_query).collect()
                st.success("Curso creado y añadido al catálogo de cursos!")

# Sección para editar un curso existente
with tabs[1]:
    st.header("Editar Curso Existente")
    
    nombres_df = get_course_names()
    # Check if the DataFrame is empty before accessing it
    if nombres_df.empty:
        st.error("No se encontraron cursos.")
    else:
        selected_course_name_with_dates = st.selectbox("Selecciona el Curso:", nombres_df['course_name_with_dates'], key='select1')

        # Get the ID_CURSO for the selected course
        selected_course_id = nombres_df.loc[nombres_df['course_name_with_dates'] == selected_course_name_with_dates, 'ID_CURSO'].values[0]

        # Fetch course details and allow editing if a course is selected
        if selected_course_id:
            # Get the details of the selected course
            course_details_result = session.sql(f"SELECT * FROM LABORATORIO.MONICA_SOBERON.CURSO WHERE ID_CURSO = {selected_course_id};")
            course_details_df = course_details_result.to_pandas()
            course_details = course_details_df.iloc[0]
        
        st.write("**Actualización de Datos del Curso:**")
        with st.form(key='edit_course_form'):

            course_name_result = session.sql(f"""SELECT NOMBRE_CURSO FROM LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS n inner 
                                             join LABORATORIO.MONICA_SOBERON.CURSO c on n.ID_CATALOGO = c.ID_CATALOGO WHERE c.id_curso = {selected_course_id};""")
            course_name_df = course_name_result.to_pandas()
            
            new_course_start_date = st.date_input("Fecha de Inicio", value=course_details['FECHA_INICIO'])
            new_course_end_date = st.date_input("Fecha de Fin", value=course_details['FECHA_FIN'])
            new_course_provider = st.text_input("Proveedor", value=course_details['PROVEEDOR'])
            new_requires_case = st.checkbox("¿Requiere Caso de Uso?", value=course_details['REQUIERE_CASO_USO'])  # Boolean remains as is
            new_course_contact_email = st.text_input("Correo de Contacto", value=course_details['CORREO_CONTACTO'])
            
            # Select multiple sessions for the course
            session_result = session.sql("SELECT ID_SESION, NOMBRE_SESION FROM LABORATORIO.MONICA_SOBERON.SESION;")
            session_df = session_result.to_pandas()
            session_names = session_df['NOMBRE_SESION'].tolist()
            
            # Fetch current sessions for the selected course
            course_sessions_result = session.sql(f"""
                SELECT s.NOMBRE_SESION
                FROM LABORATORIO.MONICA_SOBERON.TIENE_SESION ts
                JOIN LABORATORIO.MONICA_SOBERON.SESION s ON ts.ID_SESION = s.ID_SESION
                WHERE ts.ID_CURSO = {selected_course_id};
            """)
            course_sessions_df = course_sessions_result.to_pandas()
            current_sessions = course_sessions_df['NOMBRE_SESION'].tolist()
            
            selected_sessions = st.multiselect('Selecciona las Sesiones:', session_names, default=current_sessions)
            
            # Instructor dropdown
            instructor_result = session.sql("SELECT ID_INSTRUCTOR, NOMBRE_INSTRUCTOR, APELLIDO_INSTRUCTOR FROM LABORATORIO.MONICA_SOBERON.INSTRUCTOR;")
            instructor_df = instructor_result.to_pandas()
            instructor_names = [f"{row['NOMBRE_INSTRUCTOR']} {row['APELLIDO_INSTRUCTOR']}" for index, row in instructor_df.iterrows()]
            selected_instructor = st.selectbox("Selecciona el Instructor del Curso:", instructor_names, key='select2')
            
            update_button = st.form_submit_button(label='Actualizar Curso')
            
            if update_button:
                # Build dynamic update query based on changes
                update_fields = []

                if new_course_start_date != course_details['FECHA_INICIO']:
                    update_fields.append(f"FECHA_INICIO = '{new_course_start_date}'")
                
                if new_course_end_date != course_details['FECHA_FIN']:
                    update_fields.append(f"FECHA_FIN = '{new_course_end_date}'")
                
                if new_course_provider != course_details['PROVEEDOR']:
                    update_fields.append(f"PROVEEDOR = '{new_course_provider}'")
                
                if new_requires_case != course_details['REQUIERE_CASO_USO']:
                    update_fields.append(f"REQUIERE_CASO_USO = {new_requires_case}")
                
                if new_course_contact_email != course_details['CORREO_CONTACTO']:
                    update_fields.append(f"CORREO_CONTACTO = '{new_course_contact_email}'")

                if update_fields:
                    update_course_query = f"""
                    UPDATE LABORATORIO.MONICA_SOBERON.CURSO
                    SET {', '.join(update_fields)}
                    WHERE ID_CURSO = {selected_course_id};
                    """
                    session.sql(update_course_query).collect()

                # Update the TIENE_SESION table
                session_id_result = session.sql(f"""
                SELECT ID_SESION 
                FROM LABORATORIO.MONICA_SOBERON.SESION 
                WHERE NOMBRE_SESION IN ({', '.join([f"'{s}'" for s in selected_sessions])});
                """)
                session_id_df = session_id_result.to_pandas()
                session_ids = session_id_df['ID_SESION'].tolist()
                
                # Remove existing session links
                delete_sessions_query = f"DELETE FROM LABORATORIO.MONICA_SOBERON.TIENE_SESION WHERE ID_CURSO = {selected_course_id};"
                session.sql(delete_sessions_query).collect()
                
                # Insert new session links
                for session_id in session_ids:
                    insert_session_query = f"""
                    INSERT INTO LABORATORIO.MONICA_SOBERON.TIENE_SESION (ID_CURSO, ID_SESION)
                    VALUES ({selected_course_id}, {session_id});
                    """
                    session.sql(insert_session_query).collect()

                # Update instructor
                instructor_id_result = session.sql(f"""
                SELECT ID_INSTRUCTOR FROM LABORATORIO.MONICA_SOBERON.INSTRUCTOR
                WHERE CONCAT(NOMBRE_INSTRUCTOR, ' ', APELLIDO_INSTRUCTOR) = '{selected_instructor}';
                """)
                instructor_id_df = instructor_id_result.to_pandas()
                instructor_id = instructor_id_df['ID_INSTRUCTOR'].iloc[0]
                
                update_instructor_query = f"""
                UPDATE LABORATORIO.MONICA_SOBERON.IMPARTE
                SET ID_INSTRUCTOR = {instructor_id}
                WHERE ID_CURSO = {selected_course_id};
                """
                session.sql(update_instructor_query).collect()

                st.success(f"Curso actualizado con éxito.")


with tabs[2]:
    st.header("Registrar Invitados")
    
    nombres_df = get_course_names()

    # Use the selectbox to display the combined name and dates
    selected_course_name_with_dates = st.selectbox("Selecciona el Curso:", nombres_df['course_name_with_dates'], key='select3')

    # Get the ID_CURSO for the selected course
    selected_course_id = nombres_df.loc[nombres_df['course_name_with_dates'] == selected_course_name_with_dates, 'ID_CURSO'].values[0]

    # Query for course details based on the selected course
    course_details_result = session.sql(f"""
        SELECT n.NOMBRE_CURSO, c.FECHA_INICIO, c.FECHA_FIN, c.PROVEEDOR 
        FROM LABORATORIO.MONICA_SOBERON.CURSO c inner join
        LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS n 
        ON c.ID_CATALOGO = n.ID_CATALOGO
        WHERE c.ID_CURSO = '{selected_course_id}';
    """)
    id_curso = selected_course_id

    course_details_df = course_details_result.to_pandas()

    # Display the course details
    st.write("**Detalles del Curso:**")
    for index, row in course_details_df.iterrows():
        st.write(f"Nombre del Curso: {row['NOMBRE_CURSO']}")
        st.write(f"Fecha de Inicio: {row['FECHA_INICIO']}")
        st.write(f"Fecha de Fin: {row['FECHA_FIN']}")
        st.write(f"Proveedor: {row['PROVEEDOR']}")

    # Query for registered users for the selected course
    invitados_result = session.sql(f"""
        SELECT c.NOMBRE, c.APELLIDO, c.CORREO 
        FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD c
        JOIN LABORATORIO.MONICA_SOBERON.INVITACION_CURSO r
        ON c.ID_USUARIO = r.ID_USUARIO
        WHERE r.ID_CURSO = '{id_curso}'
    """)
    
    inv_df = invitados_result.to_pandas()
    
    # Display the registered users
    st.write("**Usuarios Invitados:**")
    st.dataframe(inv_df)

    st.write("**Agregar Usuarios:**")
        
    email_input = st.text_area(
        "Pega la lista de correos electrónicos aquí (uno por línea):",
        height=300,
        key="text4"
    )
    
    if st.button("Procesar Correos de Invitados", key = "process4"):
        # Process the input emails
        email_list = [email.strip().lower() for email in email_input.split('\n') if email.strip()]
        email_list = list(set(email_list))  # Remove duplicates
        
        df_invitados = pd.DataFrame(email_list, columns=['Correo'])
        
        # Display the processed emails
        st.write("Correos electrónicos de invitados procesados:")
        st.dataframe(df_invitados)

        for email in email_list:
        # Get the user ID for the email
            user_id_result = session.sql(f"""
            SELECT ID_USUARIO 
            FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD 
            WHERE CORREO = '{email}';
        """)
            user_id_df = user_id_result.to_pandas()

            if not user_id_df.empty:
                user_id = user_id_df['ID_USUARIO'].iloc[0]

                insert_query = f"""
                    INSERT INTO LABORATORIO.MONICA_SOBERON.INVITACION_CURSO (ID_CURSO, ID_USUARIO)
                    SELECT {id_curso}, {user_id}
                    WHERE NOT EXISTS (
                    SELECT 1 
                    FROM LABORATORIO.MONICA_SOBERON.INVITACION_CURSO 
                    WHERE ID_CURSO = {id_curso} 
                    AND ID_USUARIO = {user_id}
                );
                """
                session.sql(insert_query).collect()

                st.success("Usuarios invitados nuevos agregados con éxito.")

with tabs[3]:
    st.header("Lista de Usuarios Registrados")
    
    nombres_df = get_course_names()

    selected_course_name_with_dates = st.selectbox("Selecciona el Curso:", nombres_df['course_name_with_dates'], key='select4')

    id_curso = nombres_df.loc[nombres_df['course_name_with_dates'] == selected_course_name_with_dates, 'ID_CURSO'].values[0]

    course_details_result = session.sql(f"""
        SELECT n.NOMBRE_CURSO, c.FECHA_INICIO, c.FECHA_FIN, c.PROVEEDOR 
        FROM LABORATORIO.MONICA_SOBERON.CURSO c inner join
        LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS n 
        ON c.ID_CATALOGO = n.ID_CATALOGO
        WHERE c.ID_CURSO = '{id_curso}';
    """)

    course_details_df = course_details_result.to_pandas()
    
    # Display the course details
    st.write("**Detalles del Curso:**")
    for index, row in course_details_df.iterrows():
        st.write(f"Nombre del Curso: {row['NOMBRE_CURSO']}")
        st.write(f"Fecha de Inicio: {row['FECHA_INICIO']}")
        st.write(f"Fecha de Fin: {row['FECHA_FIN']}")
        st.write(f"Proveedor: {row['PROVEEDOR']}")
    
    # Query for registered users for the selected course
    registered_result = session.sql(f"""
        SELECT c.NOMBRE, c.APELLIDO, c.CORREO 
        FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD c
        JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO r
        ON c.ID_USUARIO = r.ID_USUARIO
        WHERE r.ID_CURSO = '{id_curso}';
    """)
    
    registered_df = registered_result.to_pandas()
    
    # Display the registered users
    st.write("**Usuarios Registrados:**")
    st.dataframe(registered_df)

    st.write("**Agregar Usuarios:**")
    email_input2 = st.text_area(
"Pega la lista de correos electrónicos aquí (uno por línea):",
height=300,  key='email_input_key'  
)

    if st.button("Procesar Correos de Registrados", key = "process5"):
# Process the input emails
        email_list = [email.strip().lower() for email in email_input2.split('\n') if email.strip()]
        email_list = list(set(email_list))  # Remove duplicates

        if email_list:
            df_registrados = pd.DataFrame(email_list, columns=['Correo'])

    # Display the processed emails
            st.write("Correos electrónicos de registrados procesados:")
            st.dataframe(df_registrados)


    # Insert into the REGISTRADOS_CURSO table for each email
            for email in email_list:
        # Get the user ID for the email
                user_id_result = session.sql(f"""
            SELECT ID_USUARIO 
            FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD 
            WHERE CORREO = '{email}';
        """)
                user_id_df = user_id_result.to_pandas()

                if not user_id_df.empty:
                    user_id = user_id_df['ID_USUARIO'].iloc[0]

            # Insert the user and course relationship into REGISTRADOS_CURSO
                    insert_query = f"""
                        INSERT INTO LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO (ID_CURSO, ID_USUARIO)
                        SELECT {id_curso}, {user_id}
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO r
                            WHERE r.ID_CURSO = {id_curso} AND r.ID_USUARIO = {user_id}
                        );
                    """

                    session.sql(insert_query).collect()

            st.success("Usuarios registrados agregados con éxito.")

with tabs[4]:
    st.header("Dar de Baja o Finalizar Curso")
    st.write("En esta sección puede índicar que usuarios se han dado de baja y que usuarios han finalizado un curso.")

    nombres_df = get_course_names()

    # Use the selectbox to display the combined name and dates
    selected_course_name_with_dates = st.selectbox("Selecciona el Curso:", nombres_df['course_name_with_dates'], key='select31')

    id_curso = nombres_df.loc[nombres_df['course_name_with_dates'] == selected_course_name_with_dates, 'ID_CURSO'].values[0]

    # Execute the SQL query to fetch registered users for the course
    people = session.sql(f"""
        SELECT r.id_usuario, c.correo, c.nombre, c.apellido, r.status, r.curso_aprobado
        FROM registrados_curso as r
        INNER JOIN comunidad as c ON r.id_usuario = c.id_usuario
        WHERE r.id_curso = {id_curso};
    """)

    # Convert the result to a pandas DataFrame
    people_df = people.to_pandas()

    # Create a DataFrame with editable columns for user data
    user_data = pd.DataFrame(
        [{"Correo": row['CORREO'], "Nombre": row['NOMBRE'], "Apellido": row['APELLIDO'],  "Status": row['STATUS'], "Finalizado": row['CURSO_APROBADO']}
        for index, row in people_df.iterrows()]
    )

    # Use st.data_editor to allow editing the data
    edited_user_data = st.data_editor(
        user_data,
        use_container_width=True  # Allow better UI scaling
    )

    # Button to submit the updated data
    if st.button("Registrar Usuarios", key="submit_users_bf"):
        for index, row in edited_user_data.iterrows():
            # Assuming you want to update the `STATUS` and `CURSO_APROBADO` in the database
            update_query = f"""
            UPDATE LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO
            SET CURSO_APROBADO = {1 if row['Finalizado'] else 0},  
                STATUS = {1 if row['Status'] else 0}
            WHERE ID_USUARIO = {people_df.iloc[index]['id_usuario']} AND ID_CURSO = {id_curso};
            """
            try:
                # Execute the SQL update query
                session.sql(update_query).collect()  
                st.success(f"Usuario {row['Nombre']} registrado con éxito.")
            except Exception as e:
                st.error(f"Error al registrar {row['Nombre']}: {e}")

with tabs[5]:
    st.header("Borrar Curso")
    st.write("Solo se permite borrar cursos de la base de datos si estos no tienen clases, usuarios invitados o usuarios registrados.")
    st.write("Borrar un curso es algo definitivo.")

    nombres_df = get_course_names()
    
    # Use the selectbox to display the combined name and dates
    selected_course_name_with_dates = st.selectbox("Selecciona el Curso:", nombres_df['course_name_with_dates'], key='select9')

    # Get the ID_CURSO for the selected course
    id_curso = nombres_df.loc[nombres_df['course_name_with_dates'] == selected_course_name_with_dates, 'ID_CURSO'].values[0]

    # Query for course details based on the selected course
    course_details_result = session.sql(f"""
        SELECT n.NOMBRE_CURSO, c.FECHA_INICIO, c.FECHA_FIN, c.PROVEEDOR, c.CORREO_CONTACTO, c.REQUIERE_CASO_USO
        FROM LABORATORIO.MONICA_SOBERON.CURSO c inner join
        LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS n 
        ON c.ID_CATALOGO = n.ID_CATALOGO
        WHERE c.ID_CURSO = '{id_curso}';
    """)

    course_details_df = course_details_result.to_pandas()

    # Display the course details
    st.write("**Detalles del Curso:**")
    for index, row in course_details_df.iterrows():
        st.write(f"Nombre del Curso: {row['NOMBRE_CURSO']}")
        st.write(f"Fecha de Inicio: {row['FECHA_INICIO']}")
        st.write(f"Fecha de Fin: {row['FECHA_FIN']}")
        st.write(f"Proveedor: {row['PROVEEDOR']}")
        st.write(f"Correo Contacto: {row['CORREO_CONTACTO']}")
        st.write(f"Requiere Caso de Uso: {'Si' if row['REQUIERE_CASO_USO'] else 'No'}")

    seguro = st.checkbox("Estoy seguro de que quiero eliminar este curso.")

    # Check if the course is associated with any classes, invited users, or registered users
    checkdata = session.sql(f"""
    SELECT 
        (SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.CLASE WHERE ID_CURSO = '{id_curso}') AS CLASES_COUNT,
        (SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.INVITACION_CURSO WHERE ID_CURSO = '{id_curso}') AS INVITADOS_COUNT,
        (SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO WHERE ID_CURSO = '{id_curso}') AS REGISTRADOS_COUNT
    """)

    checkdata_df = checkdata.to_pandas()

    if seguro:
        if checkdata_df.iloc[0]['CLASES_COUNT'] == 0 and checkdata_df.iloc[0]['INVITADOS_COUNT'] == 0 and checkdata_df.iloc[0]['REGISTRADOS_COUNT'] == 0:
            borrar = st.button('Eliminar Curso', key="processC")
            if borrar:
                session.sql(f"DELETE FROM LABORATORIO.MONICA_SOBERON.CURSO WHERE ID_CURSO = '{id_curso}';").collect()
                st.success(f"El curso ha sido eliminado exitosamente.")
        else:
            st.error("Este curso no se puede eliminar porque tiene clases, invitados, o usuarios registrados asociados.")
