import os
import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col

cnx = st.connection("snowflake")
session = cnx.session()

if "auth_data" not in st.session_state:
    st.write("Please authenticate to access this page.")
    st.stop()  # Stop the execution of this page

st.title("Registrar Clases y Asistencias")

tab1, tab2 = st.tabs(["Registrar Clase", "Registrar Asistencia"])

with tab1:
    st.header("Registrar Clase")

    nombres_result = session.sql("""
    SELECT n.NOMBRE_CURSO, c.ID_CURSO, c.FECHA_INICIO, c.FECHA_FIN
    FROM LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS AS n 
    INNER JOIN LABORATORIO.MONICA_SOBERON.CURSO AS c ON n.ID_CATALOGO = c.ID_CATALOGO;
    """)
    nombres_df = nombres_result.to_pandas()

    nombres_df['FECHA_INICIO'] = pd.to_datetime(nombres_df['FECHA_INICIO'], errors='coerce').dt.strftime('%Y/%m/%d')
    nombres_df['FECHA_FIN'] = pd.to_datetime(nombres_df['FECHA_FIN'], errors='coerce').dt.strftime('%Y/%m/%d')

    # Combine course name with start and end dates for display
    nombres_df['course_name_with_dates'] = nombres_df.apply(
        lambda row: f"{row['NOMBRE_CURSO']} ({row['FECHA_INICIO']} - {row['FECHA_FIN']})" 
        if pd.notnull(row['FECHA_INICIO']) and pd.notnull(row['FECHA_FIN']) 
        else f"{row['NOMBRE_CURSO']} (Fecha no disponible)", axis=1
    )

    # Use the selectbox to display the combined name and dates
    selected_course_name_with_dates = st.selectbox("Selecciona el Curso:", nombres_df['course_name_with_dates'], key='selectc')

    # Get the ID_CURSO for the selected course
    id_curso = nombres_df.loc[nombres_df['course_name_with_dates'] == selected_course_name_with_dates, 'ID_CURSO'].values[0]

    st.write("Registrar una clase")
    fecha_clase =st.date_input("Fecha de la Clase")

    if st.button("Crear Clase", key="process6"):
        if fecha_clase:
        # Convertir la fecha al formato adecuado para SQL
            fecha_clase_str = fecha_clase.strftime('%Y-%m-%d')
        
        # Query para insertar la nueva clase
            insert_class_query = f"""
            INSERT INTO LABORATORIO.MONICA_SOBERON.CLASE (ID_CURSO, FECHA)
            VALUES ({id_curso}, '{fecha_clase_str}');
        """
        
        # Ejecutar la query
            session.sql(insert_class_query).collect()
        # Mensaje de éxito
            st.success(f"Clase creada exitosamente para el curso en la fecha {fecha_clase_str}.")
        else:
            st.error("Por favor, completa todos los campos.")

with tab2:
    st.header("Registrar Asistencia") 
    selected_course_name_with_dates = st.selectbox("Selecciona el Curso:", nombres_df['course_name_with_dates'], key='selectc12')
    id_curso = nombres_df.loc[nombres_df['course_name_with_dates'] == selected_course_name_with_dates, 'ID_CURSO'].values[0]

    # Query to get only class dates
    clases_result = session.sql(f"""
        SELECT clase.id_clase, clase.fecha 
        FROM LABORATORIO.MONICA_SOBERON.CLASE clase
        INNER JOIN LABORATORIO.MONICA_SOBERON.CURSO curso 
        ON clase.id_curso = curso.id_curso
        WHERE curso.id_curso = {id_curso};
    """).to_pandas()

    st.write(clases_result)

    if not clases_result.empty:
        # Muestra el contenido de clases_result para depuración
        st.write(clases_result)

        # Crea un diccionario que mapea las fechas a los ID de clase
        clases_dict = {row['FECHA']: row['ID_CLASE'] for index, row in clases_result.iterrows()}

        # Selecciona la fecha de la clase usando el diccionario
        selected_class_date = st.selectbox("Selecciona una Fecha de Clase:", list(clases_dict.keys()), key='class_select_asistencia3')
        
        if selected_class_date:
            id_clase = clases_dict[selected_class_date]

            # Now you can proceed with further processing using `id_clase`
            # Query for students who attended the class
            students_result = session.sql(f"""
                SELECT id_usuario 
                FROM LABORATORIO.MONICA_SOBERON.ASISTENCIA_CLASE 
                WHERE id_clase = {id_clase};
            """).to_pandas()
            
            if not students_result.empty:
                st.write("Estudiantes que asistieron a la clase:")
                st.dataframe(students_result)
            else:
                st.write(f"No hay estudiantes registrados para la clase {id_clase}.")
            
            email_input = st.text_area(
                "Pega la lista de correos electrónicos aquí (uno por línea):",
                height=300, key = "asistencia_text"
            )       

            if st.button("Procesar Correos", key="correos_asistencias"):
                assistant_email_list = [email.replace(chr(10), '').replace(chr(13), '').strip().lower() for email in email_input.split('\n') if email.strip()]
                assistant_email_list = list(set(assistant_email_list))
                if assistant_email_list:
                    # Convertir lista de correos en string para la consulta
                    email_list_str = ', '.join(f"'{email}'" for email in assistant_email_list)

                    # Consulta para verificar qué correos ya están en la comunidad
                    existing_emails_query = f"""
                        SELECT correo 
                        FROM LABORATORIO.MONICA_SOBERON.comunidad 
                        WHERE correo IN ({email_list_str})
                    """
                    existing_emails = session.sql(existing_emails_query).collect()
                    existing_email_set = set(email['CORREO'] for email in existing_emails)

                    # Mostrar los correos procesados
                    st.write("Correos electrónicos procesados:")
                    st.write(assistant_email_list)

                    # Mostrar los correos que ya están en la comunidad
                    if existing_email_set:
                        st.write("Correos encontrados en la comunidad de analítica:")
                        st.write(existing_email_set)

                        # Marcar como asistentes a los correos encontrados
                        for email in existing_email_set:
                            # Obtener el ID del usuario según el correo
                            user_id_query = f"""
                                SELECT id_usuario 
                                FROM LABORATORIO.MONICA_SOBERON.comunidad 
                                WHERE correo = '{email}'
                            """
                            user_id_result = session.sql(user_id_query).collect()
                            user_id = user_id_result[0]['ID_USUARIO'] if user_id_result else None

                            # Registrar asistencia si se encuentra el ID
                            if user_id:
                                insert_attendance_query = f"""
                                    INSERT INTO LABORATORIO.MONICA_SOBERON.asistencia_clase (id_clase, id_usuario) 
                                    VALUES ({id_clase}, {user_id});
                                """
                                session.sql(insert_attendance_query).collect()

                        st.success("Asistencias registradas con éxito para los correos encontrados.")
                    else:
                        st.warning("Ninguno de los correos está registrado en la comunidad de analítica.")
                else:
                    st.error("No se proporcionaron correos electrónicos válidos.")
    else:
        st.write("No hay clases registradas para este curso.")