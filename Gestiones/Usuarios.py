import os
import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col

cnx = st.connection("snowflake")
session = cnx.session()

if "auth_data" not in st.session_state:
    st.write("Please authenticate to access this page.")
    st.stop()  # Stop the execution of this page

st.title("Gestión de Usuarios")
st.write(
        """Esta pantalla permite la gestión de los datos personales de los usuarios,
        al igual que su estatus y detalles sobre sus cursos.
        """
    )

@st.cache_data
def get_user_names():
    users_result = session.sql("""
        SELECT correo, nombre, apellido FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD;
    """)
    users_df = users_result.to_pandas()

    # Create a formatted column 'USUARIOS' combining 'correo', 'nombre', and 'apellido'
    users_df['USUARIOS'] = users_df.apply(
        lambda row: f"{row['NOMBRE']} {row['APELLIDO']} : {row['CORREO']}", axis=1)
    return users_df

tab1, tab2, tab5, tab3, tab4 = st.tabs(["Crear Usuario", "Editar Usuario", "Pegar correos Outlook", "Registrar Instructor", "Eliminar Usuario"])

with tab1:
# Write directly to the app
    st.header("Creación de Usuarios")
    st.write("**Ingresa los Datos Personales:**")

    with st.form(key='create_form'):
        nombre_nuevo = st.text_input('Nombre:', value='')
        apellido_nuevo = st.text_input('Apellido:', value='')
        correo_nuevo = st.text_input('Correo:', value='')  # Add an input for the email
        estatus_nuevo = st.checkbox('Estatus (Activo = True, Inactivo = False)', value=False)

        # Dropdown for Negocio with "No Registrar" option
        negocio_nuevo = st.selectbox('Negocio:', ['Ventas', 'Mercadotecnia', 'Operaciones', 'Finanzas', 'No Registrar'])

        # Dropdown for Área with "No Registrar" option
        area_nueva = st.selectbox('Área:', ['IT', 'Recursos Humanos', 'Logística', 'Administración', 'Comercial', 'No Registrar'])

        # Dropdown for País with "No Registrar" option
        pais_nuevo = st.selectbox('País:', ['México', 'Estados Unidos', 'Colombia', 'Argentina', 'No Registrar'])

        submit_button = st.form_submit_button(label='Crear Usuario')

        if submit_button:
            # Convert the checkbox value (True/False) to 1/0 for the database
            estatus_value = 1 if estatus_nuevo else 0

            # Handle "No Registrar" option for Negocio, Área, and País by setting them to None in the database
            negocio_value = None if negocio_nuevo == 'No Registrar' else negocio_nuevo
            area_value = None if area_nueva == 'No Registrar' else area_nueva
            pais_value = None if pais_nuevo == 'No Registrar' else pais_nuevo

            # Use parameterized queries instead of string interpolation
            insert_query = """
            INSERT INTO LABORATORIO.MONICA_SOBERON.COMUNIDAD (NOMBRE, APELLIDO, CORREO, STATUS, NEGOCIO, AREA, PAIS)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """

            try:
                session.sql(insert_query, (nombre_nuevo, apellido_nuevo, correo_nuevo, estatus_value, negocio_value, area_value, pais_value)).collect()
                st.success("Usuario creado exitosamente.")
            except Exception as e:
                st.error(f"Error al crear el usuario: {e}")


with tab5:
    st.header("Añadir Usuarios Faltantes")
    st.write("""Esta sección sirve para pegar los correos copiados al seleccionar reply all en Outlook. 
                Aquí se formatean los correos y se añaden a la comunidad los que aún no se encuentran en ella.""")
    
    # Input for emails
    correos_input = st.text_area("Pega aquí los correos:")

    # Add a button for submitting the emails
    runbtn = st.button("Añadir Usuarios", key="usuario")

    # Initialize session state for handling the button state
    if "runbtn_state" not in st.session_state:
        st.session_state.runbtn_state = False

    # If the button is pressed or state is true
    if runbtn or st.session_state.runbtn_state:
        st.session_state.runbtn_state = True

        # Ensure the text area has input
        if correos_input:
            # Process the input emails
            correos = correos_input.split(";")  # Split by semicolon
            correos_formateados = [correo.split("<")[-1].strip().rstrip(">").replace(chr(10), '').replace(chr(13), '').strip().lower() for correo in correos]
            try:
                comunidad_result = session.sql("SELECT CORREO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD;")
                comunidad_df = comunidad_result.to_pandas()
                comunidad_correos = set(comunidad_df['CORREO'].tolist())
            except Exception as e:
                st.error(f"Error al consultar la base de datos: {e}")
                st.stop()

            # Filter new emails that are not in the community
            nuevos_correos = set(correos_formateados) - comunidad_correos
            st.write(f"Nuevos correos que no están en la comunidad: {nuevos_correos}")  # Debug point

            if nuevos_correos:
                st.write("Complete los datos de los siguientes usuarios:")

                # Create a dataframe with columns for user data input
                user_data = pd.DataFrame(
                    [{"Correo": correo, "Nombre": "", "Apellido": "", "Negocio": "", "Área": "", "País": "", "Estatus": True}
                    for correo in nuevos_correos]
                )

                # Use st.data_editor with editable columns, except for 'Correo'
                edited_user_data = st.data_editor(
                    user_data,
                    use_container_width=True  # Allow better UI scaling
                )

                # Submit button for saving the changes
                if st.button("Registrar Usuarios", key="submit_users"):
                    st.write("Formulario enviado. Procesando usuarios...")  # Debug point

                    # Process the user_data list and insert into the database
                    for index, row in edited_user_data.iterrows():
                        st.write(f"Registrando usuario: {row['Correo']}")  # Debug point

                        # Insert query with direct handling of NULL values
                        insert_query = f"""
                        INSERT INTO LABORATORIO.MONICA_SOBERON.COMUNIDAD 
                        (NOMBRE, APELLIDO, CORREO, STATUS, NEGOCIO, AREA, PAIS)
                        VALUES (
                            {f"'{row['Nombre']}'" if row['Nombre'] else 'NULL'}, 
                            {f"'{row['Apellido']}'" if row['Apellido'] else 'NULL'}, 
                            '{row['Correo']}', 
                            {row['Estatus']}, 
                            {f"'{row['Negocio']}'" if row['Negocio'] else 'NULL'}, 
                            {f"'{row['Área']}'" if row['Área'] else 'NULL'}, 
                            {f"'{row['País']}'" if row['País'] else 'NULL'}
                        );
                        """

                        try:
                            session.sql(insert_query).collect()  
                            st.success(f"Usuario {row['Nombre'] or row['Correo']} registrado con éxito.")
                        except Exception as e:
                            st.error(f"Error al registrar {row['Correo']}: {e}")
            else:
                st.success("Todos los correos ya están registrados en la comunidad.")
    
with tab2:

    st.header("Editar Usuarios") 
    # Display selectbox for individual member selection
    users_df = get_user_names()
    miembro = st.selectbox('Selecciona un miembro:', users_df['USUARIOS'])
    if miembro:
        # Query to get individual member details
        miembro = miembro.split(' : ')[1]
        miembro_sql = session.sql(f"""
            SELECT NOMBRE, APELLIDO, CORREO, STATUS, NEGOCIO, AREA, PAIS, BAJA_EMPRESA
            FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD 
            WHERE CORREO = '{miembro}';
        """)
        miembro_df = miembro_sql.to_pandas()

        # Display member details
        st.write("**Detalles del Miembro:**")
        for index, row in miembro_df.iterrows():
            st.write(f" Nombre: {row['NOMBRE']}")
            st.write(f" Apellido: {row['APELLIDO']}")
            st.write(f" Correo: {row['CORREO']}")
            st.write(f" Estatus: {row['STATUS']}")
            st.write(f" Negocio: {row['NEGOCIO']}")
            st.write(f" Área: {row['AREA']}")
            st.write(f" País: {row['PAIS']}")
            st.write(f" Baja Empresa: {row['BAJA_EMPRESA']}")

        st.write("**Actualización de Datos Personales:**")
        # Form to update user details
        with st.form(key='update_form'):
            # Fetch the user data
            miembro_sql = session.sql(f"""
                SELECT NOMBRE, APELLIDO, CORREO, STATUS, NEGOCIO, AREA, PAIS, BAJA_EMPRESA
                FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD 
                WHERE CORREO = '{miembro}';
            """)
            miembro_df = miembro_sql.to_pandas()

            if not miembro_df.empty:
                row = miembro_df.iloc[0]  # Get the first row

                # Display the form with pre-filled values
                nombre_nuevo = st.text_input('Nombre Nuevo:', value=row['NOMBRE'])
                apellido_nuevo = st.text_input('Apellido Nuevo:', value=row['APELLIDO'])
                correo_nuevo = st.text_input('Correo Nuevo:', value=row['CORREO'])
                estatus_nuevo = st.checkbox('Estatus (Activo = True, Inactivo = False)', value=bool(row['STATUS']))

                # Dropdowns with current value pre-selected, without "No Registrar"
                negocio_nuevo = st.selectbox('Negocio Nuevo:', 
                                            ['Ventas', 'Mercadotecnia', 'Operaciones', 'Finanzas'], 
                                            index=0 if row['NEGOCIO'] is None else 
                                            ['Ventas', 'Mercadotecnia', 'Operaciones', 'Finanzas'].index(row['NEGOCIO']) if row['NEGOCIO'] else 0)

                area_nueva = st.selectbox('Área Nueva:', 
                                        ['IT', 'Recursos Humanos', 'Logística', 'Administración', 'Comercial'], 
                                        index=0 if row['AREA'] is None else 
                                        ['IT', 'Recursos Humanos', 'Logística', 'Administración', 'Comercial'].index(row['AREA']) if row['AREA'] else 0)

                pais_nuevo = st.selectbox('País Nuevo:', 
                                        ['México', 'Estados Unidos', 'Colombia', 'Argentina'], 
                                        index=0 if row['PAIS'] is None else 
                                        ['México', 'Estados Unidos', 'Colombia', 'Argentina'].index(row['PAIS']) if row['PAIS'] else 0)

                baja_nuevo = st.checkbox('Baja Empresa (Baja de la Empresa = True, En la Empresa = False)', value=bool(row['BAJA_EMPRESA']))

                submit_button = st.form_submit_button(label='Actualizar Detalles')

                if submit_button:
                    # Keep values unchanged if they are not updated. NULL fields stay NULL unless changed by the user.

                    negocio_value = row['NEGOCIO'] if row['NEGOCIO'] else None  # Keep original or NULL
                    area_value = row['AREA'] if row['AREA'] else None  # Keep original or NULL
                    pais_value = row['PAIS'] if row['PAIS'] else None  # Keep original or NULL

                    # Only update fields if the new value is different from the original
                    update_query = """
                    UPDATE LABORATORIO.MONICA_SOBERON.COMUNIDAD
                    SET NOMBRE = ?, APELLIDO = ?, STATUS = ?, CORREO = ?, NEGOCIO = ?, AREA = ?, PAIS = ?, BAJA_EMPRESA = ?
                    WHERE CORREO = ?
                    """
                    try:
                        session.sql(update_query, (nombre_nuevo, apellido_nuevo, estatus_nuevo, correo_nuevo, 
                                                negocio_value, area_value, pais_value, baja_nuevo, miembro)).collect()
                        st.success("Detalles actualizados exitosamente.")
                    except Exception as e:
                        st.error(f"Error al actualizar el usuario: {e}")
            else:
                st.error("No se encontraron detalles para el miembro seleccionado.")


with tab3:
    st.header("Crear Nuevo Instructor")
    
    with st.form(key='new_instructor_form'):
        first_name = st.text_input("Nombre del Instructor")
        last_name = st.text_input("Apellido del Instructor")
        instructor_email = st.text_input("Correo Electrónico del Instructor")
        
        submit_instructor_button = st.form_submit_button(label='Crear Instructor')
        
        if submit_instructor_button:
            if first_name and last_name and instructor_email:
                # Insert new instructor into the database
                query = f"""
                INSERT INTO LABORATORIO.MONICA_SOBERON.INSTRUCTOR (NOMBRE_INSTRUCTOR, APELLIDO_INSTRUCTOR, CORREO_INSTRUCTOR)
                VALUES ('{first_name}', '{last_name}', '{instructor_email}');
                """
                session.sql(query).collect()
                st.success(f"Instructor '{first_name} {last_name}' creado con éxito.")
            else:
                st.error("Por favor, ingrese el nombre, apellido y correo del instructor.")

with tab4:
    st.header("Eliminar Usuario")
    st.write(
        """Eliminar un usuario es algo definitivo. Se borrarán todos sus datos. Solo se pueden borrar usuarios que no se han invitado, asistido, o registrado a un curso o sesión."""
    )

    users_df = get_user_names()
    miembro = st.selectbox('Selecciona un miembro:', users_df['USUARIOS'], key='del')
    if miembro:
        # Extract the email from the selected member
        miembro_del = miembro.split(' : ')[1]

        # Query to get individual member details using parameterized query
        miembro_sql_del = session.sql("""
            SELECT NOMBRE, APELLIDO, CORREO, STATUS
            FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD
            WHERE CORREO = ?;       
        """, [miembro_del])
        miembro_df_del = miembro_sql_del.to_pandas()

        if not miembro_df_del.empty:
            # Display member details
            st.write("**Estás seguro que deseas eliminar al usuario:**")
            row = miembro_df_del.iloc[0]
            st.write(f"Nombre: {row['NOMBRE']}")
            st.write(f"Apellido: {row['APELLIDO']}")
            st.write(f"Correo: {row['CORREO']}")
            st.write(f"Estatus: {row['STATUS']}")

            # Query to get the user ID using parameterized query
            miembro_id_result = session.sql("""
                SELECT id_usuario
                FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD
                WHERE CORREO = ?;
            """, [miembro_del])
            miembro_id_df = miembro_id_result.to_pandas()

            if not miembro_id_df.empty:
                miembro_id = miembro_id_df.iloc[0, 0]

                # Confirmation checkbox
                seguro = st.checkbox("Estoy seguro de que quiero eliminar este usuario.")

                if seguro:
                    # Check if the user has participated in any courses or sessions
                    check_data = session.sql("""
                        SELECT 
                            (SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.INVITACION_CURSO WHERE ID_USUARIO = ?) AS INVITADOS_COUNT,
                            (SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO WHERE ID_USUARIO = ?) AS REGISTRADOS_COUNT,
                            (SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.INVITACION_SESION WHERE ID_USUARIO = ?) AS INVITADO_COUNT,
                            (SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION WHERE ID_USUARIO = ?) AS ASISTENTES_COUNT
                    """, [miembro_id, miembro_id, miembro_id, miembro_id]).to_pandas()

                    counts = check_data.iloc[0]

                    if any(counts > 0):
                        st.error("Este usuario no se puede eliminar porque ha participado en cursos o sesiones.")
                        st.write(f"Invitaciones a Cursos: {counts['INVITADOS_COUNT']}")
                        st.write(f"Inscripciones a Cursos: {counts['REGISTRADOS_COUNT']}")
                        st.write(f"Invitaciones a Sesiones: {counts['INVITADO_COUNT']}")
                        st.write(f"Asistencia a Sesiones: {counts['ASISTENTES_COUNT']}")
                    else:
                        # Deletion button, only enabled if the user has not participated
                        borrar = st.button('Eliminar Usuario', key="processU")
                        if borrar:
                            session.sql("""
                                DELETE FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE ID_USUARIO = ?;
                            """, [miembro_id]).collect()
                            st.success("El usuario ha sido eliminado exitosamente.")
            else:
                st.error("No se pudo obtener el ID del usuario seleccionado.")
        else:
            st.error("No se encontraron detalles para el usuario seleccionado.")
    else:
        st.info("Por favor, selecciona un miembro para eliminar.")
