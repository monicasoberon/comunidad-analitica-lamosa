import os
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from snowflake.snowpark.functions import col

# Connect to Snowflake
cnx = st.connection("snowflake")
session = cnx.session()

# Authentication check
if "auth_data" not in st.session_state:
    st.write("Please authenticate to access this page.")
    st.stop()

# Set a general Seaborn style for all visualizations
sns.set_style("whitegrid")
color_palette = sns.color_palette("muted")

st.title("Estadísticas del Comunidad Analítica")

### User Engagement Metrics ###
st.write("### Métricas de Participación de Usuarios")
st.write(
    """
    Resumen de las métricas de participación de los 
    usuarios en el Comunidad Analítica, incluyendo el número 
    total de usuarios, el número de usuarios que han participado, sesiones a las que han asistido, cursos en los que 
    se han inscrito, y la tasa de finalización de los cursos.
    """
)
engagement_metrics_result = session.sql("""
    SELECT 
        COUNT(DISTINCT C.ID_USUARIO) AS total_usuarios,
        COUNT(DISTINCT CASE WHEN S.ID_SESION IS NOT NULL OR R.ID_CURSO IS NOT NULL THEN C.ID_USUARIO END) AS usuarios_participantes,
        COUNT(DISTINCT S.ID_SESION) AS sesiones_asistidas,
        COUNT(DISTINCT R.ID_CURSO) AS cursos_inscritos,
        SUM(CASE WHEN R.CURSO_APROBADO = 'True' THEN 1 ELSE 0 END) AS cursos_completados
    FROM LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R
    RIGHT JOIN LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
    ON R.ID_USUARIO = C.ID_USUARIO
    LEFT JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS S
    ON C.ID_USUARIO = S.ID_USUARIO;
""")
engagement_metrics_df = engagement_metrics_result.to_pandas()

# Step 1: Display Total Usuarios and Participating Usuarios as a DataFrame
st.write("**Resumen de Usuarios**")

# Create a DataFrame for total users and participating users
usuarios_df = pd.DataFrame({
    'Métrica': ['Total Usuarios', 'Usuarios Participantes', 'Usuarios No Participantes'],
    'Cantidad': [
        engagement_metrics_df['TOTAL_USUARIOS'].values[0],
        engagement_metrics_df['USUARIOS_PARTICIPANTES'].values[0],
        engagement_metrics_df['TOTAL_USUARIOS'].values[0] - engagement_metrics_df['USUARIOS_PARTICIPANTES'].values[0]
    ]
})

# Display the DataFrame in Streamlit
st.write(usuarios_df)

# Step 2: Simple DataFrame for Sessions with Attendance, Courses with Registrations, and Passed Courses
st.write("**Resumen de Sesiones con asistencia registrada, Cursos con estudiantes y Cursos completados**")

# Extract the relevant metrics into a new DataFrame for display
metrics_df = pd.DataFrame({
    'Métrica': ['Sesiones Asistidas', 'Cursos Inscritos', 'Cursos Completados'],
    'Cantidad': [
        engagement_metrics_df['SESIONES_ASISTIDAS'].values[0],
        engagement_metrics_df['CURSOS_INSCRITOS'].values[0],
        engagement_metrics_df['CURSOS_COMPLETADOS'].values[0]
    ]
})

# Display the DataFrame in Streamlit
st.write(metrics_df)



###Top 10 Most Involved Users ###
st.write("### Usuarios Más Involucrados")
st.write(
    """
    Esta gráfica muestra los 10 usuarios que han estado más involucrados 
    en el Comunidad Analítica, con base en las sesiones a las 
    que han asistido y los cursos en los que se han inscrito.
    """
)

# Add a selectbox for area selection, including "Todos" to include everyone
areas = ['Todos', 'Mercadotecnia', 'Comercial', 'Logistica', 'Operaciones', 'Administración', 'Recursos Humanos', 'IT']
selected_area = st.selectbox("Selecciona un Área:", areas)

# Modify the SQL query based on the selected area
if selected_area == 'Todos':
    # If "Todos" is selected, do not filter by area and handle null areas with 'Sin área'
    top_users_query = """
        SELECT 
            C.CORREO,
            COALESCE(C.AREA, 'Sin área') AS AREA,  -- Handle null areas
            COUNT(DISTINCT A.ID_SESION) AS sesiones_asistidas, 
            COUNT(DISTINCT R.ID_CURSO) AS cursos_inscritos, 
            COUNT(DISTINCT A.ID_SESION) + COUNT(DISTINCT R.ID_CURSO) AS participacion_total
        FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
        LEFT JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS A
        ON C.ID_USUARIO = A.ID_USUARIO
        LEFT JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R
        ON C.ID_USUARIO = R.ID_USUARIO
        GROUP BY C.CORREO, C.AREA
        ORDER BY participacion_total DESC
        LIMIT 10;
    """
else:
    # If a specific area is selected, filter by that area, handling nulls
    top_users_query = f"""
        SELECT 
            C.CORREO,
            COALESCE(C.AREA, 'Sin área') AS AREA,  -- Handle null areas
            COUNT(DISTINCT A.ID_SESION) AS sesiones_asistidas, 
            COUNT(DISTINCT R.ID_CURSO) AS cursos_inscritos, 
            COUNT(DISTINCT A.ID_SESION) + COUNT(DISTINCT R.ID_CURSO) AS participacion_total
        FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
        LEFT JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS A
        ON C.ID_USUARIO = A.ID_USUARIO
        LEFT JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R
        ON C.ID_USUARIO = R.ID_USUARIO
        WHERE COALESCE(C.AREA, 'Sin área') = '{selected_area}'
        GROUP BY C.CORREO, C.AREA
        ORDER BY participacion_total DESC
        LIMIT 10;
    """

# Execute the query and fetch the results
top_users_result = session.sql(top_users_query)
top_users_df = top_users_result.to_pandas()

# Create a figure for top users with the area filter applied
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(data=top_users_df, x='PARTICIPACION_TOTAL', y='CORREO', ax=ax, palette=color_palette, dodge=False)
ax.set_title(f'Top 10 Usuarios Más Involucrados en {selected_area if selected_area != "Todos" else "Todas las Áreas"}', fontsize=16, weight='bold')
ax.set_xlabel('Total de Participaciones (Sesiones + Cursos)', fontsize=12)
ax.set_ylabel('Correo del Usuario', fontsize=12)
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)


# Display the figure in Streamlit
st.pyplot(fig)

# Optionally, display the filtered data in a table as well
st.write(f"**Usuarios del Área: {selected_area if selected_area != 'Todos' else 'Todas las Áreas'}**")
st.write(top_users_df)


### Section 2: Most Popular Courses ###
st.write("### Cursos Más Populares")
st.write(
    """
    Aquí puedes ver los cursos más populares basados en la cantidad de 
    inscripciones de los usuarios. Esta métrica ayuda a identificar qué 
    cursos están generando mayor interés en la Comunidad Analítica.
    """
)

# SQL query for popular courses
popular_courses_result = session.sql("""
    SELECT N.NOMBRE_CURSO, COUNT(R.ID_USUARIO) AS inscripciones
    FROM LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS AS N
    INNER JOIN LABORATORIO.MONICA_SOBERON.CURSO AS C
    ON N.ID_CATALOGO = C.ID_CATALOGO
    INNER JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R
    ON C.ID_CURSO = R.ID_CURSO
    GROUP BY N.NOMBRE_CURSO
    ORDER BY inscripciones DESC
    LIMIT 10;
""")
popular_courses_df = popular_courses_result.to_pandas()

# Create a figure for popular courses
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(data=popular_courses_df, x='INSCRIPCIONES', y='NOMBRE_CURSO', ax=ax, hue='NOMBRE_CURSO', dodge=False, palette="Blues_d", legend=False)
ax.set_title('Cursos Más Populares por Tipo', fontsize=16, weight='bold')
ax.set_xlabel('Número de Inscripciones', fontsize=12)
ax.set_ylabel('Nombre del Curso', fontsize=12)
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)
sns.despine()

# Display the figure in Streamlit
st.pyplot(fig)

### Cantidades de cursos por tipo de curso (catalogo cursos) ###
st.write("### Cantidades de Cursos por Tipo de Curso")
curso_por_tipo = session.sql(f"""
    SELECT COUNT(C.ID_CURSO) AS CANTIDAD_CURSOS, K.NOMBRE_CURSO
    FROM LABORATORIO.MONICA_SOBERON.CURSO AS C
    INNER JOIN LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS AS K
    ON C.ID_CATALOGO = K.ID_CATALOGO
    GROUP BY K.NOMBRE_CURSO;""")

curso_por_tipo_df = curso_por_tipo.to_pandas()

fig, ax = plt.subplots(figsize=(12, 8))

# Generate a colorful palette using a colormap
palette = sns.color_palette("tab10", len(curso_por_tipo_df))
ax.barh(curso_por_tipo_df['NOMBRE_CURSO'], curso_por_tipo_df['CANTIDAD_CURSOS'], color=palette)
ax.set_title('Cantidad de Cursos por Tipo', fontsize=16, weight='bold')
ax.set_xlabel('Cantidad de Cursos', fontsize=12)
ax.set_ylabel('Tipo de Curso', fontsize=12)
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)
plt.gca().invert_yaxis()  # Display the highest values at the top
plt.tight_layout()

# Display the graph in Streamlit
st.pyplot(fig)


### Course Completion Rates ###
st.write("### Tasas de Finalización de Cursos")
st.write(
    """
    Esta gráfica compara el número de inscripciones con el número de 
    finalizaciones para cada curso, permitiendo ver qué tan efectivos son 
    los cursos en mantener a los participantes hasta el final.
    """
)

# SQL query for completion rates
completion_rates_result = session.sql("""
    SELECT N.NOMBRE_CURSO, COUNT(R.ID_USUARIO) AS inscritos, 
           SUM(CASE WHEN R.CURSO_APROBADO = 'True' THEN 1 ELSE 0 END) AS completados
    FROM LABORATORIO.MONICA_SOBERON.CURSO AS C
    INNER JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R
    ON C.ID_CURSO = R.ID_CURSO
    INNER JOIN LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS AS N
    ON C.ID_CATALOGO = N.ID_CATALOGO
    GROUP BY N.NOMBRE_CURSO
    ORDER BY inscritos DESC;
""")
completion_rates_df = completion_rates_result.to_pandas()

# Calculate completion rate
completion_rates_df['COMPLETION_RATE'] = (completion_rates_df['COMPLETADOS'] / completion_rates_df['INSCRITOS']) * 100

# Create a figure for course completion rates
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(data=completion_rates_df, x='COMPLETION_RATE', y='NOMBRE_CURSO', ax=ax, hue = 'NOMBRE_CURSO', dodge=False, palette="Greens_d")
ax.set_title('Tasas de Finalización de Cursos', fontsize=16, weight='bold')
ax.set_xlabel('Porcentaje de Finalización (%)', fontsize=12)
ax.set_ylabel('Nombre del Curso', fontsize=12)
ax.set_xlim(0, 100)
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)
sns.despine()

# Display the figure in Streamlit
st.pyplot(fig)

# SQL query to get all session IDs and names
session_list_query = session.sql("""
    SELECT ID_SESION, NOMBRE_SESION
    FROM LABORATORIO.MONICA_SOBERON.SESION;
""")
session_list_df = session_list_query.to_pandas()

# Combine session ID and name for display in the dropdown
session_list_df['session_display'] = session_list_df['NOMBRE_SESION'] + ' (ID: ' + session_list_df['ID_SESION'].astype(str) + ')'
# Create a session selector
selected_session = st.selectbox("Selecciona una Sesión:", session_list_df['session_display'])

# Extract the selected session ID
selected_session_id = session_list_df.loc[session_list_df['session_display'] == selected_session, 'ID_SESION'].values[0]
# SQL query to calculate attendance percentage for the selected session
attendance_percentage_query = session.sql(f"""
    SELECT 
        S.ID_SESION, 
        S.NOMBRE_SESION,
        COUNT(DISTINCT A.ID_USUARIO) AS numero_asistentes, 
        (COUNT(DISTINCT A.ID_USUARIO) / 
        (SELECT COUNT(DISTINCT C.ID_USUARIO) 
         FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C)) * 100 AS porcentaje_asistencia
    FROM LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS A
    RIGHT JOIN LABORATORIO.MONICA_SOBERON.SESION AS S
    ON A.ID_SESION = S.ID_SESION
    WHERE S.ID_SESION = {selected_session_id}
    GROUP BY S.ID_SESION, S.NOMBRE_SESION;
""")
attendance_percentage_df = attendance_percentage_query.to_pandas()
# Display the attendance percentage for the selected session
if not attendance_percentage_df.empty:
    st.write(f"**Porcentaje de Asistencia para la Sesión: {selected_session}**")
    st.write(attendance_percentage_df)

    # Plot the attendance percentage
    fig, ax = plt.subplots(figsize=(6, 4))
    sns.barplot(data=attendance_percentage_df, x='ID_SESION', y='PORCENTAJE_ASISTENCIA', palette="Blues_d", ax=ax)
    ax.set_title('Porcentaje de Asistencia', fontsize=16, weight='bold')
    ax.set_xlabel('ID de Sesión', fontsize=12)
    ax.set_ylabel('Porcentaje de Asistencia (%)', fontsize=12)
    ax.set_ylim(0, 100)
    plt.xticks(fontsize=10)
    plt.yticks(fontsize=10)
    sns.despine()

    # Display the plot
    st.pyplot(fig)
else:
    st.write("No hay datos de asistencia disponibles para esta sesión.")



### User Invitation Recommendations ###
st.write("### Recomendaciones de Invitación a Cursos")
st.write(
    """
    Esta tabla muestra a los usuarios que han asistido a sesiones pero 
    aún no se han inscrito en ningún curso.
    """
)

# SQL query for users to invite to courses
invite_recommendations_result = session.sql("""
    SELECT C.NOMBRE, C.APELLIDO, C.CORREO, COUNT(A.ID_USUARIO) AS sesiones_asistidas
    FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
    LEFT JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS A
    ON C.ID_USUARIO = A.ID_USUARIO
    LEFT JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R
    ON C.ID_USUARIO = R.ID_USUARIO
    WHERE R.ID_USUARIO IS NULL
    GROUP BY C.NOMBRE, C.APELLIDO, C.CORREO
    HAVING COUNT(A.ID_USUARIO) > 0
    ORDER BY sesiones_asistidas DESC;
""")
invite_recommendations_df = invite_recommendations_result.to_pandas()

st.write("**Usuarios a Invitar a Cursos**")
st.write(invite_recommendations_df)

### Dynamic Course Attendance Information ###
st.write("### Información Dinámica de Asistencia por Curso")

# Function to get course names
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
    nombres_df['course_name_with_dates'] = nombres_df.apply(
        lambda row: f"{row['NOMBRE_CURSO']} ({row['FECHA_INICIO']} - {row['FECHA_FIN']})" 
        if pd.notnull(row['FECHA_INICIO']) and pd.notnull(row['FECHA_FIN']) 
        else f"{row['NOMBRE_CURSO']} (Fecha no disponible)", axis=1
    )
    return nombres_df

# Function to get class dates and attendance counts for a specific course
@st.cache_data
def get_class_attendance_for_course(course_id):
    attendance_result = session.sql(f"""
        SELECT clase.fecha, COUNT(DISTINCT asistencia.id_usuario) AS NUMERO_ASISTENTES
        FROM LABORATORIO.MONICA_SOBERON.CLASE clase
        LEFT JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_CLASE asistencia
        ON clase.id_clase = asistencia.id_clase
        WHERE clase.id_curso = {course_id}
        GROUP BY clase.fecha
        ORDER BY clase.fecha;
    """).to_pandas()
    return attendance_result

# Load course names and create a selectbox for course selection
courses_df = get_course_names()
selected_course_name_with_dates = st.selectbox("Selecciona un Curso:", courses_df['course_name_with_dates'])
selected_course_id = courses_df.loc[courses_df['course_name_with_dates'] == selected_course_name_with_dates, 'ID_CURSO'].values[0]

# Fetch attendance data for all classes of the selected course
attendance_data_df = get_class_attendance_for_course(selected_course_id)

if not attendance_data_df.empty:
    st.write(f"### Asistencia por Fecha para el Curso: {selected_course_name_with_dates}")

    # Plot the attendance data
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(attendance_data_df['FECHA'], attendance_data_df['NUMERO_ASISTENTES'], marker='o', linestyle='-')
    ax.set_title('Número de Personas que han Asistido a las Clases', fontsize=16, weight='bold')
    ax.set_xlabel('Fechas de las Clases', fontsize=12)
    ax.set_ylabel('Número de Asistentes', fontsize=12)
    ax.tick_params(axis='x', rotation=45)
    plt.xticks(fontsize=10)
    plt.yticks(fontsize=10)
    plt.tight_layout()
    
    # Display the graph in Streamlit
    st.pyplot(fig)
    
else:
    st.write(f"No hay datos de asistencia disponibles para el curso: {selected_course_name_with_dates}")



