import mysql.connector
from mysql.connector import errorcode
from config import CONFIG

def connect_to_database():
    # Conexion a la base de datos MySQL con las credenciales de config.py
    try:
        connection = mysql.connector.connect(**CONFIG)
        if connection.is_connected():
            print("Conectado exitosamente a la base de datos")
            return connection
    except errorcode as e:
        print(f"Error al conectar a MySQL: {e}")
        return None

def execute_query(connection, query):
    # Ejecutar una consulta en la base de datos
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except errorcode as e:
        print(f"Error al ejecutar la consulta: {e}")
        return None
    finally:
        cursor.close()

def reconnect_and_execute_query(query):
    connection = connect_to_database()
    if connection:
        results = execute_query(connection, query)
        connection.close()
        return results
    else:
        return None

def get_promedios_colombia(start=20142, end=20222):
    # Consulta para obtener los promedios nacionales saber 11 por periodo
    query = f"""
    SELECT
      *
    FROM promedios_colombia
    WHERE periodo >= {start} AND periodo <= {end}
    LIMIT 1000
    """
    return reconnect_and_execute_query(query)

def get_promedios_departamento(start=20142, end=20222):
    # Consulta para obtener los promedios departamentales saber 11 por periodo
    query = f"""
    SELECT
      *
    FROM promedios_departamento
    WHERE periodo >= {start} AND periodo <= {end}
    LIMIT 1000
    """
    return reconnect_and_execute_query(query)

def get_promedios_colombia_year(start=20142, end=20222):
    # Consulta para obtener los promedios nacionales saber 11 por año
    query = f"""
    SELECT
        LEFT(periodo, 4) AS year,
        CAST(AVG(promedio_global) AS DECIMAL(25,2)) AS promedio_global,
        CAST(AVG(promedio_lectura_critica) AS DECIMAL(25,2)) AS promedio_lectura_critica,
        CAST(AVG(promedio_matematicas) AS DECIMAL(25,2)) AS promedio_matematicas,
        CAST(AVG(promedio_c_naturales) AS DECIMAL(25,2)) AS promedio_c_naturales,
        CAST(AVG(promedio_sociales_ciudadanas) AS DECIMAL(25,2)) AS promedio_sociales_ciudadanas,
        CAST(AVG(promedio_ingles) AS DECIMAL(25,2)) AS promedio_ingles
    FROM 
        promedios_colombia
    WHERE periodo >= {start} AND periodo <= {end}
    GROUP BY 
        LEFT(periodo, 4)
    ORDER BY 
        year
    LIMIT 1000
    """

    return reconnect_and_execute_query(query)

def get_promedios_departamento_year(start=20142, end=20222):
    # Consulta para obtener los promedios departamentales saber 11 por año
    query = f"""
    SELECT
        LEFT(periodo, 4) AS year,
        departamento,
        CAST(AVG(promedio_global) AS DECIMAL(25,2)) AS promedio_global,
        CAST(AVG(promedio_lectura_critica) AS DECIMAL(25,2)) AS promedio_lectura_critica,
        CAST(AVG(promedio_matematicas) AS DECIMAL(25,2)) AS promedio_matematicas,
        CAST(AVG(promedio_c_naturales) AS DECIMAL(25,2)) AS promedio_c_naturales,
        CAST(AVG(promedio_sociales_ciudadanas) AS DECIMAL(25,2)) AS promedio_sociales_ciudadanas,
        CAST(AVG(promedio_ingles) AS DECIMAL(25,2)) AS promedio_ingles
    FROM 
        promedios_departamento
    WHERE periodo >= {start} AND periodo <= {end}
    GROUP BY 
        departamento, 
        LEFT(periodo, 4)
    ORDER BY 
        year
    LIMIT 1000
    """

    return reconnect_and_execute_query(query)

def get_promedios_colombia_pro(start=20183, end=20226):
    # Consulta para obtener los promedios nacionales saber pro por periodo
    query = f"""
    SELECT
        *
    FROM 
        promedios_colombia_pro
    WHERE periodo >= {start} AND periodo <= {end}
    LIMIT 1000
    """

    return reconnect_and_execute_query(query)

def get_promedios_departamento_pro(start=20142, end=20222):
    # Consulta para obtener los promedios departamentales saber pro por periodo
    query = f"""
    SELECT
        *
    FROM 
        promedios_departamento_pro
    WHERE periodo >= {start} AND periodo <= {end}
    LIMIT 1000
    """

    return reconnect_and_execute_query(query)

def get_promedios_colombia_pro_year(start=20183, end=20226):
    # Consulta para obtener los promedios nacionales saber pro por año
    query = f"""
    SELECT
        LEFT(periodo, 4) AS year,
        CAST(AVG(promedio_c_ciudadana) AS DECIMAL(25,2)) AS promedio_c_ciudadana,
        CAST(AVG(promedio_comuni_escrita) AS DECIMAL(25,2)) AS promedio_comuni_escrita,
        CAST(AVG(promedio_ingles) AS DECIMAL(25,2)) AS promedio_ingles,
        CAST(AVG(promedio_lectura_critica) AS DECIMAL(25,2)) AS promedio_lectura_critica,
        CAST(AVG(promedio_razona_cuantitativo) AS DECIMAL(25,2)) AS promedio_razona_cuantitativo
    FROM 
        promedios_colombia_pro
    WHERE periodo >= {start} AND periodo <= {end}
    GROUP BY 
        LEFT(periodo, 4)
    ORDER BY 
        year
    LIMIT 1000
    """

    return reconnect_and_execute_query(query)

def get_promedios_departamento_pro_year(start=20142, end=20222):
    # Consulta para obtener los promedios departamentales saber pro por año
    query = f"""
    SELECT
        LEFT(periodo, 4) AS year,
        departamento,
        CAST(AVG(promedio_c_ciudadana) AS DECIMAL(25,2)) AS promedio_c_ciudadana,
        CAST(AVG(promedio_comuni_escrita) AS DECIMAL(25,2)) AS promedio_comuni_escrita,
        CAST(AVG(promedio_ingles) AS DECIMAL(25,2)) AS promedio_ingles,
        CAST(AVG(promedio_lectura_critica) AS DECIMAL(25,2)) AS promedio_lectura_critica,
        CAST(AVG(promedio_razona_cuantitativo) AS DECIMAL(25,2)) AS promedio_razona_cuantitativo
    FROM 
        promedios_departamento_pro
    WHERE periodo >= {start} AND periodo <= {end}
    GROUP BY 
        departamento, 
        LEFT(periodo, 4)
    ORDER BY 
        year
    LIMIT 1000
    """

    return reconnect_and_execute_query(query)

def get_consulta_departamento(departamento, start_year, end_year):
    # Consulta inicial por departamento y rango de años
    start_periodo = start_year * 10 + 1
    end_periodo = end_year * 10 + 2

    query = f"""
    SELECT
    *
    FROM saber11
    WHERE cole_depto_ubicacion = {departamento} AND periodo >= {start_periodo} AND periodo <= {end_periodo}
    ORDER BY periodo
    LIMIT 1000000;
    """

    return reconnect_and_execute_query(query)