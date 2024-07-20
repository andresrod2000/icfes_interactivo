import mysql.connector
from mysql.connector import errorcode
from config import CONFIG

def connect_to_database():
    try:
        connection = mysql.connector.connect(**CONFIG)
        if connection.is_connected():
            print("Conectado exitosamente a la base de datos")
            return connection
    except errorcode as e:
        print(f"Error al conectar a MySQL: {e}")
        return None

def execute_query(connection, query):
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

def get_promedios_colombia(connection, start=20142, end=20222):
    query = f"""
    SELECT
      *
    FROM promedios_colombia
    WHERE periodo >= {start} AND periodo <= {end}
    LIMIT 1000
    """
    return execute_query(connection, query)

def get_promedios_departamento(connection, start=20142, end=20222):
    query = f"""
    SELECT
      *
    FROM promedios_colombia
    WHERE periodo >= {start} AND periodo <= {end}
    LIMIT 1000
    """
    return execute_query(connection, query)

def get_promedios_colombia_year(connection, start=20142, end=20222):
    query = """
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
    WHERE periodo >=20142
    GROUP BY 
        LEFT(periodo, 4)
    ORDER BY 
        year
    LIMIT 1000
    """

    return execute_query(connection, query)

def get_promedios_departamento_year(connection, start=20142, end=20222):
    query = """
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
    WHERE periodo >=20142
    GROUP BY 
        departamento, 
        LEFT(periodo, 4)
    ORDER BY 
        year
    LIMIT 1000
    """

    return execute_query(connection, query)