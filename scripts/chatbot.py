from transformers import MarianMTModel, MarianTokenizer, T5Tokenizer, T5ForConditionalGeneration
import unidecode
import re
import pandas as pd
import mysql.connector
from mysql.connector import errorcode


# Cargar el tokenizador y el modelo de traducción de español a inglés
tokenizer_es_en = MarianTokenizer.from_pretrained("Helsinki-NLP/opus-mt-es-en")
model_es_en = MarianMTModel.from_pretrained("Helsinki-NLP/opus-mt-es-en")

# Cargar el tokenizador y el modelo T5 finetuned para generación de SQL
#tokenizer_sql = T5Tokenizer.from_pretrained("mrm8488/t5-base-finetuned-wikiSQL")
#model_sql = T5ForConditionalGeneration.from_pretrained("mrm8488/t5-base-finetuned-wikiSQL")

tokenizer_sql = T5Tokenizer.from_pretrained('./path_to_trained_model')  # Cambiar la ruta al modelo entrenado
model_sql = T5ForConditionalGeneration.from_pretrained('./path_to_trained_model') 
# Definir reglas para las columnas y funciones
column_rules = {
    "promedio general": "AVG(promedio_global)",
    "promedio lectura crítica": "AVG(promedio_lectura_critica)",
    "promedio matemáticas": "AVG(promedio_matematicas)",
    "promedio ciencias naturales": "AVG(promedio_c_naturales)",
    "promedio sociales y ciudadanas": "AVG(promedio_sociales_ciudadanas)",
    "promedio inglés": "AVG(promedio_ingles)",
    "departamento": "departamento"
}

column_rules_inverted = {
    "avg(promedio_global)": "avg(punt_global)",
    "avg(promedio_lectura_critica)": "avg(punt_lectura_critica)",
    "avg(promedio_matematicas)": "avg(punt_matematicas)",
    "avg(promedio_c_naturales)": "avg(punt_c_naturales)",
    "avg(promedio_sociales_ciudadanas)": "avg(punt_sociales_ciudadanas)",
    "avg(promedio_ingles)": "avg(punt_ingles)",
    "departamento": "departamento"
}



def add_quotes_to_departments(sql_query):
    departments = [
        'Amazonas', 'Antioquia', 'Arauca', 'Atlántico', 'Bolívar', 'Boyacá',
        'Caldas', 'Caquetá', 'Casanare', 'Cauca', 'Cesar', 'Chocó', 'Córdoba',
        'Cundinamarca', 'Guainía', 'Guaviare', 'Huila', 'La Guajira', 'Magdalena',
        'Meta', 'Nariño', 'Norte de Santander', 'Putumayo', 'Quindío', 'Risaralda',
        'San Andrés y Providencia', 'Santander', 'Sucre', 'Tolima', 'Valle del Cauca',
        'Vaupés', 'Vichada'
    ]
    for department in departments:
        # Convertir ambos a minúsculas y eliminar tildes para comparación
        normalized_department = unidecode.unidecode(department).lower()
        sql_query = re.sub(rf'\b{normalized_department}\b', f"'{department}'", unidecode.unidecode(sql_query).lower(), flags=re.IGNORECASE)
    return sql_query

def add_quotes_to_municipalities(sql_query):
    df = pd.read_csv('municipios.csv')
    municipalities = df['Nombre Municipio'].tolist()
    for municipality in municipalities:
        municipality = str(municipality)
        # Convertir ambos a minúsculas y eliminar tildes para comparación
        normalized_municipality = unidecode.unidecode(municipality).lower()
        sql_query = re.sub(rf'\b{normalized_municipality}\b', f"'{municipality}'", unidecode.unidecode(sql_query).lower(), flags=re.IGNORECASE)
    return sql_query

def apply_column_rules(text):
    for esp, sql in column_rules.items():
        text = text.replace(esp, sql)
    return text

def apply_inverse_column_rules(text):
    for esp, sql in column_rules_inverted.items():
        text = text.replace(esp, sql)
    return text

def translate_to_english(text):
    # Aplicar reglas a las columnas y funciones antes de la traducción
    marked_text = apply_column_rules(text)

    inputs = tokenizer_es_en(marked_text, return_tensors="pt", truncation=True)
    translated_tokens = model_es_en.generate(**inputs)
    translated_text = tokenizer_es_en.decode(translated_tokens[0], skip_special_tokens=True)

    return translated_text

def translate_to_sql(text_en):
    prompt = f"translate English to SQL: {text_en}"
    inputs = tokenizer_sql.encode(prompt, return_tensors="pt", truncation=True)
    outputs = model_sql.generate(inputs, max_length=100)
    sql_query = tokenizer_sql.decode(outputs[0], skip_special_tokens=True)
    return sql_query.strip()

def format_sql_query(sql_query):
    # Asegurarnos de que la consulta SQL sea correcta y no se traduzcan las columnas y funciones
    table = None

    for esp, sql in column_rules.items():
        sql_query = sql_query.replace(esp, sql)
    if "Department"  in sql_query:
        sql_query = sql_query.replace("table", "promedios_departamento")
    if "Municipality" in sql_query:
        table= "saber11"
        sql_query = sql_query.replace("table", "saber11")    
    #sql_query = sql_query.replace("table", "promedios_departamento")
    sql_query = sql_query.replace("School", "departamento")
    sql_query = sql_query.replace("Department", "departamento")
    sql_query = sql_query.replace("Municipality", "cole_mcpio_ubicacion")
    sql_query = sql_query.replace("global average", "promedio_global")
    sql_query = sql_query.replace("AVG English","AVG (promedio_ingles)")
    sql_query = sql_query.replace("AVG Math","AVG (promedio_matematicas)")
    sql_query = sql_query.replace("AVG Natural Sciences", "AVG(promedio_c_naturales)")
    sql_query = sql_query.replace("AVG Natural Science", "AVG(promedio_c_naturales)")
    sql_query = sql_query.replace("AVG Social and Citizenship", "AVG(promedio_sociales_ciudadanas)")
    sql_query = sql_query.replace("AVG Critical Reading", "AVG(promedio_lectura_critica)")
    sql_query = sql_query.replace("Year","periodo")
    sql_query = add_quotes_to_departments(sql_query)
    sql_query = add_quotes_to_municipalities(sql_query)
    if table=="saber11":
        sql_query=apply_inverse_column_rules(sql_query)

    return sql_query
    

# Probar la traducción y la generación de SQL
user_input = "¿Cuál es el puntaje promedio de ciencias naturales en el departamento de santander?"


# Traducir del español al inglés
translated_text = translate_to_english(user_input)
print("Texto traducido a inglés:", translated_text)

# Traducir del inglés a SQL
sql_query = translate_to_sql(translated_text)
print("Consulta generada antes de formatear:", sql_query)

# Formatear la consulta SQL
formatted_sql_query = format_sql_query(sql_query)
if "periodo" not in formatted_sql_query:
    formatted_sql_query = formatted_sql_query+' and periodo >= 20142 AND periodo <= 20222'

print("Consulta generada después de formatear:", formatted_sql_query)


# Detalles de configuración de la base de datos
CONFIG = {
    'user': 'admin',
    'password': 'Unacontra123.',
    'host': 'icfes.cxa2yu08iduf.us-east-2.rds.amazonaws.com',
    'raise_on_warnings': True,
    'database': 'icfes_11'
}

def execute_query(query, config):
    try:
        # Conectar a la base de datos
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        # Ejecutar la consulta
        cursor.execute(query)

        # Obtener los resultados
        results = cursor.fetchall()

        # Cerrar la conexión
        cursor.close()
        connection.close()

        return results
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Algo está mal con tu nombre de usuario o contraseña")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("La base de datos no existe")
        else:
            print(err)
    else:
        connection.close()

# Probar la traducción y la generación de SQL
user_input = "¿Cuál es el puntaje promedio de ciencias naturales en el departamento de santander?"

# Traducir del español al inglés
translated_text = translate_to_english(user_input)
print("Texto traducido a inglés:", translated_text)

# Traducir del inglés a SQL
sql_query = translate_to_sql(translated_text)
print("Consulta generada antes de formatear:", sql_query)


# Formatear la consulta SQL
formatted_sql_query = format_sql_query(sql_query)
#if "periodo" not in formatted_sql_query:
    #formatted_sql_query = formatted_sql_query + ' and periodo >= 20142 AND periodo <= 20222'

print("Consulta generada después de formatear:", formatted_sql_query)

# Ejecutar la consulta en la base de datos
results = execute_query(formatted_sql_query, CONFIG)

# Mostrar los resultados
for result in results:
    print(result)