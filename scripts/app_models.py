from flask import Flask, request, jsonify
from flask_cors import CORS
from transformers import MarianMTModel, MarianTokenizer, T5Tokenizer, T5ForConditionalGeneration
import unidecode
import re
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import xgboost
import joblib
from datetime import datetime

app = Flask(__name__)
CORS(app)
# Cargar los modelos y tokenizadores
tokenizer_es_en = MarianTokenizer.from_pretrained("Helsinki-NLP/opus-mt-es-en")
model_es_en = MarianMTModel.from_pretrained("Helsinki-NLP/opus-mt-es-en")
tokenizer_sql = T5Tokenizer.from_pretrained("mrm8488/t5-base-finetuned-wikiSQL")
model_sql = T5ForConditionalGeneration.from_pretrained("mrm8488/t5-base-finetuned-wikiSQL")


##
#tokenizer_sql = T5Tokenizer.from_pretrained('./path_to_trained_model')  # Cambiar la ruta al modelo entrenado
#model_sql = T5ForConditionalGeneration.from_pretrained('./path_to_trained_model') 

##
# Reglas para columnas
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

import pandas as pd
import unidecode

def descomponer_fecha(fecha):
    if fecha:
        fecha_dt = datetime.strptime(fecha, "%Y-%m-%d")
        return fecha_dt.day, fecha_dt.month, fecha_dt.year
    else:
        return None, None, None
    

def parse_query(query):
    # Extraer la parte después de 'where'
    conditions = query.split('where')[-1].strip()
    conditions = conditions.replace('language = english and', '').strip()
    conditions = conditions.replace('average english = best and','').strip()
    conditions = conditions.replace('average = best math and', '').strip()
    conditions = conditions.replace('best average math','').strip()
    conditions = conditions.replace('best math','').strip()
    
    return conditions

def construct_new_query(original_query):
    columna_locacion = get_column_based_on_query(original_query)
    if 'best english avg.' in original_query or 'avg best' in original_query:
        conditions = parse_query(original_query)
        # Reemplazar 'departamento' por el valor de `columna_locacion`
        conditions = conditions.replace('departamento', columna_locacion)
        conditions = conditions.replace('city', columna_locacion)
        new_query = f'''
        SELECT `cole_nombre_establecimiento`
        FROM saber11
        WHERE {conditions}
        ORDER BY `punt_ingles` DESC
        LIMIT 1;
        '''

    elif 'avg(punt_ingles)' in original_query:
        conditions = parse_query(original_query)
        # Reemplazar 'cole_mcpio_ubicacion' por el valor de `columna_locacion`
        conditions = conditions.replace('cole_mcpio_ubicacion', columna_locacion)
        new_query = f'''
        SELECT `cole_nombre_establecimiento`
        FROM saber11
        WHERE {conditions}
        ORDER by 'punt_ingles' DESC
        LIMIT 1;
        '''
    

    elif 'best math average' in original_query or 'average = best math' in original_query or 'average math' in original_query or 'best math' in original_query:
        conditions = parse_query(original_query)
        # Reemplazar 'departamento' por el valor de `columna_locacion`
        conditions = conditions.replace('departamento', columna_locacion)
        conditions = conditions.replace('city', columna_locacion)
        new_query = f'''
        SELECT `cole_nombre_establecimiento`
        FROM saber11
        WHERE {conditions}
        ORDER BY `punt_matematicas` DESC
        LIMIT 1;
        '''

    elif 'best avg' in original_query:
        conditions = parse_query(original_query)
        # Reemplazar 'departamento' por el valor de `columna_locacion`
        conditions = conditions.replace('departamento', columna_locacion)
        conditions = conditions.replace('city', columna_locacion)
        new_query = f'''
        SELECT `cole_nombre_establecimiento`
        FROM saber11
        WHERE {conditions}
        ORDER BY `punt_c_naturales` DESC
        LIMIT 1;
        '''   


    else:
        new_query = original_query
        if "best natural science" in original_query:
            new_query=new_query.replace('best natural science', '')
            if "LIMIT" not in original_query:
                new_query = new_query + ' LIMIT 1;'
            #new_query+='ORDER BY '
        if "average english = best and" in original_query or "english = best average" in original_query or "english = best and average" in original_query:
            new_query=new_query.replace('average english = best and', '')
            new_query=new_query.replace('english = best average',f' {columna_locacion} =')
            new_query=new_query.replace('english = best and average',f' {columna_locacion} ')
            if "LIMIT" not in original_query:
                new_query = new_query + ' LIMIT 1;'

        if "best-age english" in original_query:
            new_query=new_query.replace('best-age english', '')
            if "LIMIT" not in original_query:
                new_query = new_query + ' LIMIT 1;'

        
        #new_query = "Consulta no reconocida o no soportada."
    
    return new_query


def get_column_based_on_query(query):
    departments = [
        'Amazonas', 'Antioquia', 'Arauca', 'Atlántico', 'Bolívar', 'Boyacá',
        'Caldas', 'Caquetá', 'Casanare', 'Cauca', 'Cesar', 'Chocó', 'Córdoba',
        'Cundinamarca', 'Guainía', 'Guaviare', 'Huila', 'La Guajira', 'Magdalena',
        'Meta', 'Nariño', 'Norte de Santander', 'Putumayo', 'Quindío', 'Risaralda',
        'San Andrés y Providencia', 'Santander', 'Sucre', 'Tolima', 'Valle del Cauca',
        'Vaupés', 'Vichada','Bogotá'
    ]

    query = unidecode.unidecode(query).lower()

    for department in departments:
        normalized_department = unidecode.unidecode(department).lower()
        if normalized_department in query:
            print(department)
            return 'cole_depto_ubicacion'
    
    return 'cole_mcpio_ubicacion'





def fix_query(query):

    columna_locacion = get_column_based_on_query(query)
    if "best avg" in query:
        # Extract the avg column
        avg_column_condition = re.search(r'best avg\s*\(\s*(.*?)\s*\)', query, re.IGNORECASE)
        if avg_column_condition:    
            avg_column = avg_column_condition.group(1).strip()
        else:
            raise ValueError("Query format is incorrect. Could not find the avg column.")
        
        # Extract the department condition, accounting for both formats
        department_condition_match = re.search(r'best avg\s*\(\s*.*?\s*\)\s*=\s*\'(.*?)\'', query, re.IGNORECASE) or re.search(r'departamento\s*=\s*\'(.*?)\'', query, re.IGNORECASE)
        if department_condition_match:
            department_value = department_condition_match.group(1).strip()
        else:
            raise ValueError("Query format is incorrect. Could not find the department value.")
        
        # Extract the other conditions
        other_conditions_match = re.search(r'where (.*)', query, re.IGNORECASE)
        if other_conditions_match:
            other_conditions = other_conditions_match.group(1).strip()
            # Remove the 'best avg' part and 'departamento' part from other conditions
            other_conditions = re.sub(r'best avg\s*\(\s*.*?\s*\)\s*=\s*\'(.*?)\'\s*AND\s*', '', other_conditions, flags=re.IGNORECASE)
            other_conditions = re.sub(r'departamento\s*=\s*\'(.*?)\'\s*AND\s*', '', other_conditions, flags=re.IGNORECASE)
        else:   
            raise ValueError("Query format is incorrect. Could not find the conditions.")
        
        # Create the new query
        new_query = f'''
        SELECT `cole_nombre_establecimiento` 
        FROM saber11 
        WHERE {other_conditions} 
          AND `{columna_locacion}` = '{department_value}' 
        ORDER BY punt_global DESC 
        LIMIT 1;
        '''
        return new_query.strip()
    return query

def add_quotes_to_departments(sql_query):
    departments = [
        'Amazonas', 'Antioquia', 'Arauca', 'Atlántico', 'Bolívar', 'Boyacá',
        'Caldas', 'Caquetá', 'Casanare', 'Cauca', 'Cesar', 'Chocó', 'Córdoba',
        'Cundinamarca', 'Guainía', 'Guaviare', 'Huila', 'La Guajira', 'Magdalena',
        'Meta', 'Nariño', 'Norte de Santander', 'Putumayo', 'Quindío', 'Risaralda',
        'San Andrés y Providencia', 'Santander', 'Sucre', 'Tolima', 'Valle del Cauca',
        'Vaupés', 'Vichada','Bogotá'
    ]
    for department in departments:
        normalized_department = unidecode.unidecode(department).lower()
        sql_query = re.sub(
            rf'\b{normalized_department}\b', f"'{department}'", unidecode.unidecode(sql_query).lower(), flags=re.IGNORECASE
        )
    return sql_query

def add_quotes_to_municipalities(sql_query):
    df = pd.read_csv('municipios.csv')
    df['Nombre Municipio'] = df['Nombre Municipio'].astype(str)  # Asegura que todos los valores sean cadenas
    municipalities = df['Nombre Municipio'].tolist()
    
    for municipality in municipalities:
        normalized_municipality = unidecode.unidecode(municipality).lower()
        # Comprueba si el municipio ya está entre comillas en la consulta
        pattern_with_quotes = rf"'{normalized_municipality}'"
        if re.search(pattern_with_quotes, sql_query, re.IGNORECASE):
            continue
        
        # Añade comillas al municipio si no las tiene
        sql_query = re.sub(
            rf'\b{normalized_municipality}\b', f"'{municipality}'", unidecode.unidecode(sql_query).lower(), flags=re.IGNORECASE
        )
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

def format_sql_query(sql_query,texto):
    # Asegurarnos de que la consulta SQL sea correcta y no se traduzcan las columnas y funciones
    table = None

    for esp, sql in column_rules.items():
        sql_query = sql_query.replace(esp, sql)
    if "Department"  in sql_query:
        columna_locacion = get_column_based_on_query(sql_query)
        sql_query = sql_query.replace("Location",columna_locacion)
        sql_query = sql_query.replace("Location","departamento")
        if "colegio" not in texto:
            print("tests: ",sql_query)
            sql_query = sql_query.replace("table", "promedios_departamento")
        else:
            columna_locacion=get_column_based_on_query(sql_query)
            sql_query = sql_query.replace("Location",columna_locacion)
            table ="saber11"
            sql_query = sql_query.replace("table", "saber11")
    else:
        if "colegio" in texto:
            columna_locacion = get_column_based_on_query(sql_query)
            sql_query = sql_query.replace("City",columna_locacion)
            sql_query = sql_query.replace("Location",columna_locacion)
            sql_query = sql_query.replace("Location","`cole_mcpio_ubicacion`")
            table ="saber11"
            sql_query = sql_query.replace("table", "saber11")

    if any(word in sql_query for word in ["Municipality", "School"]):
        table = "saber11"
        sql_query = sql_query.replace("table", "saber11")
    
    #sql_query = sql_query.replace("table", "promedios_departamento")
    sql_query = sql_query.replace("School", "`cole_nombre_establecimiento`")
    sql_query = sql_query.replace("Department", "departamento")
    sql_query = sql_query.replace("city","estu_depto_presentacion")
    columna_locacion = get_column_based_on_query(sql_query)
    sql_query = sql_query.replace("Location",columna_locacion)
    sql_query = sql_query.replace("Municipality", "cole_mcpio_ubicacion")
    sql_query = sql_query.replace("global average", "promedio_global")
    sql_query = sql_query.replace("AVG English","AVG(promedio_ingles)")
    sql_query = sql_query.replace("AVG Math","AVG (promedio_matematicas)")
    sql_query = sql_query.replace("AVG Natural Sciences", "AVG(promedio_c_naturales)")
    sql_query = sql_query.replace("AVG Natural Science", "AVG(promedio_c_naturales)")
    sql_query = sql_query.replace("AVG Social and Citizenship", "AVG(promedio_sociales_ciudadanas)")
    sql_query = sql_query.replace("AVG Critical Reading", "AVG(promedio_lectura_critica)")
    sql_query = sql_query.replace("Year","periodo")
    columna_locacion = get_column_based_on_query(sql_query)
    sql_query = sql_query.replace("Department", columna_locacion)
    sql_query = add_quotes_to_departments(sql_query)
    sql_query = add_quotes_to_municipalities(sql_query)
    if table=="saber11":
        sql_query=apply_inverse_column_rules(sql_query)
        sql_query = sql_query.replace("departmento", "cole_depto_ubicacion")

    return sql_query

CONFIG = {
    'user': 'admin',
    'password': 'Unacontra123.',
    'host': 'icfes.cxa2yu08iduf.us-east-2.rds.amazonaws.com',
    'raise_on_warnings': True,
    'database': 'icfes_11'
}

def execute_query(query, config):
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        connection.close()

        # Convertir los resultados a una lista de diccionarios
        formatted_results = [{'result': row[0]} for row in results]
        return formatted_results
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            return {"error": "Algo está mal con tu nombre de usuario o contraseña"}
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            return {"error": "La base de datos no existe"}
        else:
            return {"error": str(err)}

@app.route('/makepredict', methods=['POST'])
def make_predict():
    types_df = pd.read_csv('variable_types.csv')

    # Crear un diccionario con los tipos de variables
    types_dict = dict(zip(types_df['column_name'], types_df['dtype']))

    data = request.get_json()
    model = joblib.load('model.pkl')
    
    columns_encoded = joblib.load('columns_encoded.pkl')
    # Accede a los valores individuales desde el diccionario data
    periodo = data.get("PERIODO")
    cole_area_ubicacion = data.get("COLE_AREA_UBICACION")
    cole_naturaleza = data.get("COLE_NATURALEZA")
    cole_cod_dane_establecimiento = data.get("COLE_COD_DANE_ESTABLECIMIENTO")
    cole_depto_ubicacion = data.get("COLE_DEPTO_UBICACION")
    cole_mcio_ubicacion = data.get("COLE_MCPIO_UBICACION")
    cole_nombre_establecimiento = data.get("COLE_NOMBRE_ESTABLECIMIENTO")
    estu_depto_presentacion = data.get("ESTU_DEPTO_PRESENTACION")
    estu_mcio_presentacion = data.get("ESTU_MCIO_PRESENTACION")
    estu_fechanacimiento = data.get("ESTU_FECHANACIMIENTO")
    estu_genero = data.get("ESTU_GENERO")
    fami_estratovivienda = data.get("FAMI_ESTRATOVIVIENDA")
    dia, mes, year = descomponer_fecha(estu_fechanacimiento)
    cole_naturaleza="NO OFICIAL"
    cole_area_ubicacion = "URBANO"
    datos_modelo = {
        "PERIODO": periodo,
        "COLE_AREA_UBICACION": cole_area_ubicacion,
        "COLE_NATURALEZA": cole_naturaleza,
        "COLE_COD_DANE_ESTABLECIMIENTO": cole_cod_dane_establecimiento,
        "COLE_DEPTO_UBICACION": cole_depto_ubicacion,
        "COLE_MCPIO_UBICACION": cole_mcio_ubicacion,
        "COLE_NOMBRE_ESTABLECIMIENTO": cole_nombre_establecimiento,
        "ESTU_DEPTO_PRESENTACION": estu_depto_presentacion,
        "ESTU_MCPIO_PRESENTACION": estu_mcio_presentacion,
        "DIA": dia,
        "MES": mes,
        "AÑO": year,
        "ESTU_GENERO": estu_genero,
        "FAMI_ESTRATOVIVIENDA": fami_estratovivienda
    }

    df_modelo = pd.DataFrame([datos_modelo])

    # Convertir las columnas a sus tipos originales
    for col, dtype in types_dict.items():
        if dtype == 'category' or dtype == 'object':
            df_modelo[col] = df_modelo[col].astype('category')
        else:
            df_modelo[col] = df_modelo[col].astype(dtype)

    new_data_encoded = pd.get_dummies(df_modelo)
    new_data_encoded.columns = [col.replace('[', '').replace(']', '').replace('<', '').replace('>', '') for col in new_data_encoded.columns]

    # Alinear las columnas con las del modelo entrenado
    new_data_encoded = new_data_encoded.reindex(columns=columns_encoded, fill_value=0)

    prediccion = model.predict(new_data_encoded)
    
    return jsonify({"message": "Predicción realizada", "data": prediccion.tolist()})


@app.route('/chat', methods=['POST'])
def chat():
    data = request.get_json()
    user_input = data.get('user_input', '')
    if not user_input:
        return jsonify({'error': 'No se recibió entrada del usuario'}), 400

    print(f"Entrada del Usuario: {user_input}")

    # Traducir entrada del usuario a inglés
    translated_text = translate_to_english(user_input)
    print(f"Texto Traducido: {translated_text}")

    # Convertir texto en inglés a consulta SQL
    sql_query = translate_to_sql(translated_text)
    print(f"Consulta SQL: {sql_query}")

    # Formatear consulta SQL
    formatted_sql_query = format_sql_query(sql_query,user_input)
    print(f"Consulta SQL Formateada: {formatted_sql_query}")

    # Asegurarse de que la consulta tenga un rango de años
    if "periodo" not in formatted_sql_query:
        formatted_sql_query += ' AND periodo >= 20142 AND periodo <= 20222'
    else:
        formatted_sql_query = formatted_sql_query.replace("and periodo = ", "and LEFT(periodo, 4) = ")
        formatted_sql_query = formatted_sql_query.replace("periodo = ", "LEFT(periodo, 4) = ")

    if "best" in formatted_sql_query or "saber11" in formatted_sql_query:
        print("pre_formatted: ",formatted_sql_query)
        formatted_sql_query = fix_query(formatted_sql_query)
        print("pre_post",formatted_sql_query)
        formatted_sql_query = construct_new_query(formatted_sql_query)
        
    print(f"Consulta SQL Final: {formatted_sql_query}")

    

    # Ejecutar consulta y obtener resultados
    results = execute_query(formatted_sql_query, CONFIG)
    print(f"Resultados: {results}")

    return jsonify(results)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
