from flask import Flask, g, request, jsonify
import pandas as pd
from flask_cors import CORS
import requests


import consultas

app = Flask(__name__)
CORS(app)

PUNTAJES_SABER11 = ['punt_ingles', 'punt_matematicas', 'punt_sociales_ciudadanas', 'punt_c_naturales', 'punt_lectura_critica', 'punt_global']
PROMEDIOS_SABER11 = ['promedio_ingles', 'promedio_matematicas', 'promedio_sociales_ciudadanas', 'promedio_c_naturales', 'promedio_lectura_critica', 'promedio_global']
PUNTAJES_SABERPRO = ['punt_ingles', 'punt_razona_cuantitativo', 'punt_c_ciudadana', 'punt_lectura_critica', 'punt_comuni_escrita']
PROMEDIOS_SABERPRO = ['promedio_ingles', 'promedio_razona_cuantitativo', 'promedio_c_ciudadana', 'promedio_lectura_critica', 'promedio_comuni_escrita']

app.consulta_inicial = None
app.consulta_municipio = None

app.consulta_inicial_pro = None
app.consulta_municipio_pro = None

# <---------------- Estaticos del inicio ---------------->
@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/saber11/promedios_colombia')
def promedios_colombia():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_colombia(conn, start, end)
    results = consultas.get_promedios_colombia()
    return results

@app.route('/saber11/promedios_departamento')
def promedios_departamento():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_departamento(conn, start, end)
    results = consultas.get_promedios_departamento()
    return results

@app.route('/saber11/promedios_colombia_year')
def promedios_colombia_year():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_colombia_year(conn, start, end)
    results = consultas.get_promedios_colombia_year()
    return results

@app.route('/saber11/promedios_departamento_year')
def promedios_departamento_year():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_departamento_year(conn, start, end)
    results = consultas.get_promedios_departamento_year()
    return results

@app.route('/saberpro/promedios_colombia')
def promedios_colombia_saberpro():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_colombia(conn, start, end)
    results = consultas.get_promedios_colombia_pro()
    return results

@app.route('/saberpro/promedios_departamento')
def promedios_departamento_saberpro():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_departamento(conn, start, end)
    results = consultas.get_promedios_departamento_pro()
    return results

@app.route('/saberpro/promedios_colombia_year')
def promedios_colombia_year_saberpro():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_colombia_year(conn, start, end)
    results = consultas.get_promedios_colombia_pro_year()
    return results

@app.route('/saberpro/promedios_departamento_year')
def promedios_departamento_year_saberpro():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_departamento_year(conn, start, end)
    results = consultas.get_promedios_departamento_pro_year()
    return results

# <--------------------------------Saber 11-------------------------------->

# <---------------- Consulta inicial por departamento y rango ---------------->
@app.route('/saber11/consulta_inicial/DELETE')
def delete_consulta_inicial():
    app.consulta_inicial = None
    return 'Consulta inicial eliminada', 200

@app.route('/saber11/consulta_inicial')
# Carga la consulta inicial por departamento y rango de años en un DataFrame y lo guarda en el contexto de la aplicación
def consulta_departamento():
    departamento = request.args.get('departamento', type=str)
    start = request.args.get('start', type=int)
    end = request.args.get('end', type=int)
    
    if not all([departamento, start, end]):
        return jsonify({"error": "Faltan parámetros requeridos"}), 400

    try:
        results = consultas.get_consulta_departamento(departamento, start, end)
        
        # Convertir los resultados a un DataFrame y guardarlo en el contexto de la aplicación
        app.consulta_inicial = pd.DataFrame(results)

        # Reiniciar la consulta por municipio
        app.consulta_municipio = None
        
        return f'Consulta exitosa, size de la consulta: {app.consulta_inicial.size}', 200
    
    except Exception as e:
        print(f"Error en consulta_departamento: {str(e)}")
        return jsonify({"error": f"Error al realizar la consulta: {str(e)}"}), 500

@app.route('/saber11/consulta_inicial/test', methods=['GET'])
# Verifica si la consulta inicial se cargó correctamente en el contexto de la aplicación
def check_dataframe():
    df = app.consulta_inicial
    info = {
        "is_empty": df.empty,
        "shape": df.shape if not df.empty else None,
        "columns": df.columns.tolist() if not df.empty else None,
        "sample": df.head().to_dict(orient='records') if not df.empty else None
    }
    return jsonify(info), 200

@app.route('/saber11/consulta_inicial/municipios')
# Obtiene la lista de municipios disponibles en la consulta inicial
def get_municipios():
    if app.consulta_inicial is None:
        return jsonify({"error": "No se ha cargado la consulta inicial"}), 400
    df = app.consulta_inicial.copy()
    municipios = df['cole_mcpio_ubicacion'].sort_values().unique().tolist()
    return municipios

# <-------------------- Consulta por municipio ------------------------->

@app.route('/saber11/consulta_municipio')
# Selecciona la parte del dataframe correspondiente a un municipio específico
def consulta_municipio():
    municipio = request.args.get('municipio', type=str)
    if not municipio:
        return jsonify({"error": "Falta el parámetro 'municipio'"}), 400

    df = app.consulta_inicial.copy()
    df = df[df['cole_mcpio_ubicacion'] == municipio]
    df.drop(columns=['cole_mcpio_ubicacion'], inplace=True)
    app.consulta_municipio = df
    return f'Consulta exitosa, size de la consulta: {app.consulta_municipio.size}', 200

@app.route('/saber11/consulta_municipio/test', methods=['GET'])
# Verifica si la consulta por municipio se guardó correctamente en el contexto de la aplicación
def check_dataframe_municipio():
    df = app.consulta_municipio
    info = {
        "is_empty": df.empty,
        "shape": df.shape if not df.empty else None,
        "columns": df.columns.tolist() if not df.empty else None,
        "sample": df.head().to_dict(orient='records') if not df.empty else None
    }
    return jsonify(info), 200

# <--------------------- Consultas generales (estados) --------------------->
@app.route('/saber11/consultas/historico')
# Obtiene los promedios de los puntajes SABER 11 por periodo
def consulta_historico():
    municipio = request.args.get('municipio', type=str)

    if not municipio or municipio == 'Seleccione':
        df = app.consulta_inicial.copy()
    
    elif app.consulta_municipio is not None:
        df = app.consulta_municipio.copy()
    
    else:
        return jsonify({"error": "No se ha cargado la consulta por municipio"}), 400
    
    df = df[['periodo', *PUNTAJES_SABER11]].groupby('periodo', as_index=False).mean().round(2).sort_values('periodo')
    df.columns = ['periodo', *PROMEDIOS_SABER11]
    return df.to_json(orient='records'), 200
    

@app.route('/saber11/consultas/estrato')
# Obtiene los promedios de los puntajes SABER 11 por estrato
def consulta_estrato():
    municipio = request.args.get('municipio', type=str)

    if not municipio or municipio == 'Seleccione':
        df = app.consulta_inicial.copy()

    elif app.consulta_municipio is not None:
        df = app.consulta_municipio.copy()
    
    else:
        return jsonify({"error": "No se ha cargado la consulta por municipio"}), 400

    df = df[['fami_estratovivienda', *PUNTAJES_SABER11]].groupby('fami_estratovivienda', as_index=False).mean().round(2).sort_values('fami_estratovivienda')
    df.columns = ['Estrato', *PROMEDIOS_SABER11]
    return df.to_json(orient='records'), 200

# <---------------------------- Saber Pro ---------------------------->

# <---------------- Consulta inicial por departamento y rango ---------------->

@app.route('/saberpro/consulta_inicial/DELETE')
def delete_consulta_inicial_pro():
    app.consulta_inicial_pro = None
    return 'Consulta inicial eliminada', 200

@app.route('/saberpro/consulta_inicial')
def consulta_departamento_pro():
    departamento = request.args.get('departamento', type=str)
    start = request.args.get('start', type=int)
    end = request.args.get('end', type=int)
    
    if not all([departamento, start, end]):
        return jsonify({"error": "Faltan parámetros requeridos"}), 400

    try:
        results = consultas.get_consulta_departamento_pro(departamento, start, end)
        
        # Convertir los resultados a un DataFrame y guardarlo en el contexto de la aplicación
        app.consulta_inicial_pro = pd.DataFrame(results)

        # Reiniciar la consulta por municipio
        app.consulta_municipio_pro = None
        
        return f'Consulta exitosa, size de la consulta: {app.consulta_inicial_pro.size}', 200
    
    except Exception as e:
        print(f"Error en consulta_departamento_pro: {str(e)}")
        return jsonify({"error": f"Error al realizar la consulta: {str(e)}"}), 500
    
@app.route('/saberpro/consulta_inicial/test', methods=['GET'])
def check_dataframe_pro():
    df = app.consulta_inicial_pro
    info = {
        "is_empty": df.empty,
        "shape": df.shape if not df.empty else None,
        "columns": df.columns.tolist() if not df.empty else None,
        "sample": df.head().to_dict(orient='records') if not df.empty else None
    }
    return jsonify(info), 200

@app.route('/saberpro/consulta_inicial/municipios')
def get_municipios_pro():
    if app.consulta_inicial_pro is None:
        return jsonify({"error": "No se ha cargado la consulta inicial"}), 400
    df = app.consulta_inicial_pro.copy()
    municipios = df['ESTU_INST_MUNICIPIO'].sort_values().unique().tolist()
    return municipios

# <-------------------- Consulta por municipio ------------------------->

@app.route('/saberpro/consulta_municipio')
def consulta_municipio_pro():
    municipio = request.args.get('municipio', type=str)
    if not municipio:
        return jsonify({"error": "Falta el parámetro 'municipio'}"}), 400

    df = app.consulta_inicial_pro.copy()
    df = df[df['ESTU_INST_MUNICIPIO'] == municipio]
    df.drop(columns=['ESTU_INST_MUNICIPIO'], inplace=True)
    app.consulta_municipio_pro = df
    return f'Consulta exitosa, size de la consulta: {app.consulta_municipio_pro.size}', 200

@app.route('/saberpro/consulta_municipio/test', methods=['GET'])
def check_dataframe_municipio_pro():
    df = app.consulta_municipio_pro
    info = {
        "is_empty": df.empty,
        "shape": df.shape if not df.empty else None,
        "columns": df.columns.tolist() if not df.empty else None,
        "sample": df.head().to_dict(orient='records') if not df.empty else None
    }
    return jsonify(info), 200

# <--------------------- Consultas generales (estados) --------------------->
@app.route('/saberpro/consultas/historico')
def consulta_historico_pro():
    municipio = request.args.get('municipio', type=str)

    if not municipio or municipio == 'Seleccione':
        df = app.consulta_inicial_pro.copy()
    
    elif app.consulta_municipio_pro is not None:
        df = app.consulta_municipio_pro.copy()
    
    else:
        return jsonify({"error": "No se ha cargado la consulta por municipio"}), 400
    
    df = df[['periodo', *PUNTAJES_SABERPRO]].groupby('periodo', as_index=False).mean().round(2).sort_values('periodo')
    df.columns = ['periodo', *PROMEDIOS_SABERPRO]
    return df.to_json(orient='records'), 200

@app.route('/saberpro/consultas/estrato')
def consulta_estrato_pro():
    municipio = request.args.get('municipio', type=str)

    if not municipio or municipio == 'Seleccione':
        df = app.consulta_inicial_pro.copy()

    elif app.consulta_municipio_pro is not None:
        df = app.consulta_municipio_pro.copy()

    else:
        return jsonify({"error": "No se ha cargado la consulta por municipio"}), 400

    df = df[['FAMI_ESTRATOVIVIENDA', *PUNTAJES_SABERPRO]].groupby('FAMI_ESTRATOVIVIENDA', as_index=False).mean().round(2).sort_values('FAMI_ESTRATOVIVIENDA')
    df.columns = ['Estado', *PROMEDIOS_SABERPRO]
    return df.to_json(orient='records'), 200


#<---------------- Integración con Rasa ---------------->
@app.route('/chat', methods=['POST'])
def chat():
    user_message = request.json.get('message')

    # Enviar mensaje al servidor de Rasa
    rasa_response = requests.post('http://localhost:5005/webhooks/rest/webhook', json={"sender": "user", "message": user_message})
    rasa_response_json = rasa_response.json()

    # Extraer la respuesta del bot de Rasa
    bot_response = ''
    if rasa_response_json:
        bot_response = rasa_response_json[0].get('text')

    return jsonify({"response": bot_response})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)