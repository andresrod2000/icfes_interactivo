from flask import Flask, g, request, jsonify
import pandas as pd
from flask_cors import CORS
import requests


import consultas

app = Flask(__name__)
CORS(app)

PUNTAJES_SABER11 = ['punt_ingles', 'punt_matematicas', 'punt_sociales_ciudadanas', 'punt_c_naturales', 'punt_lectura_critica', 'punt_global']
PROMEDIOS_SABER11 = ['promedio_ingles', 'promedio_matematicas', 'promedio_sociales_ciudadanas', 'promedio_c_naturales', 'promedio_lectura_critica', 'promedio_global']
PUNTAJES_SABERPRO = ['']

app.consulta_inicial = None

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

# <---------------- Consulta inicial por departamento y rango ---------------->
# @app.route('/saber11/consulta_inicial')
# def consulta_departamento():
#     departamento = request.args.get('departamento', type=str)
#     start = request.args.get('start', type=int)
#     end = request.args.get('end', type=int)
    

#     results = consultas.get_consulta_departamento(departamento, start, end)

#     g.consulta_inicial = pd.DataFrame(results)

#     return results

@app.route('/saber11/consulta_inicial')
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
        
        return f'Consulta exitosa {app.consulta_inicial.size}', 200
    
    except Exception as e:
        print(f"Error en consulta_departamento: {str(e)}")
        return jsonify({"error": f"Error al realizar la consulta: {str(e)}"}), 500

@app.route('/saber11/consulta_inicial/test1', methods=['GET'])
def get_data():
    df = app.consulta_inicial
    if df.empty:
        return jsonify({"error": "No hay datos disponibles"}), 400
    
    return df.to_json(orient='records'), 200

@app.route('/saber11/consulta_inicial/test2', methods=['GET'])
def check_dataframe():
    df = app.consulta_inicial
    info = {
        "is_empty": df.empty,
        "shape": df.shape if not df.empty else None,
        "columns": df.columns.tolist() if not df.empty else None,
        "sample": df.head().to_dict(orient='records') if not df.empty else None
    }
    return jsonify(info), 200

@app.route('/saber11/consulta_inicial/estrato')
def consulta_estrato():
    # modo = request.args.get('modo', type=str)
    df = app.consulta_inicial.copy()
    df = df[['fami_estratovivienda', *PUNTAJES_SABER11]].groupby('fami_estratovivienda').mean().round(2).sort_values('fami_estratovivienda')
    df.columns = ['Estrato', *PROMEDIOS_SABER11]
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