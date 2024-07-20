from flask import Flask, g, request, jsonify
import pandas as pd
from flask_cors import CORS


import consultas

app = Flask(__name__)
CORS(app)

def get_consulta_inicial():
    if 'consulta_inicial' not in g:
        g.consulta_inicial = pd.DataFrame()  # Inicializa un DataFrame vacío
    return g.consulta_inicial

@app.before_request
def before_request():
    get_consulta_inicial() 

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
@app.route('/saber11/consulta_inicial')
def consulta_departamento():
    departamento = request.args.get('departamento', type=str)
    start = request.args.get('start', type=int)
    end = request.args.get('end', type=int)
    

    results = consultas.get_consulta_departamento(departamento, start, end)

    g.consulta_inicial = pd.DataFrame(results)

    return results

# @app.route('/saber11/consulta_inicial/test1', methods=['GET'])
# def get_data():
#     df = get_consulta_inicial()
#     if df.empty:
#         return jsonify({"error": "No hay datos disponibles"}), 400
    
#     return df.to_json(orient='records'), 200

@app.route('/saber11/consulta_inicial')
def consulta_departamento():
    departamento = request.args.get('departamento', type=str)
    start = request.args.get('start', type=int)
    end = request.args.get('end', type=int)
    
    if not all([departamento, start, end]):
        return jsonify({"error": "Faltan parámetros requeridos"}), 400

    try:
        results = consultas.get_consulta_departamento(departamento, start, end)
        
        # Convertir los resultados a un DataFrame y guardarlo en g
        g.consulta_inicial = pd.DataFrame(results)
        
        # Verificar que se ha guardado correctamente
        if hasattr(g, 'consulta_inicial') and isinstance(g.consulta_inicial, pd.DataFrame):
            print(f"DataFrame guardado en g.consulta_inicial. Shape: {g.consulta_inicial.shape}")
            
            # Devolver información sobre el DataFrame guardado
            return jsonify({
                "message": "Consulta realizada y guardada exitosamente",
                "shape": g.consulta_inicial.shape,
                "columns": g.consulta_inicial.columns.tolist(),
                "sample": g.consulta_inicial.head().to_dict(orient='records')
            }), 200
        else:
            return jsonify({"error": "Error al guardar el DataFrame en g"}), 500
    
    except Exception as e:
        print(f"Error en consulta_departamento: {str(e)}")
        return jsonify({"error": f"Error al realizar la consulta: {str(e)}"}), 500

@app.after_request
def after_request(response):
    if hasattr(g, 'consulta_inicial'):
        print(f"After request: consulta_inicial DataFrame shape: {g.consulta_inicial.shape}")
    return response

@app.route('/check_dataframe', methods=['GET'])
def check_dataframe():
    df = get_consulta_inicial()
    info = {
        "is_empty": df.empty,
        "shape": df.shape if not df.empty else None,
        "columns": df.columns.tolist() if not df.empty else None,
        "sample": df.head().to_dict(orient='records') if not df.empty else None
    }
    return jsonify(info), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)