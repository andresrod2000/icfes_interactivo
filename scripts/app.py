from flask import Flask, jsonify, request
import pandas as pd
import json

import consultas

app = Flask(__name__)

conn = consultas.connect_to_database()

# <---------------- Estaticos del inicio ---------------->
@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/saber11/promedios_colombia')
def promedios_colombia():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_colombia(conn, start, end)
    results = consultas.get_promedios_colombia(conn)
    return results

@app.route('/saber11/promedios_departamento')
def promedios_departamento():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_departamento(conn, start, end)
    results = consultas.get_promedios_departamento(conn)
    return results

@app.route('/saber11/promedios_colombia_year')
def promedios_colombia_year():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_colombia_year(conn, start, end)
    results = consultas.get_promedios_colombia_year(conn)
    return results

@app.route('/saber11/promedios_departamento_year')
def promedios_departamento_year():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_departamento_year(conn, start, end)
    results = consultas.get_promedios_departamento_year(conn)
    return results

@app.route('/saberpro/promedios_colombia')
def promedios_colombia_saberpro():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_colombia(conn, start, end)
    results = consultas.get_promedios_colombia_pro(conn)
    return results

@app.route('/saberpro/promedios_departamento')
def promedios_departamento_saberpro():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_departamento(conn, start, end)
    results = consultas.get_promedios_departamento_pro(conn)
    return results

@app.route('/saberpro/promedios_colombia_year')
def promedios_colombia_year_saberpro():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_colombia_year(conn, start, end)
    results = consultas.get_promedios_colombia_pro_year(conn)
    return results

@app.route('/saberpro/promedios_departamento_year')
def promedios_departamento_year_saberpro():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_departamento_year(conn, start, end)
    results = consultas.get_promedios_departamento_pro_year(conn)
    return results

# <----------------  ---------------->

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

    