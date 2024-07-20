from flask import Flask, jsonify, request
import pandas as pd
import json

import consultas

app = Flask(__name__)

conn = consultas.connect_to_database()

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/promedios_colombia')
def promedios_colombia():
    # start = request.args.get('start', default=20142, type=int)
    # end = request.args.get('end', default=20222, type=int)
    # results = consultas.get_promedios_colombia(conn, start, end)
    results = consultas.get_promedios_colombia(conn)
    return results

if __name__ == '__main__':
    app.run(debug=True)

    