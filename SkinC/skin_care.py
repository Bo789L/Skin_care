import sqlite3
from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def index():
    # open the connection to the database
    conn = sqlite3.connect('SC_data.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    # fetch data from the deployments table
    cur.execute("select * from deployments")
    rows_deploy = cur.fetchall()
    # fetch data from the status table
    cur.execute("select * from status")
    rows_status = cur.fetchall()
    conn.close()
    return render_template('index.html', rows_deploy=rows_deploy, rows_status=rows_status)