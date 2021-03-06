#https://medevel.com/flask-tutorial-upload-csv-file-and-insert-rows-into-the-database/
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from flask import Flask, render_template, request, redirect, url_for
import os
from os.path import join, dirname, realpath
import xml.etree.ElementTree as ET
import datetime as dt

import pandas as pd
import mysql.connector
import pymysql
from sqlalchemy import create_engine

from manipulatecsv import * #cw file
from xmlwork import * #cw file

app = Flask(__name__)

# enable debugging mode
app.config["DEBUG"] = True

# Upload folder
UPLOAD_FOLDER = 'static/files'
app.config['UPLOAD_FOLDER'] =  UPLOAD_FOLDER


#submittodbcw = pd.read_csv(r"C:\Users\16158\CWScratch\Health Data\CWLWHealthData2022-01-25.csv")
ENV = 'prod'

if ENV == 'dev':

    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user="root",
                                   pw="Knights11",
                                   db="csvdata"))
else:
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                           .format(user="rlyf4otyqxxspeg2",
                                   host='s29oj5odr85rij2o.cbetxkdyhwsb.us-east-1.rds.amazonaws.com:3306',
                                   pw="k5evpiaggcflg26k",
                                   db="i72q4yrl61imjedb"))


# Root URL
@app.route('/')
def index():
     # Set The upload HTML template '\templates\index.html'
    return render_template('index.html')


# Get the uploaded files
@app.route("/submit", methods=['POST'])
def uploadFiles():
      # get the uploaded file
      uploaded_file = request.files['file']
      emailaddress = request.form['email']
      if uploaded_file.filename != '':
           file_path = os.path.join(app.config['UPLOAD_FOLDER'], uploaded_file.filename)
          # set the file path
           uploaded_file.save(file_path)
           parseCSV(file_path, emailaddress)
          # save the file
      #return redirect(url_for('index'))
      return render_template('success.html')

def parseCSV(filePath, emailaddress):
      # CVS Column Names
      #col_names = ['first_name','last_name','address', 'street', 'state' , 'zip']
      # Use Pandas to parse the CSV file
      #csvData = pd.read_csv(filePath,names=col_names, header=None)
      #csvData = csvData.where((pd.notnull(csvData)), None)
      # Loop through the Rows
      #for i,row in csvData.iterrows():
             #sql = "INSERT INTO addresses (first_name, last_name, address, street, state, zip) VALUES (%s, %s, %s, %s, %s, %s)"
             #value = (row['first_name'],row['last_name'],row['address'],row['street'],row['state'],str(row['zip']))
             #mycursor.execute(sql, value, if_exists='append')
             #mycursor.execute(sql, value)
             #mydb.commit()
             #print(i,row['first_name'],row['last_name'],row['address'],row['street'],row['state'],row['zip'])

      # Create the connection and close it(whether successed of failed)
      #csvData = pd.read_csv(filePath)
      pre_process(filePath)
      health_df = convert_xml()
      merged = manipulatecsv(health_df)
      #csvData = csvData.where((pd.notnull(csvData)), None)
      #csvData = csvData.where((pd.notnull(csvData)), None)
      with engine.begin() as connection:
          merged.to_sql(name=emailaddress, con=connection, if_exists='append', index=False)




if (__name__ == "__main__"):
     app.run(port = 5000)
