
from sqlalchemy import create_engine
import pymysql
import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
import MySQLdb
import configparser
Config = configparser.ConfigParser()
Config.read("config.ini")

def ConfigSectionMap(section):
    # parsing the config
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

def setSQLCredentials():
    # Setting SQL Credentials 
    # These credentials MUST be changed and set as required in order to run the below program
    global user
    global pword
    global host
    global database
    user = ConfigSectionMap("cred")['user']
    pword = ConfigSectionMap("cred")['pword']
    host = ConfigSectionMap("cred")['host']
    database = ConfigSectionMap("cred")['database']

def load(path, table_name):
    global user
    global pword
    global host
    global database
    try:
        df = pd.read_csv(path)
        #engine = create_engine("mysql://" + user + ":" + pword + "@" + host +  "/" + database)
        engine = create_engine("mysql://root:password@localhost/csci721_project")
        con = engine.connect()
        df.to_sql(name = table_name, con=con, if_exists = 'replace')
        con.close()
    except Error as e:
        print("Error while connecting to MySQL", e)

def load_datasets():
    MLB_dataset = ConfigSectionMap("cred")['mlb_dataset']
    chadwick_dataset_batting = ConfigSectionMap("cred")['chadwick_dataset_batting']
    chadwick_dataset_people = ConfigSectionMap("cred")['chadwick_dataset_people']
    chadwick_dataset_salaries = ConfigSectionMap("cred")['chadwick_dataset_salaries']

    table = dict()
    table[MLB_dataset] = 'MLB_batting'
    table[chadwick_dataset_batting] = 'batting'
    table[chadwick_dataset_people] = 'people' 
    table[chadwick_dataset_salaries] = 'salaries'
    for path,table_name in table.items():
        load(path, table_name)


def main():
    setSQLCredentials()
    load_datasets()

if __name__ == '__main__':
    main()