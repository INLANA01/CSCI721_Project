
from sqlalchemy import create_engine
import pymysql
import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
import MySQLdb
import configparser
import pandas as pd
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

def GetDataset(query):
    try:
        cnx = mysql.connector.connect(user=user, password=pword, host=host, database=database, auth_plugin='mysql_native_password')
        mycursor = cnx.cursor()
        mycursor.execute(query)
        DataSet = mycursor.fetchall()
        columns = mycursor.column_names
        df = pd.DataFrame(DataSet, columns = columns)
        return df
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
      else:
        print(err)
    else:
      cnx.close()

query1 = ''' Select playerID, playerName, Season, teamID, League, G, AB, R, H, 2B, 3B, HR, RBI, SB, BB, SO, IBB, HBP, SH, SF, salary from ChadDatasetTable
        GROUP BY playerName, Season
        having
        R != 0
        and G > 10
        and SF != 0
        and season >= 2000
        order by season desc; '''
query2 = '''Select playerID, `Player Name` as playerName,  Season, teamID, League, G, AB, R, H, 2B, 3B, HR, RBI, SB, BB, SO, IBB, HBP, SH, SF, salary from mlb_batting
        GROUP BY playerName, Season
        having
        R != 0
        and G > 10
        and SF != 0
        and season >= 2000 
        order by season desc;'''

def main():
    global query1
    global query2

    setSQLCredentials()
    #Get Chad Dataset
    ChadDs = GetDataset(query1)
    #Get MLB Dataset
    MLBDs = GetDataset(query2)
    #Merge the two Datasets
    result = pd.DataFrame(np.concatenate([ChadDs.values, MLBDs.values]), columns = ChadDs.columns) 
    
    #Rename columns in descriptive way  
    col_names = {"playerID":"playerID", "playerName":"PlayerName", "Season":"Season", "teamID":"TeamID", "League":"LeagueID", "G":"Games", "AB": "atBats", "R":"runsAllowed", "H": "hitsAllowed", "2B":"doubles", "3B":"triples", "HR":"homeRuns", "RBI":"runsBattedIn", "SB":"stolenBases", "BB":"walks", "SO":"striekeOuts", "IBB":"intentionalBasesOnBall", "HBP":"timesHitByPitches", "SH":"sacrificeHits", "SF":"sacrificeFlies", "salary""AB": "atBats", "R":"runsAllowed", "H": "hitsAllowed", "2B":"doubles", "3B":"triples", "HR":"homeRuns", "RBI":"runsBattedIn", "SB":"stolenBases", "BB":"walks", "SO":"striekeOuts", "IBB":"intentionalBasesOnBall", "HBP":"timesHitByPitches", "SH":"sacrificeHits", "SF":"sacrificeFlies", "salary":"salary"}
    result.rename(columns=col_names, inplace= True)

    #Remove inconsistencies in teamID
    mapping = {"CHA":"CHW","CHN":"CHC","FLO":"FLA","KCA":"KCR","LAN":"LAD","MIL":"MLU","NYA":"NYY","NYN":"NYM","SDN":"SDP","SFN":"SFG","SLN":"STL","TBA":"TBD","WAS":"WNA","BFL":"BFB","IHO":"IND","KCC":"KCN","PHA":"PHQ","SYR":"SYS","WNL":"WNA","WNL":"WNA"}    
    for key, values in mapping.items():
        result['teamID'] = np.where(result['teamID'] == key, values, result['teamID'])

    #Discard columns with unique values 
    del result['playerID']
    result.to_csv(r'C:\Users\adity\source\repos\CSCI721_Project\data\baseballdatabank-master\baseballdatabank-master\MergedDataSet\MergedDataSet.csv')

if __name__ == '__main__':
    main()