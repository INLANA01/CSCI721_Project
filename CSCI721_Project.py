
#Packages

import pandas as pd
import numpy as np
import ConfigParser
Config = ConfigParser.ConfigParser()
MLB_dataset = pd.read_csv('C:\\Users\\adity\\source\\repos\\CSCI721_Project\\data\\MLB Stats.csv')
#print(MLB_dataset)
chadwick_dataset_batting = pd.read_csv('C:/Users/adity/source/repos/CSCI721_Project/data/baseballdatabank-master/baseballdatabank-master/core/batting.csv')
chadwick_dataset_people = pd.read_csv('C:/Users/adity/source/repos/CSCI721_Project/data/baseballdatabank-master/baseballdatabank-master/core/People.csv')
chadwick_dataset_salaries = pd.read_csv('C:/Users/adity/source/repos/CSCI721_Project/data/baseballdatabank-master/baseballdatabank-master/core/salaries.csv')

join_batting = chadwick_dataset_people.join(chadwick_dataset_batting.set_index('playerID'), on='playerID')
join_salary = join_batting.join(chadwick_dataset_salaries.set_index('playerID'), on='playerID')
#join_salary = join_batting.join(chadwick_dataset_salaries, lsuffix='playerID', rsuffix='playerID')
#print(join_salary)


df = join_salary[['playerIDplayerID', 'nameFirst', 'nameLast', 'R', '2B', 'salary']] 

print(df)


