# Import mysql connector, pandas, numpy and msvcrt
import mysql.connector as mysql
import pandas as pd

# Connect to SQL Server
connection = mysql.connect(

    host = "localhost",
    user = "root",
    passwd = "root"

)
def CreateDatabes():

    cursor = connection.cursor(buffered=True)

    # Create the database
    cursor.execute("create database {}".format(DB_NAME)) 
    
    cursor.execute("select database();")
    
    # Create Table
    cursor.execute(f"USE {DB_NAME}")
    cursor.execute("CREATE TABLE Planets (name nvarchar(50) not null, rotation_period long, orbital_period long, \
        diameter long, climate nvarchar(100), gravity nvarchar(100), terrain nvarchar(100), surface_water long, population long)")

    # Create Table
    cursor.execute(f"USE {DB_NAME}")
    cursor.execute("CREATE TABLE Species (name varchar(50) not null, classification nvarchar(50), designation nvarchar(20), \
        average_height int, skin_colors nvarchar(100), hair_colors nvarchar(100), eye_colors nvarchar(100), average_lifespan int, \
             language nvarchar(100), homeworld nvarchar(50))")

    #loop through the data frame
    for i,row in dataP.iterrows():
        
        #here %S means string values 
        sql = f"INSERT INTO {DB_NAME}.planets VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
        
        #save our changes
        connection.commit()
    
    #loop through the data frame
    for i,row in dataS.iterrows():
        
        #here %S means string values 
        sql = f"INSERT INTO {DB_NAME}.species VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
        
        #save our changes
        connection.commit()

DB_NAME = "starCitizen"

# Pandas reads csv file and creates data frames dataP and dataS
dataPlanets = pd.read_csv (r'planets.csv', delimiter=",")
dataSpecies = pd.read_csv (r'species.csv', delimiter=",")
dataShips = pd.read_csv(r'SpaceStations.csv', delimiter=",")
# TODO dataWeapons = pd.read_csv(r'')

# Removes row in planets where name is NaN
dataP.dropna(subset = ["name"], inplace=True)

# Replaces indefinite with 1010 to easier handle it
dataS["average_lifespan"].replace({"indefinite": 1010}, inplace=True)

# Change reptilian to reptile and mammals to mammal
dataS["classification"].replace({"reptilian": "reptile"}, inplace=True)
dataS["classification"].replace({"mammals": "mammal"}, inplace=True)

cursor = connection.cursor()
cursor.execute("show databases")
lst = cursor.fetchall()

# Checks if Database exists
if(DB_NAME,) in lst:
    MainMenu()
else:
    CreateDatabes()
