# Import mysql connector, pandas, numpy and msvcrt
import mysql.connector as mysql
import pandas as pd

# Connect to SQL Server
connection = mysql.connect(

    host = "localhost",
    user = "root",
    passwd = "root"

)

DB_NAME = "starCitizen"

def CreateDatabase():

    cursor = connection.cursor(buffered=True)

    # Create the database
    cursor.execute("create database {}".format(DB_NAME))
    
    cursor.execute("select database();")
    
    # Create Table
    cursor.execute(f"USE {DB_NAME}")
    cursor.execute("CREATE TABLE Planets (planet_name nvarchar(50) not null primary key, capital nvarchar(50), system nvarchar(20),\
        moons nvarchar(100), space_station nvarchar(50), min_temp int, max_temp int)")
    
    # Create Table
    cursor.execute(f"USE {DB_NAME}")
    cursor.execute("CREATE TABLE Species (species_name varchar(50) not null primary key, sentient nvarchar(20), language nvarchar(20),\
        goverment nvarchar(100))")

    cursor.execute(f"USE {DB_NAME}")
    cursor.execute("CREATE TABLE Ships (ship_name nvarchar(50) not null primary key, manufacturer nvarchar(60), price int,\
        buy_location nvarchar(50), role nvarchar(50), cargo_capacity long, lenght long, race nvarchar(20))")

    # Create Table
    cursor.execute(f"USE {DB_NAME}")
    cursor.execute("CREATE TABLE Weapons (weapon_name nvarchar(50) not null primary key, type nvarchar(50), size int,\
        dps int, fire_rate int, buy_location nvarchar(50), price int)")

    cursor.execute(f"USE {DB_NAME}")
    cursor.execute("CREATE TABLE Stations (station_id nvarchar(50) not null primary key, planet nvarchar(50), refinery nvarchar(20),\
        black_market nvarchar(20), hangars int, pads int, docking_ports int, station_name nvarchar(50), system nvarchar(20))")

    PopulateTables()

def PopulateTables():
    
    #loop through the data frames
    data = pd.read_csv(r'planets.csv', delimiter=";")
    for i,row in data.iterrows():
            
        #here %S means string values 
        sql = f"INSERT INTO {DB_NAME}.planets VALUES (%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
            
        #save our changes
        connection.commit()
    
    #loop through the data frame
    data = pd.read_csv(r'species.csv', delimiter=";")
    for i,row in data.iterrows():
        
        #here %S means string values 
        sql = f"INSERT INTO {DB_NAME}.species (species_name, sentient, language, goverment) VALUES (%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
        
        #save our changes
        connection.commit()

    #loop through the data frames
    data = pd.read_csv(r'ships.csv', delimiter=";")
    for i,row in data.iterrows():
            
        #here %S means string values 
        sql = f"INSERT INTO {DB_NAME}.ships VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
            
        #save our changes
        connection.commit()
    
    #loop through the data frames
    data = pd.read_csv(r'weapons.csv', delimiter=";")
    for i,row in data.iterrows():
            
        #here %S means string values 
        sql = f"INSERT INTO {DB_NAME}.weapons VALUES (%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
            
        #save our changes
        connection.commit()
    
    #loop through the data frames
    data = pd.read_csv(r'spacestations.csv', delimiter=";")
    for i,row in data.iterrows():
            
        #here %S means string values 
        sql = f"INSERT INTO {DB_NAME}.stations VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
            
        #save our changes
        connection.commit()


cursor = connection.cursor()
cursor.execute("show databases")
lst = cursor.fetchall()

CreateDatabase()
