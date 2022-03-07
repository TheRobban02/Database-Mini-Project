# Import mysql connector, pandas, numpy and msvcrt
import mysql.connector as mysql
import pandas as pd
import os

# Connect to SQL Server
connection = mysql.connect(

    host = "localhost",
    user = "root",
    passwd = "root"

)

DB_NAME = "starcitizen"

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
    
    # Go to main menu
    mainMenu()


def mainMenu(): 
    
    #Creates and runs the mainmenu
    cursor.execute(f"USE {DB_NAME}")
    options = input("Please make a choice:\n\
    [1] To choose a specific list to view\n\
    [2] To list all weapons with a specific maximum cost\n\
    [3] To show what ships you can buy at a specific planet\n\
    [4] To show the average price of weapon sizes\n\
    [5] To show what weapons you can buy at a specific planet\n\
    [6] To show what language a ship manufacturer speaks\n\
    [Q] To quit the program\n    Enter a number and press enter: ")

    if (options == "1"):
        subMenu()

    elif (options == "2"):
        
        choice = int(input("Whats the maximum price of the weapons to be shown? "))
        
        cursor.execute(f"CREATE VIEW selected_weapons AS SELECT \
        weapon_name, \
        type, \
        price \
        FROM Weapons \
        WHERE price < {choice}")
        
        # fetches the data from the database selected from execute
        cursor.execute(f"SELECT * FROM selected_weapons")

        for i in cursor:
            for k in i:
                print(k, end=" | ")
            print()
        
        os.system("pause")
        cursor.execute("DROP VIEW selected_weapons")
        mainMenu()
            
    elif (options == "3"):
        
        choice = input("Type the planet name: ") 
        cursor.execute(f"CREATE VIEW planetsview AS\
        SELECT\
        planets.capital,\
        ships.ship_name,\
        ships.price\
        FROM Planets\
        INNER JOIN ships\
        ON MATCH(planets.capital) AGAINST('ships.buy_location')\
        WHERE planets.planet_name = '{choice}'")

        cursor.execute(f"SELECT * FROM planetsview")

        for i in cursor:
            for k in i:
                print(k, end=" ")
            print()

        print("Press any key to return to main menu!")
        cursor.execute("DROP VIEW planetsview")
        os.system("pause")
        mainMenu()

    elif (options == "4"):
        
        cursor.execute(f"CREATE VIEW avg_price AS SELECT \
        ROUND(AVG(price), 2), size FROM weapons GROUP BY size")

        # fetches the data from the database selected from execute
        cursor.execute(f"SELECT * FROM avg_price")

        for i in cursor:
            print(f"Size: {i[1]} Weapon, Average cost: {i[0]}$")
        
        print("Press any key to return to main menu!")
        cursor.execute("DROP VIEW avg_price")
        os.system("pause")
        mainMenu()

    elif (options == "5"):
        
        choice = input("Type the planet name: ")

        planet_lst = ["Hurston", "MicroTech", "Crusader", "ArcCorp"]

        if(choice not in planet_lst):
            cursor.execute(f"CREATE VIEW weapons_station AS\
            SELECT\
            stations.station_name,\
            weapons.weapon_name,\
            weapons.price\
            FROM Stations\
            INNER JOIN Weapons\
            ON stations.station_id = weapons.buy_location\
            WHERE Stations.station_id = '{choice}'")
            cursor.execute(f"SELECT * FROM weapons_station")
        else:
            cursor.execute(f"CREATE VIEW weapons_planet AS\
            SELECT\
            planets.capital,\
            weapons.weapon_name,\
            weapons.price\
            FROM Planets\
            INNER JOIN Weapons\
            ON planets.capital = weapons.buy_location\
            WHERE planets.planet_name = '{choice}'")
            cursor.execute(f"SELECT * FROM weapons_planet")

        for i in cursor:
            print(f"City/Station: {i[0]}, Name: {i[1]}, Price: {i[2]}$")
        
        os.system("pause")
        print("Press any key to return to main menu!")
        cursor.execute(f"DROP VIEW IF EXISTS weapons_planet, weapons_station")
        mainMenu()
    
    elif (options == "Q" or "q"):
        exit
    
    else:
        print(f"{options} is not a valid option.")
        mainMenu()


def subMenu():

    choice = input("Choose what type to show\n\
    [1] To list full description on all the planets\n\
    [2] To list full description on all ship\n\
    [3] To list full description on all species\n\
    [4] To list full description on all space stations\n\
    [5] To list full description on all weapons\n\
    [B] To go back to main menu\n\
    Enter a number and press enter: ")

    if (choice == "1"):
        # Creates the viev
        cursor.execute(f"CREATE VIEW planetsdesc AS SELECT * FROM planets")

        cursor.execute("SELECT * FROM planetsdesc")

        for i in cursor:
            for k in i:
                print(k, end=" | ")
            print()

        os.system("pause")
        cursor.execute("DROP VIEW planetsdesc")
        mainMenu()

    elif (choice == "2"):
        # Creates the viev
        cursor.execute(f"CREATE VIEW shipsdesc AS SELECT * FROM ships")

        cursor.execute("SELECT * FROM shipsdesc")

        for i in cursor:
            for k in i:
                print(k, end=" | ")
            print()

        os.system("pause")
        cursor.execute("DROP VIEW shipsdesc")
        mainMenu()

    elif (choice == "3"):
        # Creates the viev
        cursor.execute(f"CREATE VIEW speciesdesc AS SELECT * FROM species")

        cursor.execute("SELECT * FROM speciesdesc")

        for i in cursor:
            for k in i:
                print(k, end=" | ")
            print()

        os.system("pause")
        cursor.execute("DROP VIEW speciesdesc")
        mainMenu()

    elif (choice == "4"):
        # Creates the viev
        cursor.execute(f"CREATE VIEW stationdesc AS SELECT * FROM stations")

        cursor.execute("SELECT * FROM stationdesc")

        for i in cursor:
            for k in i:
                print(k, end=" | ")
            print()

        os.system("pause")
        cursor.execute("DROP VIEW stationdesc")
        mainMenu()
        
    elif (choice == "5"):
        # Creates the viev
        cursor.execute(f"CREATE VIEW weapondesc AS SELECT * FROM weapons")

        cursor.execute("SELECT * FROM weapondesc")

        for i in cursor:
            for k in i:
                print(k, end=" | ")
            print()

        os.system("pause")
        cursor.execute("DROP VIEW weapondesc")
        mainMenu()
        
    elif (choice == "B" or "b"):
        mainMenu()

    else:
        print(f"{choice} is not a valid option.")
        subMenu()


cursor = connection.cursor()
cursor.execute("show databases")
lst = cursor.fetchall()

if (DB_NAME,) in lst:
    mainMenu()
else:
    CreateDatabase()
