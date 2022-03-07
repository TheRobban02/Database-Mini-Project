# Import mysql connector, pandas, numpy and msvcrt
import mysql.connector as mysql
import pandas as pd
import os
from columnar import columnar

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
    [5] To show what weapons you can buy at a specific planet or station\n\
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
        
        frame = pd.read_sql("SELECT * FROM selected_weapons", connection)

        pd.set_option('display.expand_frame_repr', False, 'display.max_rows', None)
        frame = frame.rename(columns={'weapon_name': 'Weapon Name', 'type': 'Type', 'price':'Price'})
        print(frame)
        
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
        FROM planets\
        JOIN ships\
        ON ships.buy_location LIKE concat('%', + planets.capital, + '%')\
        WHERE planets.planet_name = '{choice}'")

        frame = pd.read_sql("SELECT * FROM planetsview", connection)
        frame = frame.rename(columns={'capital': 'Capital', 'ship_name': 'Ship name', 'price':'Price'})

        pd.set_option('display.expand_frame_repr', False, 'display.max_rows', None)
        print(frame)

        os.system("pause")
        print("Press any key to return to main menu!")
        cursor.execute("DROP VIEW planetsview")
        mainMenu()

    elif (options == "4"):
        
        cursor.execute(f"CREATE VIEW avg_price AS SELECT \
        ROUND(AVG(price), 2), size FROM weapons GROUP BY size")
        
        frame = pd.read_sql("SELECT * FROM avg_price", connection)

        pd.set_option('display.expand_frame_repr', False, 'display.max_rows', None)
        frame = frame.rename(columns={'ROUND(AVG(price), 2)': 'Price', 'size': 'Size'})

        print(frame)
        
        os.system("pause")
        print("Press any key to return to main menu!")
        cursor.execute("DROP VIEW avg_price")
        mainMenu()

    elif (options == "5"):
        
        choice = input("Type the Planet/Stations name: ")

        planet_lst = ["Hurston", "MicroTech", "Crusader", "ArcCorp"]

        # Checks if you typed a station or a planet
        if(choice not in planet_lst):
            cursor.execute(f"CREATE VIEW weapons_station AS\
            SELECT\
            stations.station_name,\
            weapons.weapon_name,\
            weapons.type,\
            weapons.price\
            FROM Stations\
            INNER JOIN Weapons\
            ON weapons.buy_location LIKE concat('%', + stations.station_id, + '%')\
            WHERE Stations.station_id = '{choice}'")
            frame = pd.read_sql("SELECT * FROM weapons_station", connection)
        else:
            cursor.execute(f"CREATE VIEW weapons_planet AS\
            SELECT\
            planets.capital,\
            weapons.weapon_name,\
            weapons.type,\
            weapons.price\
            FROM Planets\
            INNER JOIN Weapons\
            ON weapons.buy_location LIKE concat('%', + planets.capital, + '%')\
            WHERE planets.planet_name = '{choice}'")
            frame = pd.read_sql("SELECT * FROM weapons_planet", connection)

        pd.set_option('display.expand_frame_repr', False, 'display.max_rows', None)
        frame = frame.rename(columns={'capital': 'Capital', 'weapon_name': 'Weapon Name', 'type': 'Type', 'price': 'Price(UEC)'})

        print(frame)

        os.system("pause")
        print("Press any key to return to main menu!")
        cursor.execute(f"DROP VIEW IF EXISTS weapons_planet, weapons_station")
        mainMenu()
    
    elif (options == "Q" or "q"):
        exit
    
    else:
        print(f"{options} is not a valid option.")
        mainMenu()


# SubMenu is a sub menu to option 1 in the main menu
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

        frame = pd.read_sql("SELECT * FROM planetsdesc", connection)
        pd.set_option('display.expand_frame_repr', False, 'display.max_rows', None)
        frame = frame.reset_index(drop=True)
        print(frame)
        
        os.system("pause")
        cursor.execute("DROP VIEW planetsdesc")
        mainMenu()

    elif (choice == "2"):
        # Creates the viev
        cursor.execute(f"CREATE VIEW shipsdesc AS SELECT * FROM ships")

        frame = pd.read_sql("SELECT * FROM shipsdesc", connection)
        pd.set_option('display.expand_frame_repr', False, 'display.max_rows', None)
        print(frame)

        os.system("pause")
        cursor.execute("DROP VIEW shipsdesc")
        mainMenu()

    elif (choice == "3"):
        # Creates the viev
        cursor.execute(f"CREATE VIEW speciesdesc AS SELECT * FROM species")

        frame = pd.read_sql("SELECT * FROM speciesdesc", connection)
        pd.set_option('display.expand_frame_repr', False, 'display.max_rows', None)
        print(frame)

        os.system("pause")
        cursor.execute("DROP VIEW speciesdesc")
        mainMenu()

    elif (choice == "4"):
        # Creates the viev
        cursor.execute(f"CREATE VIEW stationdesc AS SELECT * FROM stations")

        frame = pd.read_sql("SELECT * FROM stationdesc", connection)
        pd.set_option('display.expand_frame_repr', False, 'display.max_rows', None)
        print(frame)

        os.system("pause")
        cursor.execute("DROP VIEW stationdesc")
        mainMenu()
        
    elif (choice == "5"):
        # Creates the viev
        cursor.execute(f"CREATE VIEW weapondesc AS SELECT * FROM weapons")

        frame = pd.read_sql("SELECT * FROM weapondesc", connection)
        pd.set_option('display.expand_frame_repr', False, 'display.max_rows', None)
        print(frame)

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
