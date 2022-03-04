# Import mysql connector, pandas, numpy and msvcrt
import mysql.connector as mysql
import pandas as pd

# Connect to SQL Server
connection = mysql.connect(

    host = "localhost",
    user = "root",
    passwd = "root"

)
<<<<<<< HEAD
=======

# Main Menu
def MainMenu():

    cursor = connection.cursor(buffered=True)
    cursor.execute(f"USE {DB_NAME}")

    choice = input("\n1. List all planets\
        \n2. Search for planet details\
        \n3. Search for species with height higher than given number\
        \n4. What is the most likely desired climate of the given species?\
        \n5. What is the average lifespan per species classification?\
        \nQ: Quit\
        \n--------\
        \nChoice:  ")

    if (choice == "1"):
        
        cursor.execute("SELECT name FROM Planets")
        records = cursor.fetchall()
        
        for row in records:
            print(row[0])
        
        print("\nPress any key to return to main menu: ")

        # Listens to any keypress
        if(msvcrt.getch()):
            MainMenu()

    elif (choice == "2"):

        planetName = input("Enter the name of the planet: ")
        cursor.execute(f"SELECT * FROM Planets WHERE name = '{planetName}'")
        records = cursor.fetchall()

        for row in records:
            for i in row:
                print(i)
        
        print("\nPress any key to return to main menu: ")

        # Listens to any keypress
        if(msvcrt.getch()):
            MainMenu()

    elif (choice == "3"):

        height = int(input("Enter the average height of the species: "))
        cursor.execute(f"SELECT name FROM Species WHERE average_height > {height}")
        records = cursor.fetchall()

        for row in records:
            print(row[0])
        
        print("\nPress any key to return to main menu: ")

        # Listens to any keypress
        if(msvcrt.getch()):
            MainMenu()

    elif (choice == "4"):

        specie = input("Enter the name of the specie: ")
        cursor.execute(f"SELECT climate FROM Planets, Species WHERE Species.name = '{specie}' and Species.homeworld = Planets.name")

        records = cursor.fetchall()

        for row in records:
            print(f"Desired climate: {row[0]}")
        
        print("\nPress any key to return to main menu: ")

        # Listens to any keypress
        if(msvcrt.getch()):
            MainMenu()

    elif (choice == "5"):
        cursor.execute(f"SELECT classification, ROUND(AVG(average_lifespan), 1) FROM Species WHERE average_lifespan != '0' and classification != '0' GROUP BY classification")
        records = cursor.fetchall()

        for row in records:
            if(row[1]) == 1010:
                print(f"Classification: {row[0]}, Average Lifespan: Indefinite")
            else:
                print(f"Classification: {row[0]}, Average Lifespan: {row[1]}")

        print("\nPress any key to return to main menu: ")
        
        # Listens to any keypress
        if(msvcrt.getch()):
            MainMenu()

    else:
        exit()

# This will need to change
>>>>>>> e1f81e4a5fce225af1ed064dfec650d249b12130
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

<<<<<<< HEAD
# Removes row in planets where name is NaN
dataP.dropna(subset = ["name"], inplace=True)

# Replaces indefinite with 1010 to easier handle it
dataS["average_lifespan"].replace({"indefinite": 1010}, inplace=True)

# Change reptilian to reptile and mammals to mammal
dataS["classification"].replace({"reptilian": "reptile"}, inplace=True)
dataS["classification"].replace({"mammals": "mammal"}, inplace=True)

=======
>>>>>>> e1f81e4a5fce225af1ed064dfec650d249b12130
cursor = connection.cursor()
cursor.execute("show databases")
lst = cursor.fetchall()

# Checks if Database exists
if(DB_NAME,) in lst:
    MainMenu()
else:
    CreateDatabes()
