import requests
import json
import pandas as pd
import sqlite3
import math
import io

def haversine(coord1, coord2):
    R = 6372800  # Earth radius in meters
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    
    phi1, phi2 = math.radians(lat1), math.radians(lat2) 
    dphi       = math.radians(lat2 - lat1)
    dlambda    = math.radians(lon2 - lon1)
    
    a = math.sin(dphi/2)**2 + \
        math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    
    return 2*R*math.atan2(math.sqrt(a), math.sqrt(1 - a))

def position(i):
    return (R['data'][i]["location"]["latitude"],R['data'][i]["location"]["longitude"])

user_lat = float(input("longitude utilisateur :"))
user_long = float(input("lattitude utilisateur :"))
user_coord = (user_lat,user_long)

r = requests.get("http://api.pioupiou.fr/v1/live-with-meta/all")
R = r.json()

liste_position = []
for i in range(len(R["data"])):
    liste_position.append((R['data'][i]["id"],
                           str(position(i)),
                           str(R['data'][i]["location"]["success"]),
                            haversine(user_coord,position(i))))

con = sqlite3.connect("donnees_pioupiou.sq3")
cur = con.cursor()






cur.execute("CREATE TABLE IF NOT EXISTS position_stations (id integer PRIMARY KEY,coordonnÃ©es,statut,distance_user)")
cur.executemany("INSERT INTO position_stations values (?, ?, ?, ?)", liste_position)

con.commit()

cur.execute("select id from position_stations order by distance_user")

liste_fetchall = cur.fetchall()[0:3]
liste_id_3_stations = []

for e in liste_fetchall:
    liste_id_3_stations.append(e[0])


r1 = requests.get(f"http://api.pioupiou.fr/v1/live-with-meta/{liste_id_3_stations[0]}")
r2 = requests.get(f"http://api.pioupiou.fr/v1/live-with-meta/{liste_id_3_stations[1]}")
r3 = requests.get(f"http://api.pioupiou.fr/v1/live-with-meta/{liste_id_3_stations[2]}")
R1 = r1.json()
R2 = r2.json()
R3 = r3.json()


cur.execute("CREATE TABLE IF NOT EXISTS stations_infos (id integer PRIMARY KEY,name,description,date_installation,rating,FOREIGN KEY (id) REFERENCES position_stations(id))")
cur.execute("insert into stations_infos values (?, ?, ?, ?, ?)", (R1["data"]["id"],R1["data"]["meta"]["name"],R1["data"]["meta"]["description"],R1["data"]["meta"]["date"],str(R1["data"]["meta"]["rating"])))
cur.execute("insert into stations_infos values (?, ?, ?, ?, ?)", (R2["data"]["id"],R2["data"]["meta"]["name"],R2["data"]["meta"]["description"],R2["data"]["meta"]["date"],str(R2["data"]["meta"]["rating"])))
cur.execute("insert into stations_infos values (?, ?, ?, ?, ?)", (R3["data"]["id"],R3["data"]["meta"]["name"],R3["data"]["meta"]["description"],R3["data"]["meta"]["date"],str(R3["data"]["meta"]["rating"])))

con.commit()

cur.execute("CREATE TABLE IF NOT EXISTS station_1_measurements (Numero_enregistrement INTEGER PRIMARY KEY AUTOINCREMENT,id,date_mesure,pressure,wind_heading,wind_speed_avg,wind_speed_max,wind_speed_min,FOREIGN KEY (id) REFERENCES position_stations(id))")
cur.execute("INSERT INTO station_1_measurements(id,date_mesure,pressure,wind_heading,wind_speed_avg,wind_speed_max,wind_speed_min) values (?, ?, ?, ?, ?, ?, ?)", (R1["data"]["id"],R1["data"]["measurements"]["date"],R1["data"]["measurements"]["pressure"],R1["data"]["measurements"]["wind_heading"],R1["data"]["measurements"]["wind_speed_avg"],R1["data"]["measurements"]["wind_speed_max"],R1["data"]["measurements"]["wind_speed_min"]))

con.commit()

cur.execute("CREATE TABLE IF NOT EXISTS station_2_measurements (Numero_enregistrement INTEGER PRIMARY KEY AUTOINCREMENT,id,date_mesure,pressure,wind_heading,wind_speed_avg,wind_speed_max,wind_speed_min,FOREIGN KEY (id) REFERENCES position_stations(id))")
cur.execute("INSERT INTO station_2_measurements(id,date_mesure,pressure,wind_heading,wind_speed_avg,wind_speed_max,wind_speed_min) values (?, ?, ?, ?, ?, ?, ?)", (R2["data"]["id"],R2["data"]["measurements"]["date"],R2["data"]["measurements"]["pressure"],R2["data"]["measurements"]["wind_heading"],R2["data"]["measurements"]["wind_speed_avg"],R2["data"]["measurements"]["wind_speed_max"],R2["data"]["measurements"]["wind_speed_min"]))

con.commit()

cur.execute("CREATE TABLE IF NOT EXISTS station_3_measurements (Numero_enregistrement INTEGER PRIMARY KEY AUTOINCREMENT,id,date_mesure,pressure,wind_heading,wind_speed_avg,wind_speed_max,wind_speed_min,FOREIGN KEY (id) REFERENCES position_stations(id))")
cur.execute("INSERT INTO station_3_measurements(id,date_mesure,pressure,wind_heading,wind_speed_avg,wind_speed_max,wind_speed_min) values (?, ?, ?, ?, ?, ?, ?)", (R3["data"]["id"],R3["data"]["measurements"]["date"],R3["data"]["measurements"]["pressure"],R3["data"]["measurements"]["wind_heading"],R3["data"]["measurements"]["wind_speed_avg"],R3["data"]["measurements"]["wind_speed_max"],R3["data"]["measurements"]["wind_speed_min"]))

con.commit()

con.close()