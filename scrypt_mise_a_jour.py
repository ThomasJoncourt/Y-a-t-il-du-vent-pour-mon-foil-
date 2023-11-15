import requests
import json
import pandas as pd
import sqlite3
import math
import io


con = sqlite3.connect("donnees_pioupiou.sq3")
cur = con.cursor()

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

cur.execute("INSERT INTO station_1_measurements(id,date_mesure,pressure,wind_heading,wind_speed_avg,wind_speed_max,wind_speed_min) values (?, ?, ?, ?, ?, ?, ?)", (R1["data"]["id"],R1["data"]["measurements"]["date"],R1["data"]["measurements"]["pressure"],R1["data"]["measurements"]["wind_heading"],R1["data"]["measurements"]["wind_speed_avg"],R1["data"]["measurements"]["wind_speed_max"],R1["data"]["measurements"]["wind_speed_min"]))

con.commit()

cur.execute("INSERT INTO station_2_measurements(id,date_mesure,pressure,wind_heading,wind_speed_avg,wind_speed_max,wind_speed_min) values (?, ?, ?, ?, ?, ?, ?)", (R2["data"]["id"],R2["data"]["measurements"]["date"],R2["data"]["measurements"]["pressure"],R2["data"]["measurements"]["wind_heading"],R2["data"]["measurements"]["wind_speed_avg"],R2["data"]["measurements"]["wind_speed_max"],R2["data"]["measurements"]["wind_speed_min"]))

con.commit()

cur.execute("INSERT INTO station_3_measurements(id,date_mesure,pressure,wind_heading,wind_speed_avg,wind_speed_max,wind_speed_min) values (?, ?, ?, ?, ?, ?, ?)", (R3["data"]["id"],R3["data"]["measurements"]["date"],R3["data"]["measurements"]["pressure"],R3["data"]["measurements"]["wind_heading"],R3["data"]["measurements"]["wind_speed_avg"],R3["data"]["measurements"]["wind_speed_max"],R3["data"]["measurements"]["wind_speed_min"]))

con.commit()

cur.execute("select * from station_1_measurements")
n = len(cur.fetchall())

while n>10:
    cur.execute("DELETE FROM station_1_measurements WHERE Numero_enregistrement = (select MIN(Numero_enregistrement) from station_1_measurements)")
    con.commit()
    cur.execute("select * from station_1_measurements")
    n = len(cur.fetchall())
    
cur.execute("select * from station_2_measurements")
n = len(cur.fetchall())

while n>10:
    cur.execute("DELETE FROM station_2_measurements WHERE Numero_enregistrement = (select MIN(Numero_enregistrement) from station_2_measurements)")
    con.commit()
    cur.execute("select * from station_2_measurements")
    n = len(cur.fetchall())
    
cur.execute("select * from station_3_measurements")
n = len(cur.fetchall())

while n>10:
    cur.execute("DELETE FROM station_3_measurements WHERE Numero_enregistrement = (select MIN(Numero_enregistrement) from station_3_measurements)")
    con.commit()
    cur.execute("select * from station_3_measurements")
    n = len(cur.fetchall())

cur.execute("select MAX(Numero_enregistrement) from station_1_measurements")
dernier_numero = cur.fetchall()[0][0]
if dernier_numero%5 == 0:
    with io.open('backupdatabase.sql', 'w') as p:
        for line in con.iterdump(): 
            p.write('%s\n' % line)
            con.commit()
con.commit()
            

con.close()