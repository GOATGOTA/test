import codecs
import csv
from geopy.distance import geodesic as GD
import sqlite3

      
connect = sqlite3.connect('test.db')
cursor = connect.cursor()

def init_test_db():

    cursor.execute('''
        CREATE TABLE if not exists complex(
        complex varchar(128),
        bus_stop varchar(128),
        distance integer
        )
    ''')

    connect.commit()

init_test_db()

def add_complex_data(complex, bus_stop, distance):

    cursor.execute('''
        insert into complex(complex, bus_stop, distance)
        values (?, ?, ?)
    ''',[complex, bus_stop, distance])
   
    connect.commit()




database_stops = []
database_complexes = []



path_data_stops = 'D:\S.T.U.F.F\PythonProjects/dataset_bus_stops.csv'

path_data_complexes = 'D:\S.T.U.F.F\PythonProjects/test_complexes.csv'


with codecs.open(path_data_stops, 'r', 'cp1251') as csvfile:
    reader = csv.DictReader(csvfile, delimiter =";")
    for row in reader:
        database_stops.append(row)


with open(path_data_complexes, 'r', encoding='utf-8', newline='', ) as csvfile:
    reader = csv.DictReader(csvfile, delimiter =",")
    for row in reader:
        database_complexes.append(row)

for elem in database_complexes:
    name_complex = elem['Название ЖК']
    coordinates_complex = elem['Координаты центра']
    coordintates = coordinates_complex.split("(")[1].split(")")[0].split(" ")
    lat_and_lon_complex = coordintates[1] + ', ' + coordintates[0]
    for stop in database_stops:
        lat_and_lon_stop = stop['Latitude_WGS84'] + ', ' + stop['Longitude_WGS84']
        if GD(lat_and_lon_complex, lat_and_lon_stop).m <= 1000:
            add_complex_data(name_complex, stop['StationName'], round(GD(lat_and_lon_complex, lat_and_lon_stop).m))


# Загрузка отсортированной бд в общую csv :)


cursor.execute("SELECT * FROM 'complex' order by complex, distance asc;")
with open("db_sorted.csv", "w", encoding='utf-8', newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor.description])
    csv_writer.writerows(cursor)

# Загрузка отсортированной бд в csv каждого ЖК :)

for elem in database_complexes:
    name_complex = elem['Название ЖК']
    cursor.execute('''SELECT * FROM 'complex' where complex = ? order by complex, distance asc''',[name_complex])
    with open(f"{name_complex}.csv", "w", encoding='utf-8', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)

