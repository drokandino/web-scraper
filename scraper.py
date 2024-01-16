from bs4 import BeautifulSoup
import requests
import time
import pandas as pd
from sqlalchemy import create_engine
import datetime
import re
import math

def getData():

    url = "https://meteo.hr/naslovnica_aktpod.php?tab=aktpod"

    # Get request prema stranici, vraca se cijeli HTML kod kao string
    html = requests.get(url).text  #bez .text se dobije samo html request status

    # Instanca Beautiful Soup objekta
    soup = BeautifulSoup(html, 'html.parser')

    # Trenutni timestamp koji se kasnije pridodaje podacima
    t = time.localtime()
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", t)

    # Pornalazak izvornih imena stupaca i spremanje istih u listu
    imenaStupaca = list()
    imenaStupacaTags = soup.find_all('th')
    imenaStupacaTags = imenaStupacaTags[4:]

    for stupac in imenaStupacaTags:
        imenaStupaca.append(stupac.text)

    imenaStupaca.append("Timestamp")

    # Obrisi duplikate
    imenaStupaca.remove("Stanje vremena")

    # Rename Vjetarsmjer u Vjetar smjer
    imenaStupaca[1] = 'Vjetar smjer'
    imenaStupaca[4] = 'Relativna vlažnost'


    # lista u koju ce se spremati podaci za svaku mjernu postaju
    # elementi ce biti tipa dict
    data = list()

    # Pronadi tablicu
    tablica = soup.find_all('table', class_="fd-c-table1 table--aktualni-podaci sortable")
    tablica = tablica[0]
    tablica = tablica.find('tbody')

    # Svaki table row predstavlja podatke za jednu mjernu postaju
    tablica = tablica.find_all('tr')

    # Uzmi svaki drugi <tr> jer je prazan tag izmedu
    trs = list()
    i = 0
    for tr in tablica:
        if i % 2 != 0:
            trs.append(tr)
        i += 1


    for mjernaPostaja in trs:
        # init prazan dict sa imenaStupaca kao keys
        podaciPostaje = {key: None for key in imenaStupaca}

        # Svi podaci iz redka
        podaci = mjernaPostaja.find_all('td')

        # Mice white space i novi redak
        podaciPostaje['Postaja'] = podaci[0].text.replace(' ', '').replace('\n', '')

        # Makni \n iz zapisa
        podaciPostaje['Vjetar smjer'] = podaci[1].text.replace("\n", "")

        # Ako postoje podaci
        if podaci[2].text != '-':
            podaciPostaje['Vjetar brzina (m/s)'] = podaci[2].text.replace("\n", "")

        podaciPostaje['Temperatura zraka (°C)'] = float(podaci[3].text)

        if podaci[4].text != '-':
            podaciPostaje['Relativna vlažnost'] = int(podaci[4].text)

        podaciPostaje['Tlak zraka (hPa)'] = podaci[5].text
        podaciPostaje['Tendencija tlaka (hPa/3h)'] = podaci[6].text
        podaciPostaje['Stanje vremena'] = podaci[7].text.replace(' ', '').replace('\n', '')
        podaciPostaje['Timestamp'] = current_time
        data.append(podaciPostaje)

    # Create dataframe
    df = pd.DataFrame(data=data,
                      columns=imenaStupaca)

    # Ciscenje podataka
    for index, redak in df.iterrows():
        if redak['Postaja'][-1] == 'A':
            df.at[index, 'Postaja'] = str(redak['Postaja'])[:-1]

        if redak['Tendencija tlaka (hPa/3h)'] == '-':
            df.at[index, 'Tendencija tlaka (hPa/3h)'] = '0'

        if redak['Tlak zraka (hPa)'] == None or redak['Tlak zraka (hPa)'] == '-':
            df.at[index, 'Tlak zraka (hPa)'] = 'NULL'

        elif str(redak['Tlak zraka (hPa)'])[0] == ' ' and str(redak['Tlak zraka (hPa)'])[-1] == '*':
            df.at[index, 'Tlak zraka (hPa)'] = str(redak['Tlak zraka (hPa)'])[1:-1]

        elif str(redak['Tlak zraka (hPa)'])[-1] == '*':
            df.at[index, 'Tlak zraka (hPa)'] = str(redak['Tlak zraka (hPa)'])[:-1]

        elif str(redak['Tlak zraka (hPa)'])[0] == ' ':
            df.at[index, 'Tlak zraka (hPa)'] = str(redak['Tlak zraka (hPa)'])[1:]


        if redak['Vjetar brzina (m/s)'] == None:
            df.at[index, 'Vjetar brzina (m/s)'] = 'NULL'

        elif str(redak['Vjetar brzina (m/s)']) == '-':
            df.at[index, 'Vjetar brzina (m/s)'] = 'NULL'

        if redak['Temperatura zraka (°C)'] == None or math.isnan(float(redak['Temperatura zraka (°C)'])):
            df.at[index, 'Temperatura zraka (°C)'] = 'NULL'

        if redak['Relativna vlažnost'] == None or math.isnan(float(redak['Relativna vlažnost'])):
            df.at[index, "Relativna vlažnost"] = 'NULL'

    return df, imenaStupaca

# Credentials to database connection
hostname = "localhost"
dbname = "DHMZ"
uname = "root"
pwd = "pass"

t = time.localtime()
current_time = time.strftime("%Y-%m-%d %H:%M:%S", t)
print(str(current_time) + ": Getting first batch of data")

df, imenaStupaca = getData()


# Create SQLAlchemy engine to connect to MySQL Database
connection = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                       .format(host=hostname, db=dbname, user=uname, pw=pwd))


t = time.localtime()
current_time = time.strftime("%Y-%m-%d %H:%M:%S", t)
print(str(current_time) + ": Connected to database")


t = time.localtime()
current_time = time.strftime("%Y-%m-%d %H:%M:%S", t)
print(str(current_time) + ": Loading data to database")

# First load
# Convert dataframe to sql table
df.to_sql('podaci_postaja', connection, index=False)

t = time.localtime()
current_time = time.strftime("%Y-%m-%d %H:%M:%S", t)
print(str(current_time) + ": Load to databse finished")

counter = 0
while True:
    time.sleep(10)

    counter += 1

    t = time.localtime()
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", t)
    print(str(current_time) + ": Loading next data increment to database, increment number: " + str(counter))

    df = getData()[0]


    # Write to database
    for index, redak in df.iterrows():

        insertString = "INSERT INTO dhmz.podaci_postaja " \
                       "VALUES ('" + str(redak['Postaja']) + "', '" + str(redak['Vjetar smjer']) + \
                       "', " + str(redak['Vjetar brzina (m/s)']) + "," + str(redak['Temperatura zraka (°C)']) + \
                       ", " + str(redak['Relativna vlažnost']) + ", " + str(redak['Tlak zraka (hPa)']) + ", '" \
                       + str(redak['Tendencija tlaka (hPa/3h)']) + "', '" + str(redak['Stanje vremena']) + "', '" + redak['Timestamp'] + "')"
        res = connection.execute(insertString)
        




