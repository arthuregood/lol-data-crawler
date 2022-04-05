import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


def checkLeague(league, year, season, limit):
  try:
    url = f'https://lol.fandom.com/wiki/Special:RunQuery/MatchHistoryGame?pfRunQueryFormName=MatchHistoryGame&MHG%5Bpreload%5D=Tournament&MHG%5Btournament%5D={league}%2F{year}+Season%2F{season}&MHG%5Blimit%5D={limit}&MHG%5Btextonly%5D%5Bis_checkbox%5D=true&MHG%5Btextonly%5D%5Bvalue%5D=1&wpRunQuery=Run+query&pf_free_text='
    html = urlopen(url)
    bs = BeautifulSoup(html.read(), 'html.parser')

    soup = bs.find('table', {'class': 'wikitable'})
    rows = soup.findAll('tr')

    data = {}
    for index, row in enumerate(rows[3:]):
        csvRow = []
        for cell in row.findAll('td'):
            csvRow.append(cell.get_text().strip())

        data[index] = csvRow
    if data != {}:
      df = pd.DataFrame(data, titles)
      df = df.drop('Scoreboard')

      data = df.transpose()
      database_name = f"{league}_{year}_{season}".replace("+", "_")
      insertDatabase(database_name, data)

  except AttributeError as e:
    print(e)

def insertDatabase(db_name, data):

  connect_string = f"mysql+pymysql://arthur:root@localhost/lol_db"
  dbEngine = create_engine(connect_string, connect_args={'connect_timeout': 10}, echo=False)

  try:
    if not database_exists(dbEngine.url):
        create_database(dbEngine.url)
        print("created database")

    dbEngine.connect()
    print("connected to database")
  except Exception as e:
    print(f'Engine invalid: {str(e)}')

  #insert into database
  try:
    data.to_sql(con=dbEngine, schema="lol_db", name=db_name, if_exists="append", index=True, index_label="match_id", method="multi")
    print("inserted data - ", db_name)

  except Exception as e:
    print(f'Insert error: {str(e)}')




leagues = ['Champions', 'LCK', 'NA_LCS', 'LCS', 'EU_LCS', 'LEC', 'LPL', 'LMS', 'PCS', 'LST', 'GPL', 'VCS', 'CBLOL', 'LCO', 'LCL', 'LJL', 'LLA', 'TCL']
seasons = ['Grand_Finals', 'Winter', 'Spring', 'Spring+Season', 'Spring+Playoffs', 'Split+1', 'Split+1+Playoffs', 'Summer', 'Summer+Season', 'Summer+Playoffs', 'Split+2', 'Split+2+Playoffs']
titles = ['Date', 'Patch', 'BlueSide', 'RedSide', 'Winner', 'BlueBans', 'RedBans', 'BluePicks', 'RedPicks', 'BlueTeam', 'RedTeam', 'Len', 'BlueGold', 'BlueKills', 'BlueTowers', 'BlueDragons', 'BlueBarons', 'BlueRiftHeralds', 'RedGold', 'RedKills', 'RedTowers', 'RedDragons', 'RedBarons', 'RedRiftHeralds', 'Scoreboard', 'VOD']


for league in leagues:
  for year in range(2011, 2023):
    for season in seasons:
      checkLeague(league, year, season, limit=500)

