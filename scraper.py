import requests
import pandas as pd
import multiprocessing as mp
from bs4 import BeautifulSoup
from functools import reduce

def scrape_clubs_from_table(table_link = "http://www.transfermarkt.de/1-bundesliga/tabelle/wettbewerb/L1/saison_id/2019"):

    col_names =  ['Club', 'Name']
    players_df  = pd.DataFrame(columns = col_names)

    table_page = requests.get(table_link, headers={'User-Agent': 'Mozilla/5.0'})
    table_soup = BeautifulSoup(table_page.content, 'html.parser')
    table_container = table_soup.find('div', class_='responsive-table')
    table_rows = table_container.find_all("tr")

    pool = mp.Pool(mp.cpu_count())
    result_objects = []

    for i, row in enumerate(table_rows):

        if i != 0 and i < len(table_rows) - 1:
            a = row.find("a", class_="vereinprofil_tooltip")
            
            link = a["href"]
            link_parts = link.split("/")

            club_name = link_parts[1]

            club_link = "http://www.transfermarkt.de/"+link_parts[1]+"/kader/verein/"+link_parts[4]+"/saison_id/2019"

            result_objects.append(pool.apply_async(scrape_players_from_clubs, args=(club_link, club_name)))

    pool.close()
    pool.join() 

    res = map(lambda result: result.get(), result_objects)
    players_df = pd.concat(res)

    return players_df

def scrape_players_from_clubs(club_link, club_name):
    col_names =  ['Club', 'Name']
    players_df  = pd.DataFrame(columns = col_names)
    
    print("request at: ", club_link)
    club_request = requests.get(club_link, headers={'User-Agent': 'Mozilla/5.0'})
    squad_soup = BeautifulSoup(club_request.content, 'html.parser')
    players_table = squad_soup.find("table", class_="items")
    players_rows = players_table.find_all("tr", {'class':['even', 'odd']})

    for j, player in enumerate(players_rows):
        if j > 0:
            main_link = player.find("a", {'class':['spielprofil_tooltip', 'tooltipstered']})
            player_name = main_link.get_text()

            players_df.loc[len(players_df)] = [club_name, player_name]
    return players_df


if __name__ == "__main__":

    print(scrape_clubs_from_table().to_string())#.to_csv("players.xlsx", encoding='utf-8', index=False))
    
