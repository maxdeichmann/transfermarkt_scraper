import requests
import pandas as pd
from bs4 import BeautifulSoup


def scrape_bundesliga_players():

    col_names =  ['Club', 'Name']
    players_df  = pd.DataFrame(columns = col_names)

    table_page = requests.get("http://www.transfermarkt.de/1-bundesliga/tabelle/wettbewerb/L1/saison_id/2019", headers={'User-Agent': 'Mozilla/5.0'})
    table_soup = BeautifulSoup(table_page.content, 'html.parser')
    table_container = table_soup.find('div', class_='responsive-table')
    table_rows = table_container.find_all("tr")

    for i, row in enumerate(table_rows):
        if i != 0:
            a = row.find("a", class_="vereinprofil_tooltip")
            
            link = a["href"]
            link_parts = link.split("/")

            club_name = link_parts[1]

            new_link = "http://www.transfermarkt.de/"+link_parts[1]+"/kader/verein/"+link_parts[4]+"/saison_id/2019"
            club_request = requests.get(new_link, headers={'User-Agent': 'Mozilla/5.0'})
            squad_soup = BeautifulSoup(club_request.content, 'html.parser')
            players_table = squad_soup.find("table", class_="items")
            players_rows = players_table.find_all("tr")

            for player in players_rows:
                    
                main_link = player.find("a", class_="spielprofil_tooltip")
                if main_link is not None:
                    player_name = main_link.get_text()
                
                    players_df.loc[len(players_df)] = [club_name, player_name]
    
    return players_df.drop_duplicates(subset="Name", keep='first', inplace=False)


if __name__ == "__main__":
    print(scrape_bundesliga_players().to_string())
    
