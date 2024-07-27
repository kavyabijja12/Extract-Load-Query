import requests
import pandas as pd
from bs4 import BeautifulSoup

class ScrapeData:
    def __init__(self) -> None:
        self. url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'

    def get_data(self):
        df = pd.DataFrame(columns=["Average Rank","Film","Year"])
        count = 0
        html_page = requests.get(self.url).text
        data = BeautifulSoup(html_page, 'html.parser')
        tables = data.find_all('tbody')
        rows = tables[0].find_all('tr')

        for row in rows:
            if count<50:
                col = row.find_all('td')
                if len(col)!=0:
                    data_dict = {"Average Rank": col[0].contents[0],
                                "Film": col[1].contents[0],
                                "Year": col[2].contents[0]}
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df,df1], ignore_index=True)
                    count+=1
            else:
                break

        return df



