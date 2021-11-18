"""
Downloads all country codes from PVOutput.
Prints and saves a dictionary mapping the country names to
their codes.
"""
import json

import urllib3
from bs4 import BeautifulSoup

COUNTRY_PAGES = "https://pvoutput.org/map.jsp?country="
MAX_COUNTRY_INT = 257


def get_country_name(manager: urllib3.PoolManager, code: int) -> str:

    country_url = f"{COUNTRY_PAGES}{code}"

    response = manager.request("GET", country_url)
    soup = BeautifulSoup(response.data, "html.parser")

    title = soup.title.string

    return title.split("|")[0].strip()


def get_all_countries() -> None:

    output_dict = {}

    manager = urllib3.PoolManager()

    for country_int in range(1, MAX_COUNTRY_INT + 1):
        country_str = get_country_name(manager, country_int)
        output_dict[country_str] = int(country_int)

    print(output_dict)
    str_dict = json.dumps(output_dict)

    with open("country_codes.txt", "w") as f:
        f.write(str_dict)


if __name__ == "__main__":
    get_all_countries()
