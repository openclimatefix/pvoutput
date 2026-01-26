import inspect
import os
import pytest
from bs4 import BeautifulSoup


# --- Helper to find the data directory ---
@pytest.fixture
def data_dir():
    # Taken from http://stackoverflow.com/a/6098238/732596
    data_dir = os.path.dirname(inspect.getfile(inspect.currentframe()))
    data_dir = os.path.abspath(data_dir)
    return data_dir


# --- LOADS THE HTML FILE (Replaces the broken pickle) ---
@pytest.fixture
def map_soup():
    # This points to the file you downloaded via curl
    fpath = os.path.join(os.path.dirname(__file__), "data", "map_search.html")

    if not os.path.exists(fpath):
        pytest.fail(f"Missing test data: {fpath}")

    with open(fpath, "r", encoding="utf-8") as f:
        return BeautifulSoup(f.read(), "html.parser")
