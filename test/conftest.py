import json
from os.path import dirname, join
from typing import Dict

import pytest
from dotenv import load_dotenv


@pytest.fixture(autouse=True, scope="package")
def load_env():
    """Load in the environment variables for any live tests"""
    curr_dir = dirname(__file__)
    # traversing up directories using dirname()
    env_path = join(dirname(dirname(curr_dir)), ".env")
    load_dotenv(dotenv_path=env_path)
