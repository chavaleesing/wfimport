from dotenv import load_dotenv
from services.import_data import ImportData
# from services.load_data import LoadData
import os, sys

path = os.path.dirname(os.path.abspath(__file__))  # NOQA
sys.path.append(path)  # NOQA

load_dotenv(override=True, dotenv_path='.env')

# To execute
# LoadData().bulk_load('data/credit')
# ImportData().bulk_import("data/credit")

ImportData().bulk_import("data/credit")
