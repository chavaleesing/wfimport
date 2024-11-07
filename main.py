from dotenv import load_dotenv
from services.import_data import ImportData
import os, sys

path = os.path.dirname(os.path.abspath(__file__))  # NOQA
sys.path.append(path)  # NOQA

load_dotenv()

# To execute
ImportData().bulk_import('data/credit')
