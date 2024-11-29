from services.import_data import ImportData
import os, sys

path = os.path.dirname(os.path.abspath(__file__))  # NOQA
sys.path.append(path)  # NOQA


# To execute
ImportData().bulk_import("data/company")
