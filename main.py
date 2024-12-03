from services.import_data import ImportData
import os, sys

path = os.path.dirname(os.path.abspath(__file__))  # NOQA
sys.path.append(path)  # NOQA

if len(sys.argv) == 2:
    # To execute
    ImportData().bulk_import(sys.argv[1])
else:
    print("Pls run script in format `python3 main.py data/company`")