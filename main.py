from services.import_data import ImportData
from services.update_data import UpdateData
import os, sys

path = os.path.dirname(os.path.abspath(__file__))  # NOQA
sys.path.append(path)  # NOQA


if len(sys.argv) == 2:
    if sys.argv[1].startswith("data/"):
        ImportData().bulk_import(sys.argv[1])
    elif sys.argv[1] == "update":
        UpdateData().update_data_to_mysql()
    else:
        print("Pls run script in format `python3 main.py data/<folder_name>` OR `python3 main.py update`")
else:
    print("Pls run script in format `python3 main.py data/<folder_name>` OR `python3 main.py update`")
