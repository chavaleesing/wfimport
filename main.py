from services.import_data import ImportData
from services.update_data import UpdateData
import os, sys

path = os.path.dirname(os.path.abspath(__file__))  # NOQA
sys.path.append(path)  # NOQA


if len(sys.argv) == 3:
    if sys.argv[1] == "import":
        ImportData().bulk_import(sys.argv[2])
    elif sys.argv[1] == "update":
        UpdateData().bulk_update(sys.argv[2])
    else:
        print("Pls run script in format `python3 main.py <import/update> data/<folder_name>`")
else:
    print("Pls run script in format `python3 main.py <import/update> data/<folder_name>`")
