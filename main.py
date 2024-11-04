from dotenv import load_dotenv
from services.import_data import bulk_import


load_dotenv()
bulk_import('data/customer')
