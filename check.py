import os
from dotenv import load_dotenv

load_dotenv()

print("POSTGRES_USER:", os.getenv("POSTGRES_USER"))
print("POSTGRES_DB:", os.getenv("POSTGRES_DB"))
