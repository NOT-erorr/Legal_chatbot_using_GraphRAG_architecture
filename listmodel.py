from google import genai
from dotenv import load_dotenv
import os
load_dotenv()
# 1. Khởi tạo client với API key
client = genai.Client(api_key='AIzaSyAzaC2VaC5e-ZbhZ7p0YcY3GmUxc0WLCgY')

# 2. Gọi API list models
models = client.models.list()

# 3. In danh sách model
for m in models:
    print("Name:", m.name)
    print("Display Name:", getattr(m, "display_name", None))
    print("Supported methods:", getattr(m, "supported_generation_methods", None))
    print("-" * 40)