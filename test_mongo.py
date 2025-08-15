from pymongo import MongoClient

uri = "mongodb+srv://skryd1x:lt39p63gohPH2KuD@cluster0.arzsaof.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

try:
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    print("✅ Подключение успешно! MongoDB Atlas работает.")
except Exception as e:
    print("❌ Ошибка подключения:", e)
