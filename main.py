import requests
from datetime import datetime

def fetch_caviar_news():
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": "caviar",
        "sortBy": "publishedAt",
        "language": "en",
        "apiKey": "YOUR_NEWSAPI_KEY"
    }
    response = requests.get(url, params=params)
    return response.json()

if __name__ == "__main__":
    data = fetch_caviar_news()
    print(f"Fetched {len(data.get('articles', []))} articles about caviar on {datetime.now().date()}")
