# 🐟 Caviar Monitor

**Caviar Monitor** is a simple Python service that collects news, updates, and research about caviar around the world.  
It can summarize new articles and send you a **daily email digest**.

---

### 🚀 How It Works
1. Fetches caviar-related news using the [NewsAPI](https://newsapi.org/).  
2. Filters and ranks articles by relevance and importance.  
3. Summarizes and formats them into a clean HTML digest.  
4. Emails the digest automatically once per day via SendGrid.

---

### 🧰 Main Files
| File | Purpose |
|------|----------|
| `main.py` | Main runner script |
| `requirements.txt` | Python dependencies |
| `.env.example` | Environment-variable template |
| `templates/digest_template.html` | Email HTML template |

---

### ⚙️ Setup Overview
1. Get a NewsAPI key and a SendGrid API key.  
2. Copy `.env.example` → `.env` and fill in your keys.  
3. Deploy to Render or any cloud that supports daily cron jobs.  
4. Enjoy your daily **Caviar Digest** in your inbox!

---

© 2025 Caviar Monitor Project

