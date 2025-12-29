# 💼 AI-Powered Job Scraper for Finance Roles in India

Automated job scraper that finds and matches finance jobs from official company career sites using AI. Upload your CV once, and get personalized job matches daily!

## ✨ Features

- 🤖 **AI-Powered Matching** - Ollama analyzes your CV and scores each job (0-100)
- 🔄 **Daily Auto-Scraping** - Automatically fetches new jobs at 2 AM every day
- 📄 **One-Time CV Upload** - Store your CV permanently, never upload again
- ⚡ **Instant Results** - Jobs pre-matched when you visit the website
- 🎯 **Match Reasoning** - See why each job is good for you
- 🌐 **Web Dashboard** - Beautiful UI to browse and filter jobs
- 🏢 **Official Career Sites** - Scrapes from company websites (no third-party aggregators)

## 🏦 Supported Companies

- Goldman Sachs
- JPMorgan Chase
- Morgan Stanley
- Barclays
- Citi
- Nomura
- HSBC (limited)

## 🚀 Quick Start

### Prerequisites

```bash
# Python 3.8+
python3 --version

# Ollama (for AI matching)
ollama --version
```

### Installation

```bash
# Clone the repository
git clone https://github.com/Anant1213/job_scrapper.git
cd job_scrapper/job_scrapper

# Install dependencies
pip3 install -r requirements.txt

# Install Playwright browsers
python3 -m playwright install chromium

# Seed companies into database
export USE_LOCAL_DB=true
python3 tools/seed_companies.py

# Run database migration for AI features
python3 tools/migrate_db_for_cv.py
```

### Usage

```bash
# Start the web application
python3 app.py

# Open in browser
open http://localhost:5001
```

## 📖 How It Works

### 1. One-Time Setup (2 minutes)

1. Start the app: `python3 app.py`
2. Open http://localhost:5001
3. Upload your CV (PDF/DOCX/TXT)
4. AI analyzes and stores your profile

### 2. Daily Automation (Runs at 2 AM)

- Scrapes latest jobs from official career sites
- Matches each new job against your stored CV
- Calculates match score (0-100) and reasoning
- Stores results in database

### 3. Browse Matches Anytime

- Open website to see pre-matched jobs
- Jobs sorted by match score
- Filter by company, score, keywords
- Click "Apply Now" on best matches

## 🎯 Job Scoring

Jobs are scored 0-100 based on:

- **Title Match** (25 pts) - Quant, Data Science, ML roles
- **Skills Match** (30 pts) - Python, SQL, AWS, etc.
- **Location Match** (20 pts) - Mumbai, Bengaluru, Hyderabad
- **Experience Match** (10 pts) - Years of experience alignment
- **Recency** (10 pts) - Newer jobs score higher
- **Education** (5 pts) - Degree requirements

## 🛠️ Tech Stack

- **Backend**: Flask, SQLite
- **Scraping**: Playwright, BeautifulSoup
- **AI**: Ollama (llama3:latest)
- **Frontend**: HTML, CSS, JavaScript
- **Scheduler**: Python schedule library

## 📁 Project Structure

```
job_scrapper/
├── app.py                    # Flask web application
├── connectors/              # Website scrapers
│   ├── all_official_sites.py
│   ├── jpmorgan_official.py
│   └── improved_scrapers.py
├── database/                # Database layer
│   └── local_db.py          # SQLite operations
├── tools/                   # Utility scripts
│   ├── cv_parser.py         # PDF/DOCX parsing
│   ├── ollama_client.py     # AI integration
│   ├── scheduler.py         # Daily automation
│   ├── scrape_final.py      # Main scraper
│   └── scoring.py           # Job ranking
├── templates/               # HTML templates
│   ├── index.html
│   └── control_panel.html
├── static/                  # CSS, JS
│   ├── css/style.css
│   └── js/control-panel.js
└── requirements.txt
```

## 🔧 Configuration

### Scraping Schedule

Edit `tools/scheduler.py`:

```python
# Daily at 2 AM (default)
schedule.every().day.at("02:00").do(daily_scrape_and_match)

# Or customize:
schedule.every().day.at("08:00").do(...)  # 8 AM
schedule.every(6).hours.do(...)            # Every 6 hours
```

### Ollama Model

Edit `tools/ollama_client.py`:

```python
DEFAULT_MODEL = "llama3:latest"  # Or use deepseek-r1:8b, etc.
```

## 🧪 Testing

### Test Ollama Integration

```bash
python3 -m tools.ollama_client
```

### Test CV Parser

```bash
python3 tools/cv_parser.py path/to/resume.pdf
```

### Run Scheduler Immediately

```bash
python3 tools/scheduler.py now
```

### Manual Scraping

```bash
python3 tools/scrape_final.py
```

## 📊 Database

Jobs are stored in SQLite (`jobs.db`):

- **companies** - Company info
- **jobs** - Job postings with AI scores
- **user_cv** - Your CV and extracted skills

To view:

```bash
sqlite3 jobs.db
.tables
SELECT * FROM jobs ORDER BY ai_match_score DESC LIMIT 10;
```

## 🐛 Troubleshooting

**Ollama not running**
```bash
ollama serve
# Or: ollama run llama3:latest
```

**No jobs showing**
```bash
# Run manual scrape
python3 tools/scrape_final.py
```

**CV upload failed**
- Check file size (<16MB)
- Only PDF, DOCX, TXT supported
- Verify `uploads/` folder exists

**Scheduler not working**
- Flask must be running 24/7
- Check logs for "✓ Background scheduler started"

## 🚧 Known Limitations

- Some companies (Deutsche Bank, Wells Fargo, BlackRock, UBS) have strong anti-bot protection
- HSBC scraper yields limited results
- AI matching requires Ollama running locally
- Scheduler only works while Flask app is running

## 🔮 Future Enhancements

- [ ] Email notifications for high-score jobs
- [ ] Mobile app (Flutter)
- [ ] More companies (20+ financial institutions)
- [ ] Application tracking system
- [ ] Salary predictions
- [ ] Job market analytics

## 📄 License

MIT

## 👤 Author

Anant

## 🙏 Acknowledgments

- [Ollama](https://ollama.ai/) for local AI
- [Playwright](https://playwright.dev/) for browser automation
- [Flask](https://flask.palletsprojects.com/) for web framework

---

**Made with ❤️ for job seekers in finance**
