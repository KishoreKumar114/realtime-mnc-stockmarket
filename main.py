import yfinance as yf
import pymysql
import time
from datetime import datetime
import warnings

# ✅ Suppress warnings
warnings.filterwarnings("ignore")

# -------------------- MySQL connection --------------------
db = pymysql.connect(
    host="localhost",
    user="root",
    password="pass123",
    database="stockdb"
)
cursor = db.cursor()

# -------------------- Company tickers --------------------
tickers = {
    "INFY.NS": "Infosys",
    "TCS.NS": "TCS",
    "HCLTECH.NS": "HCL",
    "CAP.PA": "Capgemini",
    "CTSH": "Cognizant",
    "AMZN": "Amazon"
}

# -------------------- Company info (with 'about') --------------------
company_info = {
    "Infosys": {
        "logo_url": "https://logo.clearbit.com/infosys.com",
        "ceo": "Salil Parekh",
        "ceo_img_url": "https://upload.wikimedia.org/wikipedia/commons/f/f1/Salil_Parekh.jpg",
        "headquarters": "Bangalore, India",
        "sector": "IT Services",
        "currency": "INR",
        "disclaimer": "Data for educational use only",
        "latitude": 12.9716,
        "longitude": 77.5946,
        "about": "Infosys is a global leader in technology services and consulting, headquartered in Bangalore, India. They provide business solutions, IT consulting, and outsourcing services worldwide."
    },
    "TCS": {
        "logo_url": "https://logo.clearbit.com/tcs.com",
        "ceo": "K. Krithivasan",
        "ceo_img_url": "https://upload.wikimedia.org/wikipedia/commons/2/28/K_Krithivasan_TCS_CEO.jpg",
        "headquarters": "Mumbai, India",
        "sector": "IT Services",
        "currency": "INR",
        "disclaimer": "Data for educational use only",
        "latitude": 19.0760,
        "longitude": 72.8777,
        "about": "Tata Consultancy Services (TCS) is an IT services and consulting giant based in Mumbai, India. TCS delivers enterprise solutions, consulting, and digital transformation for global clients."
    },
    "HCL": {
        "logo_url": "https://logo.clearbit.com/hcltech.com",
        "ceo": "C. Vijayakumar",
        "ceo_img_url": "https://upload.wikimedia.org/wikipedia/commons/a/aa/C._Vijayakumar_HCL_Technologies_CEO.jpg",
        "headquarters": "Noida, India",
        "sector": "IT Services",
        "currency": "INR",
        "disclaimer": "Data for educational use only",
        "latitude": 28.5355,
        "longitude": 77.3910,
        "about": "HCL Technologies is a leading Indian IT services company headquartered in Noida, India. It offers software development, infrastructure management, and digital transformation services."
    },
    "Capgemini": {
        "logo_url": "https://logo.clearbit.com/capgemini.com",
        "ceo": "Aiman Ezzat",
        "ceo_img_url": "https://upload.wikimedia.org/wikipedia/commons/4/4c/Aiman_Ezzat_CEO_Capgemini.jpg",
        "headquarters": "Paris, France",
        "sector": "IT Consulting",
        "currency": "EUR",
        "disclaimer": "Data for educational use only",
        "latitude": 48.8566,
        "longitude": 2.3522,
        "about": "Capgemini is a French multinational consulting and IT services company, headquartered in Paris. They specialize in IT services, consulting, technology solutions, and digital transformation worldwide."
    },
    "Cognizant": {
        "logo_url": "https://logo.clearbit.com/cognizant.com",
        "ceo": "Ravi Kumar S",
        "ceo_img_url": "https://upload.wikimedia.org/wikipedia/commons/5/50/Ravi_Kumar_S_Cognizant_CEO.jpg",
        "headquarters": "Teaneck, USA",
        "sector": "IT Services",
        "currency": "USD",
        "disclaimer": "Data for educational use only",
        "latitude": 40.8976,
        "longitude": -74.0159,
        "about": "Cognizant is a multinational IT services and consulting company headquartered in Teaneck, New Jersey, USA. They provide digital, technology, consulting, and operations services globally."
    },
    "Amazon": {
        "logo_url": "https://logo.clearbit.com/amazon.com",
        "ceo": "Andy Jassy",
        "ceo_img_url": "https://upload.wikimedia.org/wikipedia/commons/1/1e/Andy_Jassy_2021.jpg",
        "headquarters": "Seattle, USA",
        "sector": "E-commerce & Cloud",
        "currency": "USD",
        "disclaimer": "Data for educational use only",
        "latitude": 47.6062,
        "longitude": -122.3321,
        "about": "Amazon is a US-based multinational technology company focusing on e-commerce, cloud computing, digital streaming, and AI. Headquartered in Seattle, it is one of the world's largest online retailers."
    }
}

# -------------------- Fetch & store --------------------
def fetch_and_store():
    for symbol, name in tickers.items():
        try:
            data = yf.download(symbol, period="1d", interval="1m", progress=False)
            if not data.empty:
                latest = data.tail(1)
                for index, row in latest.iterrows():
                    info = company_info[name]
                    cursor.execute("""
                        INSERT INTO stocks 
                        (company, datetime, open, high, low, close, volume, 
                        logo_url, ceo, ceo_img_url, headquarters, sector, currency, 
                        disclaimer, latitude, longitude, about)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        name,
                        index.to_pydatetime(),
                        float(row["Open"]),
                        float(row["High"]),
                        float(row["Low"]),
                        float(row["Close"]),
                        int(row["Volume"]),
                        info["logo_url"],
                        info["ceo"],
                        info["ceo_img_url"],
                        info["headquarters"],
                        info["sector"],
                        info["currency"],
                        info["disclaimer"],
                        info["latitude"],
                        info["longitude"],
                        info["about"]
                    ))
                    db.commit()
                print(f"[{datetime.now()}] ✅ {name} data inserted!")
            else:
                print(f"[{datetime.now()}] ⚠️ No data for {name}")
        except Exception as e:
            print(f"[{datetime.now()}] ❌ Error fetching {name}: {e}")

# -------------------- MAIN LOOP --------------------
print("🚀 Starting real-time stock fetcher with full company details including 'about'...")
while True:
    fetch_and_store()
    time.sleep(60)  # fetch every 1 minute

