import yfinance as yf
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# -----------------------------
# Stocks list
# -----------------------------
tickers = tickers = [
    # Nifty 100 Constituents
    "TCS.NS", "INFY.NS", "RELIANCE.NS", "HDFCBANK.NS", "ICICIBANK.NS", "HINDUNILVR.NS", "KOTAKBANK.NS", "SBIN.NS", "HCLTECH.NS", "LT.NS",
    "BHARTIARTL.NS", "ASIANPAINT.NS", "ITC.NS", "AXISBANK.NS", "MARUTI.NS", "TITAN.NS", "ONGC.NS", "NTPC.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS",
    "ULTRACEMCO.NS", "SUNPHARMA.NS", "WIPRO.NS", "MM.NS", "TATASTEEL.NS", "INDUSINDBK.NS", "POWERGRID.NS", "JSWSTEEL.NS", "HINDALCO.NS", "COALINDIA.NS",
    "GRASIM.NS", "SBILIFE.NS", "CIPLA.NS", "TATACONSUM.NS", "EICHERMOT.NS", "DRREDDY.NS", "DIVISLAB.NS", "BPCL.NS", "ADANIPORTS.NS", "AMBUJACEM.NS",
    "HAVELLS.NS", "TECHM.NS", "GODREJCP.NS", "VEDL.NS", "ADANIENT.NS", "TORNTPHARM.NS", "INDIGO.NS", "MOTHERSON.NS", "PIDILITIND.NS", "JINDALSTEL.NS",
    "JSWENERGY.NS", "ABB.NS", "SIEMENS.NS", "CANBK.NS", "BANKBARODA.NS", "GAIL.NS", "NTPC.NS", "TATAPOWER.NS", "ADANIGREEN.NS", "INDHOTEL.NS",
    "IRCTC.NS", "LICI.NS", "ICICIPRULI.NS", "JIOFINANCIAL.NS", "ADANITRANS.NS", "ADANIPOWER.NS", "APOLLOHOSP.NS", "LUPIN.NS", "MAXHEALTH.NS", "BAJAJAUTO.NS",
    "HINDCOPPER.NS", "BERGEPAINT.NS", "AUROPHARMA.NS", "ONGC.NS", "SHRIRAMFIN.NS", "MCDOWELL-ND.NS", "COLPAL.NS", "NMDC.NS", "SBICARD.NS", "LBIEL.NS",
    "ASHOKLEY.NS", "ACC.NS", "BOSCHLTD.NS", "GMRINFRA.NS", "ASHIANPAINT.NS", "HDFCLIFE.NS", "INDUSTOWER.NS", "TATACHEM.NS", "JPASSOCIAT.NS", "GLAND.NS",
    "IOC.NS", "NESTLEIND.NS", "HDFC.NS", "PNB.NS", "RECLTD.NS", "HCLTECH.NS", "ADANIGREEN.NS", "ADANIPORTS.NS",
    
    # Nifty Next 50 Constituents (Additional Stocks)
    "ABBOTINDIA.NS", "ACCELYA.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "AIAENG.NS", "AJANTPHARM.NS", "AKZOINDIA.NS", "ALOKINDS.NS", "AMBER.NS", "AMRUTANJAN.NS",
    "ANDHRABANK.NS", "APOLLOHOSP.NS", "APOLLOPIPE.NS", "ARVIND.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "ASTRAL.NS", "ATUL.NS", "AUROPHARMA.NS", "AXISBANK.NS",
    "BAJAJCON.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS", "BANKBARODA.NS", "BATAINDIA.NS", "BERGEPAINT.NS", "BHARATFORG.NS", "BHARTIARTL.NS", "BIOCON.NS", "BOSCHLTD.NS",
    "CADILAHC.NS", "CANBK.NS", "CIPLA.NS", "COALINDIA.NS", "COLPAL.NS", "CONCOR.NS", "COROMANDEL.NS", "CUMMINSIND.NS", "DABUR.NS", "DIVISLAB.NS",
    "DLF.NS", "DRREDDY.NS", "EICHERMOT.NS", "ESCORTS.NS", "EXIDEIND.NS", "FEDERALBNK.NS", "GAIL.NS", "GLENMARK.NS", "GODREJCP.NS", "GODREJPROP.NS",
    "GRASIM.NS", "GSKCONS.NS", "HAVELLS.NS", "HCLTECH.NS", "HDFC.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HEROMOTOCO.NS", "HINDALCO.NS", "HINDPETRO.NS",
    "HINDUNILVR.NS", "ICICIBANK.NS", "ICICIPRULI.NS", "IDFCFIRSTB.NS", "INDHOTEL.NS", "INDIGO.NS", "INDUSINDBK.NS", "INFY.NS", "IOC.NS", "ITC.NS",
    "JINDALSTEL.NS", "JSWSTEEL.NS", "KOTAKBANK.NS", "LALPATHLAB.NS", "LICI.NS", "LT.NS", "LUPIN.NS", "M&M.NS", "MARUTI.NS", "MCDOWELL-ND.NS",
    "MOTHERSON.NS", "MPHASIS.NS", "NESTLEIND.NS", "NTPC.NS", "OIL.NS", "ONGC.NS", "PIDILITIND.NS", "PNB.NS", "POWERGRID.NS", "RECLTD.NS",
    "RELIANCE.NS", "SBILIFE.NS", "SBIN.NS", "SHRIRAMFIN.NS", "SIEMENS.NS", "SRF.NS", "SUNPHARMA.NS", "TATACONSUM.NS", "TATACHEM.NS", "TATAPOWER.NS",
    "TCS.NS", "TECHM.NS", "TORNTPHARM.NS", "ULTRACEMCO.NS", "UPL.NS", "VEDL.NS", "WIPRO.NS", "YESBANK.NS", "ZEELEARN.NS"
]


# -----------------------------
# Google Sheets auth
# -----------------------------
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)

# Connect to your Sheet
sheet = client.open_by_key("1TNfsLgqOcSU2l05X0fJmTy4a4MVsSK-bHk19jW7dQyA").sheet1
sheet.clear()
sheet.append_row(["Ticker", "Date", "Open", "High", "Low", "Close", "Prev Close", "% Change", "Volume", "Time"])

# -----------------------------
# Prepare all rows first
# -----------------------------
all_rows = []

# Historical data (last 30 days)
for ticker in tickers:
    hist = yf.download(ticker, period="30d", interval="1d", auto_adjust=False)
    if hist.empty:
        print(f"⚠️ No data for {ticker}")
        continue

    prev_close = None
    for idx, row in hist.iterrows():
        open_price = float(row["Open"])
        high_price = float(row["High"])
        low_price = float(row["Low"])
        close_price = float(row["Close"])
        volume = int(row["Volume"])
        date_str = idx.strftime("%Y-%m-%d")
        pct_change = ((close_price - prev_close) / prev_close * 100) if prev_close else 0

        all_rows.append([
            ticker, date_str, open_price, high_price, low_price, close_price,
            prev_close if prev_close else close_price,
            round(pct_change, 2), volume,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ])
        prev_close = close_price
    print(f"✅ Historical data ready for {ticker}")

# Latest intraday data (last 5 min)
for ticker in tickers:
    df = yf.download(ticker, period="1d", interval="5m", auto_adjust=False).tail(1)
    if not df.empty:
        latest = df.iloc[0]
        all_rows.append([
            ticker,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            float(latest["Open"]),
            float(latest["High"]),
            float(latest["Low"]),
            float(latest["Close"]),
            float(latest["Close"]),  # prev_close = latest close
            0,
            int(latest["Volume"]),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ])
        print(f"✅ Latest intraday ready for {ticker}")

# -----------------------------
# Batch append all rows at once
# -----------------------------
if all_rows:
    sheet.append_rows(all_rows)
    print(f"✅ All data pushed to Google Sheets ({len(all_rows)} rows)")
