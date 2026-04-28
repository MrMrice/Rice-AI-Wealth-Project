import os
import json
from datetime import date
import pandas as pd
import yfinance as yf
from fastapi import FastAPI, Request, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import io
import anthropic

app = FastAPI()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

PORTFOLIO_PATH = os.path.join(os.path.dirname(__file__), "..", "simulated_portfolio.csv")

# Target sector allocations (%) — wealth manager sets these
SECTOR_TARGETS = {
    "Technology": 30,
    "Healthcare": 15,
    "Financials": 15,
    "Consumer Discretionary": 10,
    "Industrials": 10,
    "Energy": 8,
    "Utilities": 7,
    "Materials": 5,
}


# Map common sector name variants to standard names
SECTOR_MAP = {
    "financial services": "Financials",
    "financials": "Financials",
    "basic materials": "Materials",
    "materials": "Materials",
    "communication services": "Communication Services",
    "technology": "Technology",
    "healthcare": "Healthcare",
    "health care": "Healthcare",
    "consumer discretionary": "Consumer Discretionary",
    "consumer cyclical": "Consumer Discretionary",
    "industrials": "Industrials",
    "energy": "Energy",
    "utilities": "Utilities",
    "real estate": "Real Estate",
}

def normalize_sector(s):
    return SECTOR_MAP.get(str(s).strip().lower(), str(s).strip())

def _looks_like_tickers(series: pd.Series) -> bool:
    """Return True if the majority of values in this column look like stock ticker symbols."""
    vals = series.dropna().astype(str).str.strip()
    # Exclude header-like noise values
    vals = vals[~vals.str.lower().isin(["symbol", "ticker", "nan", "", "--", "n/a"])]
    if len(vals) < 2:
        return False
    # Tickers: 1-7 chars, letters/numbers/dots/hyphens, NO spaces, NO long strings
    matches = vals.str.match(r'^[A-Za-z][A-Za-z0-9.\-]{0,6}\*{0,2}$')
    return matches.mean() > 0.5


def detect_and_normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Handle both the simulated CSV format and the exercise Excel format."""
    # Aggressively clean all column names
    df.columns = [str(c).strip().lower().replace("\xa0", " ") for c in df.columns]
    print("DEBUG columns:", df.columns.tolist())

    # Already in expected format (simulated CSV)
    if "ticker" in df.columns and "lot_id" in df.columns:
        df["sector"] = df["sector"].apply(normalize_sector)
        return df[df["ticker"] != "CASH"]

    # ── Map all columns to standard names (Fidelity + generic formats) ─────────
    col_map = {
        # Fidelity standard export columns
        "symbol":                   "ticker",
        "description":              "company_name",
        "quantity":                 "shares",
        "last price":               "current_price",
        "current value":            "current_value",
        "total gain/loss dollar":   "unrealized_gain_loss",
        "average cost basis":       "cost_per_share",
        "cost basis total":         "total_cost",
        # Generic / other broker columns
        "price per share":          "cost_per_share",
        "date":                     "purchase_date",
        "cost":                     "total_cost",
    }
    df = df.rename(columns=col_map)
    print(f"DEBUG columns after rename: {df.columns.tolist()}")
    if "ticker" in df.columns:
        print(f"DEBUG ticker sample: {df['ticker'].dropna().tolist()[:8]}")

    def clean_numeric(val):
        """Handle $1,234.56, ($1,234.56), and plain number Fidelity formats."""
        if pd.isna(val):
            return None
        s = str(val).strip()
        if s in ("", "--", "n/a", "N/A", "nan"):
            return None
        negative = s.startswith("(") and s.endswith(")")
        s = s.replace("(", "").replace(")", "").replace("$", "").replace(",", "").replace("%", "").strip()
        try:
            result = float(s)
            return -abs(result) if negative else result
        except Exception:
            return None

    for col in ["shares", "current_price", "current_value", "unrealized_gain_loss", "cost_per_share", "total_cost"]:
        if col in df.columns:
            df[col] = df[col].apply(clean_numeric)
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop bad rows — cash, money market, blank tickers
    print(f"DEBUG pre-filter rows: {len(df)}")
    if "ticker" in df.columns:
        print(f"DEBUG raw ticker values: {df['ticker'].tolist()[:10]}")
    else:
        print("DEBUG ERROR: no 'ticker' column found!")

    df = df.dropna(subset=["ticker"])
    print(f"DEBUG after dropna(ticker): {len(df)} rows")

    df["ticker"] = df["ticker"].astype(str).str.strip()
    bad = ["ticker", "symbol", "nan", "pending activity", "", "--", "n/a"]
    df = df[~df["ticker"].str.lower().isin(bad)]
    print(f"DEBUG after bad-list filter: {len(df)} rows")

    df = df[~df["ticker"].str.contains(" ", na=False)]
    print(f"DEBUG after space filter: {len(df)} rows")

    df = df[df["ticker"].str.len() <= 12]
    print(f"DEBUG after len<=12 filter: {len(df)} rows")

    if "shares" in df.columns:
        print(f"DEBUG shares sample (before dropna): {df['shares'].tolist()[:5]}")
    df = df.dropna(subset=["shares"])
    print(f"DEBUG after dropna(shares): {len(df)} rows")

    print(f"DEBUG FINAL: {len(df)} rows, columns: {df.columns.tolist()}")
    print(f"DEBUG sample tickers: {df['ticker'].tolist()[:10]}")
    if "current_value" in df.columns:
        print(f"DEBUG current_value sample: {df['current_value'].tolist()[:5]}")

    # Use only valid tickers for yfinance lookups
    tickers = df["ticker"].unique().tolist()

    # Fill missing current price
    if "current_price" not in df.columns or df["current_price"].isna().all():
        prices = {}
        for t in tickers:
            try:
                prices[t] = round(float(yf.Ticker(t).fast_info.last_price), 2)
            except Exception:
                prices[t] = df.loc[df["ticker"] == t, "cost_per_share"].iloc[0]
        df["current_price"] = df["ticker"].map(prices)

    if "current_value" not in df.columns or df["current_value"].isna().all():
        df["current_value"] = (df["shares"] * df["current_price"]).round(2)

    if "unrealized_gain_loss" not in df.columns or df["unrealized_gain_loss"].isna().all():
        df["unrealized_gain_loss"] = (df["current_value"] - df["total_cost"]).round(2)

    # Fetch sector from yfinance for valid tickers only
    if "sector" not in df.columns:
        sectors = {}
        for t in tickers:
            try:
                sectors[t] = yf.Ticker(t).info.get("sector", "Unknown")
            except Exception:
                sectors[t] = "Unknown"
        df["sector"] = df["ticker"].map(sectors)

    df["sector"] = df["sector"].apply(normalize_sector)

    if "company_name" not in df.columns:
        df["company_name"] = df["ticker"]

    # Generate lot_id and holding period
    df = df.reset_index(drop=True)
    df["lot_id"] = df["ticker"] + "-" + (df.groupby("ticker").cumcount() + 1).astype(str)
    today = date.today()
    if "purchase_date" in df.columns:
        def holding(d):
            try:
                purchase = pd.to_datetime(d).date()
                days = (today - purchase).days
                return "long_term" if days >= 365 else "short_term"
            except Exception:
                return "long_term"
        df["holding_period"] = df["purchase_date"].apply(holding)
    else:
        df["holding_period"] = "unknown"

    return df


def load_portfolio(contents: bytes, filename: str):
    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        # Fidelity Excel files have account info before the real column headers.
        # Try each row as a potential header until we find one containing "Symbol" or "Ticker".
        for header_row in range(15):
            df = pd.read_excel(io.BytesIO(contents), header=header_row)
            # Lowercase all column names to compare
            cols_lower = [str(c).strip().lower().replace("\xa0", " ") for c in df.columns]
            if "symbol" in cols_lower or "ticker" in cols_lower:
                # Get the ACTUAL column name (original case) for the symbol column
                sym_key = "symbol" if "symbol" in cols_lower else "ticker"
                sym_col = df.columns[cols_lower.index(sym_key)]
                # Drop blank/footer rows using the Symbol column by name (not by index)
                df = df[df[sym_col].notna()]
                df = df[~df[sym_col].astype(str).str.strip().str.lower().str.contains(
                    r"account total|total|footnote|pending activity|^nan$",
                    regex=True, na=False
                )]
                print(f"DEBUG Excel: header row={header_row}, sym_col='{sym_col}', rows after filter={len(df)}")
                print(f"DEBUG Symbol column sample: {df[sym_col].tolist()[:8]}")
                if len(df) > 0:
                    print(f"DEBUG first row: {df.iloc[0].to_dict()}")
                return detect_and_normalize(df)
        raise ValueError("Could not find a Symbol or Ticker column in the Excel file")
    else:
        df = pd.read_csv(io.BytesIO(contents))
        return detect_and_normalize(df)


def get_analyst_ratings(tickers: list[str]) -> dict:
    ratings = {}
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            rec = stock.recommendations_summary
            if rec is not None and not rec.empty:
                latest = rec.iloc[0]
                ratings[ticker] = {
                    "strongBuy": int(latest.get("strongBuy", 0)),
                    "buy": int(latest.get("buy", 0)),
                    "hold": int(latest.get("hold", 0)),
                    "sell": int(latest.get("sell", 0)),
                    "strongSell": int(latest.get("strongSell", 0)),
                }
            else:
                ratings[ticker] = None
        except Exception:
            ratings[ticker] = None
    return ratings


def calc_sector_weights(df: pd.DataFrame) -> dict:
    total = df["current_value"].sum()
    weights = (
        df.groupby("sector")["current_value"].sum() / total * 100
    ).round(1).to_dict()
    return weights


def build_agent_prompt(df: pd.DataFrame, ratings: dict, sector_weights: dict) -> str:
    portfolio_summary = df[[
        "ticker", "company_name", "sector", "lot_id",
        "shares", "holding_period", "cost_per_share",
        "current_price", "current_value", "unrealized_gain_loss"
    ]].to_string(index=False)

    sector_comparison = "\n".join([
        f"  {sector}: current {sector_weights.get(sector, 0):.1f}% vs target {target}%"
        for sector, target in SECTOR_TARGETS.items()
    ])

    ratings_text = json.dumps(ratings, indent=2)

    return f"""You are a wealth management AI agent. Produce a concise, professional trading plan report. Keep it tight — no lengthy paragraphs. Every section should be a table or 1-2 sentences max. Do not cut off.

## Portfolio Data
{portfolio_summary}

## Sector Weights: Current vs Target
{sector_comparison}

## Analyst Recommendations
{ratings_text}

## Instructions
Balance: (1) analyst signals, (2) sector rebalancing, (3) tax efficiency — prefer ST losses first, then LT losses, then LT gains. Avoid ST gains entirely.

Use EXACTLY this format:

---

## Portfolio Snapshot
One line: total value, number of positions, total unrealized gain/loss.

---

## Analyst Ratings
Compact table sorted by score descending. Score = (strongBuy×2 + buy - sell - strongSell×2).
Columns: Ticker | Company | Score | Signal (🟢 Strong Buy / 🟢 Buy / 🟡 Hold / 🔴 Trim)
No other text.

---

## Recommended Action Plan

### Sells
One row per lot. Columns: # | Ticker | Lot | Shares | Proceeds | Gain/Loss | Tax Type | Reason (5 words max)

### Buys
One row per position. Columns: # | Ticker | Shares | Est. Cost | Reason (5 words max)

---

## Post-Trade Summary

### Sector Weights After Trades
Columns: Sector | Before | After | Target | Status (✅ On Target / 🟡 Near / 🔴 Off)

### Tax Impact
Columns: Category | Amount
Rows: ST Losses Harvested | LT Gains Triggered | Net Tax Benefit

One closing sentence on overall tax efficiency.

---

Complete all sections. No extra commentary. Tables only."""


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    try:
        return templates.TemplateResponse(request, "index.html")
    except Exception as e:
        return HTMLResponse(f"<pre>ERROR: {e}</pre>", status_code=500)


@app.post("/generate", response_class=HTMLResponse)
async def generate_report(request: Request, portfolio: UploadFile = File(...)):
    contents = await portfolio.read()
    df = load_portfolio(contents, portfolio.filename)
    tickers = df["ticker"].unique().tolist()

    ratings = get_analyst_ratings(tickers)
    sector_weights = calc_sector_weights(df)
    prompt = build_agent_prompt(df, ratings, sector_weights)

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=6000,
        messages=[{"role": "user", "content": prompt}],
    )
    report_text = message.content[0].text

    return templates.TemplateResponse(request, "report.html", {
        "report": report_text,
        "sector_weights": sector_weights,
        "sector_targets": SECTOR_TARGETS,
    })
