# Macro Shock Radar

A local macro early-warning dashboard for monitoring public macro and cross-asset data before a shock becomes obvious in headlines.

## Run

```bash
python3 server.py
```

Open:

```text
http://127.0.0.1:8787
```

Demo mode, useful when the network is blocked:

```text
http://127.0.0.1:8787/?demo=1
```

## Deploy From Git

This app needs a small Python server because the browser should not pull every macro source directly. Static GitHub Pages is not enough for the live version.

Easy public deployment:

1. Push this folder to a GitHub repository.
2. Create a new Render Web Service from that repo.
3. Render will pick up `render.yaml`, or you can set:

```text
Build command: leave blank
Start command: HOST=0.0.0.0 python3 server.py
```

The server reads `PORT` from the host automatically, so Render/Railway/Fly-style deployments work. A `Dockerfile` and `Procfile` are included for other hosts.

## What It Watches

The catalog is in `data/catalog.json`. It currently covers 56 public series across:

- Markets: VIX, S&P 500, NASDAQ, Nikkei
- Rates: 3M/2Y/10Y/30Y Treasury yields, 2s10s, 3m10y, real yields, breakevens
- Inflation: CPI, core CPI, PCE, core PCE, PPI, inflation expectations
- Growth: unemployment, payroll momentum, claims, industrial production, retail sales, housing, sentiment, manufacturing confidence
- Credit/funding: HY OAS, corporate OAS, BBB/AAA OAS, NFCI, ANFCI, STLFSI, C&I loan growth
- Liquidity: Fed balance sheet, reserves, TGA, ON RRP, M2, SOFR, Fed funds, IORB
- FX/carry: broad dollar, USDJPY, USDCNY, EURUSD
- Commodities: WTI, Brent, gasoline, natural gas, copper, gold volatility
- Valuation: Shiller CAPE, S&P 500 trailing PE, earnings yield, dividend yield, Russell 2000 trailing PE, Russell 2000 forward PE, Russell 2000 CAPE, Buffett indicator proxy, equity/M2, equity/Fed balance sheet, earnings-yield gaps

Most data comes from FRED public CSV endpoints. S&P 500 valuation multiples come from Multpl public tables. Russell 2000 PE, forward PE, and CAPE come from Siblis Research public tables. The app caches pulls in `.cache/http` for 6 hours by default.

## Scoring

Each metric gets:

- `percentile`: latest value against its own post-1990 history when available
- `robustZ`: median/MAD z-score, less fragile than normal z-score
- `momentumZ`: unusual 1-month move against the series' own history
- `riskScore`: directed score based on whether high, low, or two-sided readings are dangerous
- `anomalyScore`: two-sided historical abnormality
- `alertScore`: final headline ranking used by the table

Severity bands:

- `extreme`: 90+
- `shock`: 76-89
- `elevated`: 62-75
- `watch`: 45-61
- `normal`: below 45

Scenario scores aggregate related risk scores. The current scenario groups are JPY carry unwind, funding stress, inflation shock, growth scare, liquidity drain, cross-asset risk-off, and valuation fragility.

## Derived Ratios

The server supports `provider: "derived"` metrics built from multiple component series. Components can come from FRED or Multpl and are aligned by last known observation as of each base date.

Current derived ratios:

- Buffett indicator proxy: `NCBEILQ027S / GDP`
- Equity market value to M2: `NCBEILQ027S / M2SL`
- Equity market value to Fed assets: `NCBEILQ027S / WALCL`
- Fed model gap: S&P earnings yield minus 10-year Treasury yield
- Real earnings yield gap: S&P earnings yield minus 10-year TIPS real yield

## API

```text
GET /api/monitor
GET /api/monitor?refresh=1
GET /api/monitor?demo=1
GET /api/series?id=dexjpus
GET /api/catalog
GET /api/health
```

## Add A Metric

Add an object to `data/catalog.json`:

```json
{
  "id": "example_metric",
  "provider": "fred",
  "sourceId": "FRED_SERIES_ID",
  "name": "Human name",
  "short": "Short label",
  "group": "Rates",
  "region": "US",
  "frequency": "daily",
  "unit": "%",
  "transform": "level",
  "riskDirection": "high",
  "tags": ["rates", "funding"],
  "why": "Why this matters for macro shock monitoring."
}
```

Supported transforms:

- `level`
- `yoy_pct`
- `pct_change`
- `annualized_pct_change`
- `diff`

Use `riskDirection: "high"` when high readings are dangerous, `"low"` when low readings are dangerous, and `"two-sided"` when either tail can matter.

Derived metric example:

```json
{
  "id": "fed_model_gap",
  "provider": "derived",
  "sourceId": "S&P earnings yield - DGS10",
  "name": "Fed model earnings yield gap",
  "short": "EY - 10Y",
  "group": "Valuation",
  "frequency": "monthly",
  "unit": "pp",
  "transform": "level",
  "formula": "spread",
  "components": [
    {"name": "a", "provider": "multpl", "sourceId": "s-p-500-earnings-yield", "frequency": "monthly", "transform": "level"},
    {"name": "b", "provider": "fred", "sourceId": "DGS10", "frequency": "daily", "transform": "level"}
  ],
  "riskDirection": "low"
}
```

## Notes

This is a monitoring console, not investment advice. It highlights historical extremes and fast moves so you can investigate earlier. Click any row to inspect the full history, percentile band, event markers, historical min/max, and rule-based notes.
