# Global Market Cap Scraper

Builds a live table for four ETF-shaped equity-market regions:

- US, matching `XUU.TO`
- Canada, matching `VCN.TO`
- Developed ex North America, matching `VIU.TO`
- Emerging markets, matching `XEC.TO`

Each run checks current official ETF/provider pages, fetches the latest popular global ETF country weights it can parse, and prints each group as a share of the global equity universe.

## Usage

```bash
python3 -m src.market_caps
```

For JSON output:

```bash
python3 -m src.market_caps --json
```

## Web App

This repo is ready to deploy to Vercel as a static web app with a Python serverless function.

- `index.html` renders the table and refresh button.
- `api/index.py` returns the live JSON payload at `/api`.

## Methodology

The preferred source is Vanguard Total World Stock ETF (`VT`) country weights. VT is used as a practical proxy for each country's share of the global investable equity market because the fund tracks a broad global market-cap-weighted equity portfolio. The app uses the ETF-published country weight for the percentage column and normalizes those weights to a USD 100 tn display denominator for the market-cap column.

If VT is unavailable, the app tries the SPDR Portfolio MSCI Global Stock Market ETF (`SPGM`) geographical breakdown. If both ETF sources are unavailable, it falls back to the older index/provider chain: live S&P Global BMI index page, official S&P Global BMI factsheet PDF, public MSCI ACWI IMI factsheet, legacy S&P Developed BMI + S&P Emerging BMI sources, then a CompaniesMarketCap/WFE fallback. Index-grade and ETF sources must pass a freshness check before they are used. The checked-in S&P Global BMI snapshot is not used by default because it can become stale quickly.

The S&P factsheet source can be overridden without code changes:

- `SP_GLOBAL_BMI_FACTSHEET_URL`
- `SP_GLOBAL_BMI_FACTSHEET_PATH`
- `SP_DEVELOPED_BMI_FACTSHEET_URL`
- `SP_EMERGING_BMI_FACTSHEET_URL`
- `SP_DEVELOPED_BMI_FACTSHEET_PATH`
- `SP_EMERGING_BMI_FACTSHEET_PATH`

For local debugging only, `SP_ALLOW_STALE_SNAPSHOT=1` allows the checked-in S&P Global BMI snapshot when both live S&P Global BMI paths are blocked.

Country membership follows the ETF index methodology rather than a single provider's classification. That means some country overlap is intentional. For example, South Korea is included in the FTSE developed exposure represented by `VIU.TO` and in the MSCI emerging exposure represented by `XEC.TO`.

If a source page changes layout or blocks the country table, the run fails loudly instead of silently returning stale numbers.
