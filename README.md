# Global Market Cap Scraper

Builds a live table for four ETF-shaped equity-market regions:

- US, matching `XUU.TO`
- Canada, matching `VCN.TO`
- Developed ex North America, matching `VIU.TO`
- Emerging markets, matching `XEC.TO`

Each run checks current official ETF/provider pages, fetches the latest S&P DJI country rows it can parse, and prints each group as a share of the global equity universe.

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

The preferred source is the live S&P Global BMI index page, which exposes the same current country breakdown as the factsheet. The app uses country `Index Weight [%]` for the percentage column because those weights are float-adjusted and match the index result shown in the factsheet. The market-cap column still shows the country `Total Market Cap [USD Million]` values from the same S&P source.

If the live index page is unavailable, the app tries the official S&P Global BMI factsheet PDF. Index-grade sources must pass a freshness check before they are used. If S&P blocks both live S&P Global BMI paths, the app tries the public MSCI ACWI IMI factsheet, but only accepts it if it exposes enough country-level coverage to calculate the ETF-shaped groups honestly. After that, it fails over to the older live S&P Developed BMI + S&P Emerging BMI sources, then a CompaniesMarketCap/WFE fallback. The checked-in S&P Global BMI snapshot is not used by default because it can become stale quickly.

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
