import unittest
from unittest import mock

from src.market_caps import (
    COUNTRY_GROUPS,
    IndexSource,
    build_sp_factsheet_url,
    compile_groups,
    confidence_line,
    load_sp_global_bmi_snapshot,
    parse_msc_acwi_imi_country_caps,
    parse_msc_acwi_imi_market_cap,
    parse_msc_as_of,
    parse_cmc_all_countries,
    parse_cmc_global_market_cap,
    parse_source_date,
    parse_sp_factsheet_url,
    parse_sp_factsheet_as_of,
    require_dataset_reconciliation,
    require_fresh_source,
    reconciliation_metrics,
    reconciliation_passes,
    render_table,
    parse_sp_country_caps,
    parse_sp_factsheet_country_caps,
    fetch_sp_global_bmi_dataset,
    parse_wfe_period,
    parse_wfe_market_caps,
)


SAMPLE_SP_HTML = """
<h2>Country Breakdown</h2>
<div>Country/Region</div>
<div>United States</div>
<div>Number of Constituents</div>
<div>969</div>
<div>Total Market Cap</div>
<div>66,582,786.33</div>
<div>Index Weight</div>
<div>62.0%</div>
<div>Country/Region</div>
<div>South Korea</div>
<div>Number of Constituents</div>
<div>405</div>
<div>Total Market Cap</div>
<div>2,200,000.00</div>
<div>Index Weight</div>
<div>2.0%</div>
"""


class MarketCapsTest(unittest.TestCase):
    def source(self):
        return IndexSource(
            "Test BMI",
            "https://example.test",
            "https://example.test/file.pdf",
            "TEST_FACTSHEET_URL",
            "TEST_FACTSHEET_PATH",
        )

    def test_parse_sp_country_caps(self):
        caps = parse_sp_country_caps(self.source(), SAMPLE_SP_HTML)

        self.assertEqual(caps["United States"].market_cap_usd_millions, 66_582_786.33)
        self.assertEqual(caps["United States"].constituents, 969)
        self.assertEqual(caps["South Korea"].index_weight_pct, 2.0)

    def test_parse_sp_country_caps_fails_loudly_for_changed_layout(self):
        with self.assertRaises(RuntimeError):
            parse_sp_country_caps(self.source(), "<p>No table here</p>")

    def test_compile_groups_allows_country_overlap(self):
        caps = parse_sp_country_caps(self.source(), SAMPLE_SP_HTML)

        denominator = sum(cap.market_cap_usd_millions for cap in caps.values())
        results = compile_groups(caps, denominator)
        by_label = {result.label: result for result in results}

        self.assertAlmostEqual(denominator, 68_782_786.33)
        self.assertIn("South Korea", COUNTRY_GROUPS["Developed ex North America"]["countries"])
        self.assertIn("South Korea", COUNTRY_GROUPS["Emerging markets"]["countries"])
        self.assertAlmostEqual(by_label["US"].share_of_global_pct, 96.801525, places=6)

    def test_render_table_includes_totals_row(self):
        caps = parse_sp_country_caps(self.source(), SAMPLE_SP_HTML)
        denominator = sum(cap.market_cap_usd_millions for cap in caps.values())
        results = compile_groups(caps, denominator)

        table = render_table(results, denominator, "Test source")

        total_market_cap = sum(result.market_cap_usd_millions for result in results)
        total_share = sum(result.share_of_global_pct for result in results)
        self.assertIn("Totals", table)
        self.assertIn(f"{total_market_cap / 1_000_000:,.2f}", table)
        self.assertIn(f"{total_share:,.2f}%", table)

    def test_reconciliation_flags_source_mismatch(self):
        caps = parse_sp_country_caps(self.source(), SAMPLE_SP_HTML)
        results = compile_groups(caps, 60_000_000)

        metrics = reconciliation_metrics(results, caps, 60_000_000)

        self.assertGreater(metrics["group_share_sum_pct"], 106)
        self.assertFalse(reconciliation_passes(metrics))

    def test_parse_cmc_global_market_cap(self):
        html = '<span>total market cap: <a href="/total-marketcap/">$139.294 T</a></span>'

        self.assertEqual(parse_cmc_global_market_cap(html), 139_294_000)

    def test_parse_cmc_all_countries(self):
        html = """
        <tr><td class="rank-td td-right" data-sort="1">1</td><td data-sort="United States"><a href="/usa/largest-companies-in-the-usa-by-market-cap/">United States</a></td><td class="td-right" data-sort="69972181189012"><span class="currency-symbol-left">$</span>69.972 T</td><td class="td-right" data-sort="3487">3487</td></tr>
        <tr><td class="rank-td td-right" data-sort="11">11</td><td data-sort="South Korea"><a href="/south-korea/largest-companies-in-south-korea-by-market-cap/">South Korea</a></td><td class="td-right" data-sort="2889066488670"><span class="currency-symbol-left">$</span>2.889 T</td><td class="td-right" data-sort="138">138</td></tr>
        """

        caps = parse_cmc_all_countries(html)

        self.assertEqual(caps["United States"].market_cap_usd_millions, 69_972_181.189012)
        self.assertEqual(caps["South Korea"].constituents, 138)

    def test_confidence_line_labels_fallback_with_missing_countries(self):
        caps = parse_sp_country_caps(self.source(), SAMPLE_SP_HTML)
        results = compile_groups(caps, sum(cap.market_cap_usd_millions for cap in caps.values()))

        line = confidence_line(results, "CompaniesMarketCap all-countries table")

        self.assertIn("fallback estimate, not index-grade", line)
        self.assertIn("Canada", line)

    def test_parse_sp_factsheet_country_caps(self):
        text = """
        Country/Region Breakdown COUNTRY/REGION NUMBER OF CONSTITUENTS TOTAL MARKET CAP [USD MILLION] INDEX WEIGHT [%]
        United States 2,917 70,273,148.79 66.5
        Hong Kong SAR, China 137 1,036,106.85 0.5
        Luxembourg 4 67,213.45 0
        Based on index constituents' country of domicile.
        """

        caps = parse_sp_factsheet_country_caps(self.source(), text)

        self.assertEqual(caps["United States"].constituents, 2917)
        self.assertEqual(caps["Hong Kong"].market_cap_usd_millions, 1_036_106.85)
        self.assertEqual(caps["Luxembourg"].index_weight_pct, 0)

    def test_sp_global_bmi_snapshot_uses_index_weights(self):
        caps, as_of = load_sp_global_bmi_snapshot()
        results = compile_groups(caps, sum(cap.market_cap_usd_millions for cap in caps.values()), "index_weight")
        by_label = {result.label: result for result in results}

        self.assertEqual(as_of, "March 31 2026")
        self.assertAlmostEqual(by_label["US"].share_of_global_pct, 60.7)
        self.assertAlmostEqual(by_label["Canada"].share_of_global_pct, 3.2)

    def test_parse_sp_factsheet_as_of(self):
        self.assertEqual(parse_sp_factsheet_as_of("S&P GLOBAL BMI AS OF MARCH 31, 2026"), "March 31, 2026")

    def test_require_fresh_source_rejects_stale_index_data(self):
        with self.assertRaises(RuntimeError):
            require_fresh_source("December 31, 2025", "Test index", max_age_days=30)

    def test_parse_source_date_accepts_iso_and_index_formats(self):
        self.assertEqual(str(parse_source_date("2026-03-31")), "2026-03-31")
        self.assertEqual(str(parse_source_date("March 31, 2026")), "2026-03-31")

    def test_build_sp_factsheet_url_uses_stable_index_id(self):
        url = build_sp_factsheet_url("5457913")

        self.assertIn("idsenhancedfactsheet/file.pdf", url)
        self.assertIn("calcFrequency=M", url)
        self.assertIn("force_download=true", url)
        self.assertIn("indexId=5457913", url)
        self.assertIn("languageId=1", url)

    def test_parse_sp_factsheet_url_from_index_page(self):
        html = """
        <a href="/spdji/en/idsenhancedfactsheet/file.pdf?calcFrequency=M&amp;force_download=true&amp;indexId=5457913&amp;languageId=1">
          Factsheet
        </a>
        """

        url = parse_sp_factsheet_url(html, "https://www.spglobal.com/spdji/en/indices/equity/sp-global-bmi/")

        self.assertEqual(
            url,
            "https://www.spglobal.com/spdji/en/idsenhancedfactsheet/file.pdf?calcFrequency=M&force_download=true&indexId=5457913&languageId=1",
        )

    def test_sp_global_bmi_dataset_uses_live_index_page_before_pdf(self):
        page_text = """
        S&P GLOBAL BMI AS OF APRIL 30, 2026
        Country/Region Breakdown COUNTRY/REGION NUMBER OF CONSTITUENTS TOTAL MARKET CAP [USD MILLION] INDEX WEIGHT [%]
        United States 2,917 70,273,148.79 66.5
        Hong Kong SAR, China 137 1,036,106.85 0.5
        Luxembourg 4 67,213.45 0
        """

        with mock.patch("src.market_caps.fetch_sp_global_bmi_page_text", return_value=page_text), mock.patch(
            "src.market_caps.fetch_factsheet_text"
        ) as fetch_pdf:
            dataset = fetch_sp_global_bmi_dataset()

        fetch_pdf.assert_not_called()
        self.assertEqual(dataset.country_caps["United States"].index_weight_pct, 66.5)
        self.assertIn("April 30, 2026", dataset.freshness_label)

    def test_sp_global_bmi_dataset_does_not_use_snapshot_by_default(self):
        with mock.patch("src.market_caps.fetch_sp_global_bmi_page_text", side_effect=RuntimeError("blocked")), mock.patch(
            "src.market_caps.fetch_factsheet_text", side_effect=RuntimeError("blocked")
        ), mock.patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(RuntimeError) as context:
                fetch_sp_global_bmi_dataset()

        self.assertIn("SP_ALLOW_STALE_SNAPSHOT=1", str(context.exception))

    def test_parse_msci_acwi_imi_factsheet_country_weights(self):
        text = """
        MAR 31, 2026 Index Factsheet
        Mkt Cap ( USD Millions) Index 100,882,259.42 Largest 4,237,920.00
        COUNTRY WEIGHTS
        United States 61.98% Japan 5.69% United Kingdom 3.43% Canada 3.25%
        China 2.75% Other 22.9%
        MAR 31, 2026 Index Factsheet
        """

        caps = parse_msc_acwi_imi_country_caps(text)

        self.assertEqual(parse_msc_as_of(text), "March 31, 2026")
        self.assertEqual(parse_msc_acwi_imi_market_cap(text), 100_882_259.42)
        self.assertAlmostEqual(caps["United States"].index_weight_pct, 61.98)
        self.assertAlmostEqual(caps["Canada"].market_cap_usd_millions, 3_278_673.43115)

    def test_msci_top_country_weights_fail_full_dataset_reconciliation(self):
        text = """
        MAR 31, 2026 Index Factsheet
        Mkt Cap ( USD Millions) Index 100,882,259.42
        COUNTRY WEIGHTS
        United States 61.98% Japan 5.69% United Kingdom 3.43% Canada 3.25%
        China 2.75% Other 22.9%
        MAR 31, 2026 Index Factsheet
        """
        dataset = type(
            "Dataset",
            (),
            {
                "country_caps": parse_msc_acwi_imi_country_caps(text),
                "denominator_usd_millions": parse_msc_acwi_imi_market_cap(text),
                "share_basis": "index_weight",
                "source_label": "MSCI ACWI IMI country weights",
            },
        )()

        with self.assertRaises(RuntimeError):
            require_dataset_reconciliation(dataset)

    def test_parse_wfe_market_caps(self):
        html = """
        <tr><td>B3 - Brasil Bolsa Balcão</td><td>1,004,856.37</td><td>1,068,429.26</td><td>52.6%</td></tr>
        <tr><td>Bolsa de Valores de Colombia</td><td>154,556.47</td><td>143,212.86</td><td>56.5%</td></tr>
        <tr><td>Nasdaq - US</td><td>38,041,433.47</td><td>36,942,114.76</td><td>25.0%</td></tr>
        <tr><td>NYSE</td><td>29,477,187.70</td><td>31,861,989.26</td><td>0.6%</td></tr>
        """

        caps = parse_wfe_market_caps(html)

        self.assertEqual(caps["Brazil"].market_cap_usd_millions, 1_068_429.26)
        self.assertEqual(caps["Colombia"].market_cap_usd_millions, 143_212.86)
        self.assertAlmostEqual(caps["United States"].market_cap_usd_millions, 68_804_104.02)

    def test_parse_wfe_period(self):
        html = "<h1>Market Statistics - April 2026</h1>"

        self.assertEqual(
            parse_wfe_period(html, "https://focus.world-exchanges.org/issue/april-2026/market-statistics"),
            "April 2026",
        )


if __name__ == "__main__":
    unittest.main()
