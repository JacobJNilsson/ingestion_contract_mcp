"""Tests for data quality issues in contract generation

These tests ensure proper handling of:
- UTF-8 BOM (Byte Order Mark)
- Data type detection with sparse data
- Multiple date formats
- European number formats (comma as decimal separator)
"""

from pathlib import Path

from mcp_server.contract_generator import (
    detect_data_types,
    generate_source_analysis,
    generate_source_contract,
)


class TestBOMHandling:
    """Test handling of UTF-8 Byte Order Mark"""

    def test_strip_bom_from_csv_headers(self, tmp_path: Path) -> None:
        """BOM should be stripped from first field name"""
        csv_content = "\ufeffName;Age;City\nJohn;30;NYC\nJane;25;LA"
        csv_file = tmp_path / "bom_test.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        analysis = generate_source_analysis(str(csv_file))

        # First field should be "Name" without BOM
        assert analysis["sample_fields"][0] == "Name"
        assert "\ufeff" not in analysis["sample_fields"][0]

    def test_strip_bom_from_source_contract(self, tmp_path: Path) -> None:
        """Source contract should not contain BOM in field names and should warn about it"""
        csv_content = "\ufeffDatum;Konto;Belopp\n2025-10-25;12345;1000"
        csv_file = tmp_path / "bom_contract.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        contract = generate_source_contract(str(csv_file), "test_bom")

        # Check that BOM is not in the schema fields
        assert contract.data_schema.fields[0] == "Datum"
        assert "\ufeff" not in contract.data_schema.fields[0]

        # Check that BOM presence is noted in quality issues
        assert len(contract.quality_metrics.issues) > 0
        assert any("BOM" in issue for issue in contract.quality_metrics.issues)


class TestDataTypeDetection:
    """Test data type detection with sparse/empty values"""

    def test_detect_types_with_all_populated_row(self) -> None:
        """Types should be correctly detected when all fields are populated"""
        row = ["2025-10-25", "Account123", "190", "9.22", "USD"]
        types = detect_data_types(row)

        assert types[0] == "date"  # YYYY-MM-DD format
        assert types[1] == "text"
        assert types[2] == "numeric"
        assert types[3] == "numeric"
        assert types[4] == "text"

    def test_detect_types_with_empty_values(self) -> None:
        """Empty values should be marked as empty"""
        row = ["2025-10-25", "", "100", "", "SEK"]
        types = detect_data_types(row)

        assert types[0] == "date"
        assert types[1] == "empty"
        assert types[2] == "numeric"
        assert types[3] == "empty"
        assert types[4] == "text"

    def test_detect_types_european_numbers(self) -> None:
        """European number format (comma decimal) should be detected"""
        row = ["100,50", "9,22", "-16772,74", "9,541895"]
        types = detect_data_types(row)

        assert all(t == "numeric" for t in types), f"Expected all numeric, got {types}"

    def test_detect_types_mixed_number_formats(self) -> None:
        """Mixed US and European number formats should both be detected"""
        row = ["100.50", "9,22", "-16772.74", "9,541895"]
        types = detect_data_types(row)

        assert all(t == "numeric" for t in types), f"Expected all numeric, got {types}"

    def test_analysis_scans_multiple_rows_for_types(self, tmp_path: Path) -> None:
        """Type detection should scan multiple rows, not just the first"""
        # First data row has many empty fields
        # Later rows have data in those fields
        csv_content = """Datum;Konto;Typ;Amount;Price;Currency
2025-10-25;Account1;Deposit;;;SEK
2025-10-24;Account2;Buy;190;9,22;USD
2025-10-23;Account3;Sell;50;15,75;EUR"""
        csv_file = tmp_path / "sparse.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        analysis = generate_source_analysis(str(csv_file))
        types = analysis["data_types"]

        # All columns should be properly typed, not "empty"
        assert types[0] == "date", "Datum should be date"
        assert types[1] == "text", "Konto should be text"
        assert types[2] == "text", "Typ should be text"
        assert types[3] == "numeric", "Amount should be numeric (has 190, 50)"
        assert types[4] == "numeric", "Price should be numeric (has 9,22, 15,75)"
        assert types[5] == "text", "Currency should be text"


class TestDateFormatDetection:
    """Test detection of various date formats"""

    def test_detect_iso_date_format(self) -> None:
        """YYYY-MM-DD format should be detected as date"""
        row = ["2025-10-25", "2024-01-15", "2023-12-31"]
        types = detect_data_types(row)

        assert all(t == "date" for t in types), f"Expected all dates, got {types}"

    def test_detect_slash_date_format(self) -> None:
        """DD/MM/YYYY format should be detected as date"""
        row = ["25/10/2025", "15/01/2024", "31/12/2023"]
        types = detect_data_types(row)

        assert all(t == "date" for t in types), f"Expected all dates, got {types}"

    def test_detect_us_date_format(self) -> None:
        """MM/DD/YYYY format should be detected as date"""
        row = ["10/25/2025", "01/15/2024", "12/31/2023"]
        types = detect_data_types(row)

        assert all(t == "date" for t in types), f"Expected all dates, got {types}"

    def test_date_with_iso_format_in_contract(self, tmp_path: Path) -> None:
        """ISO date format should be properly detected in full contract"""
        csv_content = """Date;Description;Amount
2025-10-25;Payment;1000
2025-10-24;Withdrawal;-500"""
        csv_file = tmp_path / "dates.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        analysis = generate_source_analysis(str(csv_file))

        assert analysis["data_types"][0] == "date"
        assert analysis["data_types"][1] == "text"
        assert analysis["data_types"][2] == "numeric"


class TestAvanzaRealDataIssues:
    """Test with actual Avanza data that revealed the issues"""

    def test_avanza_csv_structure(self, tmp_path: Path) -> None:
        """Avanza CSV should be properly analyzed"""
        csv_content = """\ufeffDatum;Konto;Typ av transaktion;Värdepapper/beskrivning;Antal;Kurs;Belopp;Transaktionsvaluta;Courtage;Valutakurs;Instrumentvaluta;ISIN;Resultat
2025-10-25;Bröllop / Lägenhet;Insättning;Direktinsättning från Nordea Bank;;;25000;SEK;;;;;
2025-10-14;5395872;ADR-avgift;ADR ARM.O 2025-08-27;;;-3,8;SEK;;;;;
2025-10-10;5395872;Köp;Kodiak AI;190;9,22;-16772,74;SEK;57,25;9,541895;USD;US5000811043;
2025-10-10;5395872;Utländsk källskatt;Utdelning TSM 0.821965 USD/aktie 21%;22;;-36,06;SEK;;;;US8740391003;
2025-10-10;5395872;Utdelning;Taiwan Semicond Mfg Co;22;0,821965;171,73;SEK;;;USD;US8740391003;"""

        csv_file = tmp_path / "avanza.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        contract = generate_source_contract(str(csv_file), "avanza_transactions")

        # Test BOM is stripped
        assert contract.data_schema.fields[0] == "Datum"
        assert "\ufeff" not in contract.data_schema.fields[0]

        # Test proper delimiter detection
        assert contract.delimiter == ";"

        # Test data type detection (should scan multiple rows)
        types = contract.data_schema.data_types
        assert types[0] == "date", "Datum should be date"
        assert types[1] == "text", "Konto should be text"
        assert types[4] == "numeric", "Antal should be numeric (has 190, 22)"
        assert types[5] == "numeric", "Kurs should be numeric (has 9,22, 0,821965)"
        assert types[6] == "numeric", "Belopp should be numeric (has 25000, -3,8, etc)"
        assert types[8] == "numeric", "Courtage should be numeric (has 57,25)"
        assert types[9] == "numeric", "Valutakurs should be numeric (has 9,541895)"
        assert types[10] == "text", "Instrumentvaluta should be text (has USD)"
        assert types[11] == "text", "ISIN should be text (has US...)"

        # Verify that we don't have spurious "empty" types for columns with data
        populated_columns = [0, 1, 4, 5, 6, 8, 9, 10, 11]  # Columns that have data
        for col_idx in populated_columns:
            assert types[col_idx] != "empty", (
                f"Column {col_idx} ({contract.data_schema.fields[col_idx]}) should not be 'empty'"
            )
