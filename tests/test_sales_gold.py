import pandas as pd
import pytest

from scripts.warehouse.postgres.gold.sales_gold_load import (
    DimensionBuildError,
    build_dim_customer,
    build_dim_date,
    build_dim_product,
    build_dim_salesperson,
    build_dim_territory,
    build_fact_sales,
)


class TestBuildDimDate:
    """Test date dimension building logic."""

    def test_build_dim_date_creates_date_keys_and_calendar_attributes(self):
        """Date key should be YYYYMMDD format with calendar attributes."""
        headers = pd.DataFrame({"order_date": ["2011-05-31", "2011-06-01"]})

        result = build_dim_date(headers)

        assert result["date_id"].tolist() == [20110531, 20110601]
        assert result["month_name"].tolist() == ["May", "June"]
        assert result["is_weekend"].tolist() == [False, False]

    def test_build_dim_date_creates_continuous_date_range(self):
        """Date dimension should include all dates between min and max."""
        headers = pd.DataFrame({"order_date": ["2011-05-31", "2011-06-02"]})

        result = build_dim_date(headers)

        # Should have 3 rows: May 31, June 1, June 2
        assert len(result) == 3
        assert result["date_id"].min() == 20110531
        assert result["date_id"].max() == 20110602

    def test_build_dim_date_sets_year_quarter_month_attributes(self):
        """Date dimension should correctly calculate year, quarter, month."""
        headers = pd.DataFrame({"order_date": ["2011-01-15", "2011-04-15", "2011-10-15"]})

        result = build_dim_date(headers)

        # Filter to specific dates
        jan = result[result["date_id"] == 20110115].iloc[0]
        apr = result[result["date_id"] == 20110415].iloc[0]
        oct = result[result["date_id"] == 20111015].iloc[0]

        assert jan["year_number"] == 2011 and jan["quarter_number"] == 1
        assert apr["year_number"] == 2011 and apr["quarter_number"] == 2
        assert oct["year_number"] == 2011 and oct["quarter_number"] == 4

    def test_build_dim_date_identifies_weekends(self):
        """Date dimension should correctly identify weekends (Saturday/Sunday)."""
        # 2011-05-28 is Saturday, 2011-05-29 is Sunday, 2011-05-30 is Monday
        headers = pd.DataFrame({"order_date": ["2011-05-28", "2011-05-29", "2011-05-30"]})

        result = build_dim_date(headers)

        assert result[result["date_id"] == 20110528]["is_weekend"].values[0] == True  # Saturday
        assert result[result["date_id"] == 20110529]["is_weekend"].values[0] == True  # Sunday
        assert result[result["date_id"] == 20110530]["is_weekend"].values[0] == False  # Monday

    def test_build_dim_date_rejects_empty_or_invalid_input(self):
        with pytest.raises(DimensionBuildError, match="at least one valid order_date"):
            build_dim_date(pd.DataFrame({"order_date": ["not-a-date"]}))

        with pytest.raises(DimensionBuildError, match="order_date"):
            build_dim_date(pd.DataFrame())


class TestBuildDimensions:
    """Test dimension table building logic."""

    def test_build_dim_customer_deduplicates_by_customer_id(self):
        """Customer dimension should have unique customer_id."""
        customers = pd.DataFrame({
            "customer_id": [1, 1, 2],
            "customer_name": ["ABC Corp", "ABC Corp", "XYZ Inc"],
            "person_id": [None, None, 100],
            "store_id": [None, None, 10],
            "territory_id": [1, 1, 2],
            "account_number": ["AA123", "AA123", "XX456"],
        })

        result = build_dim_customer(customers)

        assert len(result) == 2
        assert result["customer_id"].is_unique

    def test_build_dim_product_deduplicates_by_product_id(self):
        """Product dimension should have unique product_id."""
        products = pd.DataFrame({
            "product_id": [100, 100, 101],
            "product_name": ["Road Bike", "Road Bike", "Mountain Bike"],
            "product_number": ["BK-R001", "BK-R001", "BK-M001"],
            "product_line": ["R", "R", "M"],
            "class": ["H", "H", "L"],
            "style": ["U", "U", "U"],
            "list_price": [500.00, 500.00, 400.00],
            "standard_cost": [250.00, 250.00, 200.00],
            "is_discontinued": [False, False, False],
        })

        result = build_dim_product(products)

        assert len(result) == 2
        assert result["product_id"].is_unique
        assert "product_class" in result.columns
        assert "product_style" in result.columns
        assert "class" not in result.columns
        assert "style" not in result.columns

    def test_dimension_duplicate_selection_is_deterministic(self):
        customers = pd.DataFrame({
            "customer_id": [1, 1],
            "customer_name": ["Zeta", "Alpha"],
            "person_id": [2, 1],
            "store_id": [2, 1],
            "territory_id": [2, 1],
            "account_number": ["ZZ", "AA"],
        })

        result = build_dim_customer(customers.sample(frac=1, random_state=7))

        assert result.iloc[0]["customer_name"] == "Alpha"

    def test_build_dim_territory_deduplicates_by_territory_id(self):
        """Territory dimension should have unique territory_id."""
        territories = pd.DataFrame({
            "territory_id": [1, 1, 2],
            "territory_name": ["Northwest", "Northwest", "Southwest"],
            "country_region_code": ["US", "US", "US"],
            "territory_group": ["North America", "North America", "North America"],
        })

        result = build_dim_territory(territories)

        assert len(result) == 2
        assert result["territory_id"].is_unique

    def test_dimension_builders_reject_missing_columns_and_null_keys(self):
        with pytest.raises(DimensionBuildError, match="Missing dimension columns"):
            build_dim_customer(pd.DataFrame({"customer_id": [1]}))

        customers = pd.DataFrame({
            "customer_id": [None],
            "customer_name": ["ABC Corp"],
            "person_id": [None],
            "store_id": [None],
            "territory_id": [1],
            "account_number": ["AA123"],
        })
        with pytest.raises(DimensionBuildError, match="NULL values"):
            build_dim_customer(customers)

    def test_build_dim_salesperson_deduplicates_by_salesperson_id(self):
        """Salesperson dimension should have unique salesperson_id."""
        salespeople = pd.DataFrame({
            "salesperson_id": [274, 274, 275],
            "business_entity_id": [274, 274, 275],
            "territory_id": [1, 1, 2],
            "sales_quota": [300000.00, 300000.00, 250000.00],
            "bonus": [4000.00, 4000.00, 5000.00],
            "commission_pct": [0.01, 0.01, 0.015],
            "salesperson_name": ["Linda Mitchell", "Linda Mitchell", "Jillian Carson"],
        })

        result = build_dim_salesperson(salespeople)

        assert len(result) == 2
        assert result["salesperson_id"].is_unique


class TestBuildFactSales:
    """Test fact table building and grain validation."""

    def test_build_fact_sales_preserves_line_item_grain(self):
        """Fact sales should maintain line-item grain (sales_order_detail_id unique)."""
        details = pd.DataFrame({
            "sales_order_id": [1, 1],
            "sales_order_detail_id": [10, 11],
            "product_id": [100, 101],
            "order_qty": [2, 1],
            "unit_price": [5.0, 10.0],
            "unit_price_discount": [0.1, 0.0],
            "line_total": [9.0, 10.0],
        })
        headers = pd.DataFrame({
            "sales_order_id": [1],
            "order_date": ["2011-05-31"],
            "customer_id": [20],
            "territory_id": [1],
            "salesperson_id": [None],
        })

        result = build_fact_sales(details, headers)

        assert len(result) == 2
        assert result["sales_order_detail_id"].is_unique

    def test_build_fact_sales_calculates_discount_amount(self):
        """Discount amount should be gross - line_total."""
        details = pd.DataFrame({
            "sales_order_id": [1],
            "sales_order_detail_id": [10],
            "product_id": [100],
            "order_qty": [2],
            "unit_price": [10.0],
            "unit_price_discount": [0.1],
            "line_total": [18.0],
        })
        headers = pd.DataFrame({
            "sales_order_id": [1],
            "order_date": ["2011-05-31"],
            "customer_id": [20],
            "territory_id": [1],
            "salesperson_id": [None],
        })

        result = build_fact_sales(details, headers)

        # Gross: 2 * 10.0 = 20.0, Line Total: 18.0, Discount: 20.0 - 18.0 = 2.0
        assert result.iloc[0]["discount_amount"] == 2.0

    def test_build_fact_sales_creates_order_date_id_yyyymmdd(self):
        """Order date ID should be YYYYMMDD format."""
        details = pd.DataFrame({
            "sales_order_id": [1],
            "sales_order_detail_id": [10],
            "product_id": [100],
            "order_qty": [1],
            "unit_price": [5.0],
            "unit_price_discount": [0.0],
            "line_total": [5.0],
        })
        headers = pd.DataFrame({
            "sales_order_id": [1],
            "order_date": ["2011-05-31"],
            "customer_id": [20],
            "territory_id": [1],
            "salesperson_id": [None],
        })

        result = build_fact_sales(details, headers)

        assert result.iloc[0]["order_date_id"] == 20110531

    def test_build_fact_sales_net_sales_equals_line_total(self):
        """Net sales should equal line total."""
        details = pd.DataFrame({
            "sales_order_id": [1],
            "sales_order_detail_id": [10],
            "product_id": [100],
            "order_qty": [1],
            "unit_price": [100.0],
            "unit_price_discount": [0.1],
            "line_total": [90.0],
        })
        headers = pd.DataFrame({
            "sales_order_id": [1],
            "order_date": ["2011-05-31"],
            "customer_id": [20],
            "territory_id": [1],
            "salesperson_id": [None],
        })

        result = build_fact_sales(details, headers)

        assert result.iloc[0]["net_sales"] == 90.0

    def test_build_fact_sales_handles_null_salesperson_id(self):
        """Salesperson ID should handle NULL values as Int64 nullable type."""
        details = pd.DataFrame({
            "sales_order_id": [1, 2],
            "sales_order_detail_id": [10, 20],
            "product_id": [100, 101],
            "order_qty": [1, 1],
            "unit_price": [5.0, 10.0],
            "unit_price_discount": [0.0, 0.0],
            "line_total": [5.0, 10.0],
        })
        headers = pd.DataFrame({
            "sales_order_id": [1, 2],
            "order_date": ["2011-05-31", "2011-06-01"],
            "customer_id": [20, 21],
            "territory_id": [1, 1],
            "salesperson_id": [274, None],
        })

        result = build_fact_sales(details, headers)

        assert str(result["salesperson_id"].dtype) == "Int64"
        assert result.iloc[0]["salesperson_id"] == 274
        assert pd.isna(result.iloc[1]["salesperson_id"])

    def test_build_fact_sales_joins_many_to_one_on_sales_order_id(self):
        """Fact sales should have many detail rows joining to one header."""
        details = pd.DataFrame({
            "sales_order_id": [1, 1, 1],
            "sales_order_detail_id": [10, 11, 12],
            "product_id": [100, 101, 102],
            "order_qty": [1, 1, 1],
            "unit_price": [5.0, 10.0, 15.0],
            "unit_price_discount": [0.0, 0.0, 0.0],
            "line_total": [5.0, 10.0, 15.0],
        })
        headers = pd.DataFrame({
            "sales_order_id": [1],
            "order_date": ["2011-05-31"],
            "customer_id": [20],
            "territory_id": [1],
            "salesperson_id": [274],
        })

        result = build_fact_sales(details, headers)

        # All 3 detail rows should have the same customer/territory/salesperson from header
        assert len(result) == 3
        assert (result["customer_id"] == 20).all()
        assert (result["territory_id"] == 1).all()
        assert (result["salesperson_id"] == 274).all()

    def test_build_fact_sales_converts_numeric_fields_to_int64_or_float(self):
        """Numeric fields should be properly typed."""
        details = pd.DataFrame({
            "sales_order_id": [1],  # Must match header type (int)
            "sales_order_detail_id": [10],
            "product_id": [100],
            "order_qty": ["2"],  # string
            "unit_price": ["5.50"],  # string
            "unit_price_discount": ["0.1"],  # string
            "line_total": ["11.00"],  # string
        })
        headers = pd.DataFrame({
            "sales_order_id": [1],  # int
            "order_date": ["2011-05-31"],
            "customer_id": [20],
            "territory_id": [1],
            "salesperson_id": [None],
        })

        result = build_fact_sales(details, headers)

        assert result.iloc[0]["order_qty"] == 2
        assert result.iloc[0]["unit_price"] == 5.50
        assert result.iloc[0]["line_total"] == 11.0


class TestFactSalesIntegrity:
    """Test referential integrity and data quality."""

    def test_fact_sales_output_columns_match_specification(self):
        """Fact sales should output correct column set."""
        details = pd.DataFrame({
            "sales_order_id": [1],
            "sales_order_detail_id": [10],
            "product_id": [100],
            "order_qty": [1],
            "unit_price": [5.0],
            "unit_price_discount": [0.0],
            "line_total": [5.0],
        })
        headers = pd.DataFrame({
            "sales_order_id": [1],
            "order_date": ["2011-05-31"],
            "customer_id": [20],
            "territory_id": [1],
            "salesperson_id": [None],
        })

        result = build_fact_sales(details, headers)

        expected_cols = {
            "sales_order_id", "sales_order_detail_id", "order_date_id",
            "customer_id", "product_id", "territory_id", "salesperson_id",
            "order_qty", "unit_price", "discount_amount", "line_total", "net_sales",
        }
        assert set(result.columns) == expected_cols

    def test_fact_sales_no_nulls_in_required_fields(self):
        """Required FK fields should not be NULL."""
        details = pd.DataFrame({
            "sales_order_id": [1],
            "sales_order_detail_id": [10],
            "product_id": [100],
            "order_qty": [1],
            "unit_price": [5.0],
            "unit_price_discount": [0.0],
            "line_total": [5.0],
        })
        headers = pd.DataFrame({
            "sales_order_id": [1],
            "order_date": ["2011-05-31"],
            "customer_id": [20],
            "territory_id": [1],
            "salesperson_id": [None],
        })

        result = build_fact_sales(details, headers)

        # These should never be NULL
        assert result["sales_order_id"].notna().all()
        assert result["sales_order_detail_id"].notna().all()
        assert result["order_date_id"].notna().all()
        assert result["customer_id"].notna().all()
        assert result["product_id"].notna().all()
        assert result["territory_id"].notna().all()
        # salesperson_id CAN be NULL for online orders


class TestKPICalculations:
    """Test KPI calculations in fact sales."""

    def test_sample_order_kpi_calculation(self):
        """Validate KPI calculation on sample order."""
        # Order: 2 units @ $10.00 with 10% discount = $18.00
        details = pd.DataFrame({
            "sales_order_id": [43697],
            "sales_order_detail_id": [100001],
            "product_id": [500],
            "order_qty": [2],
            "unit_price": [10.0],
            "unit_price_discount": [0.1],
            "line_total": [18.0],
        })
        headers = pd.DataFrame({
            "sales_order_id": [43697],
            "order_date": ["2011-06-15"],
            "customer_id": [21768],
            "territory_id": [6],
            "salesperson_id": [None],
        })

        result = build_fact_sales(details, headers)
        row = result.iloc[0]

        # Assertions
        assert row["order_qty"] == 2
        assert row["unit_price"] == 10.0
        assert row["discount_amount"] == 2.0  # (2 * 10) - 18 = 2
        assert row["net_sales"] == 18.0
        assert row["order_date_id"] == 20110615
        assert pd.isna(row["salesperson_id"])  # Online order

