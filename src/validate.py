import os
import pandas as pd
import great_expectations as gx

OLAP_DIR = "/workspace/data/Olap"


def get_validator(context, name, df):
    """Membungkus DataFrame pandas ke dalam Great Expectations Validator."""
    datasource = context.data_sources.add_or_update_pandas(name=f"ds_{name}")
    asset = datasource.add_dataframe_asset(name=f"asset_{name}")
    batch_definition = asset.add_batch_definition_whole_dataframe(f"batch_{name}")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    return context.get_validator(batch=batch)


def validate_dim_date(context, df):
    v = get_validator(context, "dim_date", df)
    v.expect_table_columns_to_match_ordered_list(["year", "decade"])
    v.expect_column_values_to_not_be_null("year")
    v.expect_column_values_to_be_unique("year")
    v.expect_column_values_to_be_between("year", min_value=2000, max_value=2050)
    v.expect_column_values_to_not_be_null("decade")
    v.expect_column_values_to_match_regex("decade", r"^\d{4}s$")
    return v.validate()


def validate_dim_country(context, df):
    v = get_validator(context, "dim_country", df)
    v.expect_table_columns_to_match_ordered_list(
        ["country_name", "iso3_code", "continent", "is_asean"]
    )
    v.expect_column_values_to_not_be_null("country_name")
    v.expect_column_values_to_be_unique("country_name")
    v.expect_column_values_to_be_in_set("is_asean", [True, False])
    # Pastikan baris pembanding 'World' dan 'United States' ikut masuk
    v.expect_column_distinct_values_to_contain_set(
        "country_name", ["World", "United States"]
    )
    return v.validate()


def validate_dim_us_state(context, df):
    v = get_validator(context, "dim_us_state", df)
    v.expect_table_columns_to_match_ordered_list(["state_code", "state_name"])
    v.expect_column_values_to_not_be_null("state_code")
    v.expect_column_values_to_be_unique("state_code")
    v.expect_column_value_lengths_to_equal("state_code", 2)
    v.expect_column_values_to_not_be_null("state_name")
    return v.validate()


def validate_dim_data_center(context, df, valid_countries, valid_states):
    v = get_validator(context, "dim_data_center", df)
    v.expect_table_columns_to_match_ordered_list(
        [
            "datacenter_name",
            "country_name",
            "state_code",
            "owner_name",
            "owner_confidence",
            "users",
            "investor",
        ]
    )
    v.expect_column_values_to_not_be_null("datacenter_name")
    v.expect_column_values_to_be_unique("datacenter_name")
    v.expect_column_values_to_not_be_null("country_name")
    # Cek integritas Foreign Key ke dim_country & dim_us_state
    v.expect_column_values_to_be_in_set("country_name", valid_countries)
    v.expect_column_values_to_be_in_set("state_code", valid_states)  # otomatis abaikan NULL
    return v.validate()


def validate_fact_grid_country(context, df, valid_countries, valid_years):
    v = get_validator(context, "fact_grid_country", df)
    v.expect_table_columns_to_match_ordered_list(
        [
            "country_name",
            "year",
            "electricity_source",
            "generation_twh",
            "emissions_mtco2e",
            "intensity_gco2e_kwh",
        ]
    )
    v.expect_column_values_to_not_be_null("country_name")
    v.expect_column_values_to_not_be_null("year")
    v.expect_column_values_to_not_be_null("electricity_source")
    v.expect_column_values_to_not_be_null("generation_twh")
    # Composite Primary Key harus unik
    v.expect_compound_columns_to_be_unique(
        ["country_name", "year", "electricity_source"]
    )
    # Cek integritas Foreign Key
    v.expect_column_values_to_be_in_set("country_name", valid_countries)
    v.expect_column_values_to_be_in_set("year", valid_years)
    return v.validate()


def validate_fact_grid_state(context, df, valid_states, valid_years):
    v = get_validator(context, "fact_grid_state", df)
    v.expect_table_columns_to_match_ordered_list(
        [
            "state_code",
            "year",
            "electricity_source",
            "generation_twh",
            "emissions_mtco2e",
            "intensity_gco2e_kwh",
        ]
    )
    v.expect_column_values_to_not_be_null("state_code")
    v.expect_column_values_to_not_be_null("year")
    v.expect_column_values_to_not_be_null("electricity_source")
    v.expect_column_values_to_not_be_null("generation_twh")
    # Composite Primary Key harus unik
    v.expect_compound_columns_to_be_unique(
        ["state_code", "year", "electricity_source"]
    )
    # Cek integritas Foreign Key
    v.expect_column_values_to_be_in_set("state_code", valid_states)
    v.expect_column_values_to_be_in_set("year", valid_years)
    return v.validate()


def validate_fact_infrastructure(context, df, valid_datacenters):
    v = get_validator(context, "fact_infrastructure", df)
    v.expect_table_columns_to_match_ordered_list(
        [
            "datacenter_name",
            "record_date",
            "is_actual",
            "construction_status",
            "buildings_operational",
            "it_power_mw",
            "total_power_mw",
            "pue",
            "h100e",
            "annual_operating_cost_usd",
        ]
    )
    v.expect_column_values_to_not_be_null("datacenter_name")
    v.expect_column_values_to_not_be_null("record_date")
    v.expect_column_values_to_not_be_null("is_actual")
    v.expect_column_values_to_be_in_set("is_actual", [True, False])
    # Composite Primary Key harus unik
    v.expect_compound_columns_to_be_unique(["datacenter_name", "record_date"])
    # Cek integritas Foreign Key ke dim_data_center
    v.expect_column_values_to_be_in_set("datacenter_name", valid_datacenters)
    # Pastikan tidak ada kapasitas/biaya negatif yang lolos
    v.expect_column_values_to_be_between("it_power_mw", min_value=0)
    v.expect_column_values_to_be_between("total_power_mw", min_value=0)
    v.expect_column_values_to_be_between("h100e", min_value=0)
    v.expect_column_values_to_be_between("annual_operating_cost_usd", min_value=0)
    return v.validate()


def run_validation():
    # Ephemeral context: tidak perlu folder gx/ terpisah di repo
    context = gx.get_context(mode="ephemeral")

    # Baca semua hasil transformasi OLAP
    df_date = pd.read_csv(os.path.join(OLAP_DIR, "dim_date.csv"))
    df_country = pd.read_csv(os.path.join(OLAP_DIR, "dim_country.csv"))
    df_state = pd.read_csv(os.path.join(OLAP_DIR, "dim_us_state.csv"))
    df_dc = pd.read_csv(os.path.join(OLAP_DIR, "dim_data_center.csv"))

    df_grid_country = pd.read_csv(os.path.join(OLAP_DIR, "fact_grid_country.csv"))
    df_grid_state = pd.read_csv(os.path.join(OLAP_DIR, "fact_grid_state.csv"))
    df_infra = pd.read_csv(os.path.join(OLAP_DIR, "fact_infrastructure.csv"))

    # Himpunan kunci valid untuk pengecekan Foreign Key
    valid_years = set(df_date["year"].dropna().astype(int))
    valid_countries = set(df_country["country_name"].dropna())
    valid_states = set(df_state["state_code"].dropna())
    valid_datacenters = set(df_dc["datacenter_name"].dropna())

    results = {
        "dim_date": validate_dim_date(context, df_date),
        "dim_country": validate_dim_country(context, df_country),
        "dim_us_state": validate_dim_us_state(context, df_state),
        "dim_data_center": validate_dim_data_center(
            context, df_dc, valid_countries, valid_states
        ),
        "fact_grid_country": validate_fact_grid_country(
            context, df_grid_country, valid_countries, valid_years
        ),
        "fact_grid_state": validate_fact_grid_state(
            context, df_grid_state, valid_states, valid_years
        ),
        "fact_infrastructure": validate_fact_infrastructure(
            context, df_infra, valid_datacenters
        ),
    }

    failed_tables = []
    for table_name, res in results.items():
        stats = res.statistics
        status = "PASSED" if res.success else "FAILED"
        print(
            f"[{status}] {table_name}: "
            f"{stats['successful_expectations']}/{stats['evaluated_expectations']} expectations berhasil."
        )
        if not res.success:
            failed_tables.append(table_name)
            for r in res.results:
                if not r.success:
                    print(f"   -> Gagal pada: {r.expectation_config.type} | kwargs: {r.expectation_config.kwargs}")

    if failed_tables:
        raise ValueError(
            f"Validasi Great Expectations gagal pada tabel: {', '.join(failed_tables)}. Proses load dibatalkan."
        )

    print("Semua validasi OLAP berhasil! Aman untuk lanjut ke tahap load database.")


if __name__ == "__main__":
    run_validation()