import os
import re
import glob
import shutil
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import StringType

# variable tetap
RAW_DIR = "/workspace/data/Raw"
CLEAN_DIR = "/workspace/data/Clean"
OLAP_DIR = "/workspace/data/Olap"

CUTOFF_DATE = "2026-11-15"
YEAR_MIN, YEAR_MAX = 2021, 2025

MANUAL_STATE = {
    "Google Mesa": "AZ",
    "Google Kansas City East": "MO",
    "Google Storey County": "NV",
    "AWS New Albany": "OH",
    "Stream Phoenix": "AZ",
    "Anthropic Barber Lake": "TX",
    "OpenAI Stargate Michigan": "MI",
    "OpenAI Stargate Milam": "TX",
    "OpenAI Stargate New Mexico": "NM",
    "OpenAI Stargate Wisconsin": "WI",
}

COUNTRY_ALIASES = {
    "USA": "United States",
    "US": "United States",
    "U.S.": "United States",
    "UK": "United Kingdom",
    "UAE": "United Arab Emirates",
    "Czechia": "Czech Republic",
}

OWNER_ALIASES = {
    "Coreweave": "CoreWeave",
    "AWS": "Amazon",
    "Amazon Web Services": "Amazon",
    "Alphabet": "Google",
    "xAI": "SpaceXAI",
}

ASEAN_ISO3 = {"BRN", "KHM", "IDN", "LAO", "MYS", "MMR", "PHL", "SGP", "THA", "VNM"}



# FUNGSI 

def read_csv(spark, filename):
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(f"{RAW_DIR}/{filename}")
    )


def clean_str(colname):
    x = F.trim(F.regexp_replace(F.col(f"`{colname}`").cast("string"), r"\s+", " "))
    return F.when(x == "", None).otherwise(x)


def save_csv(df, out_dir, filename):
    tmp = f"{out_dir}/_tmp_{filename}"
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .option("escape", '"')
        .csv(tmp)
    )
    part = glob.glob(f"{tmp}/part-*.csv")[0]
    shutil.move(part, f"{out_dir}/{filename}")
    shutil.rmtree(tmp)
    print(f"Tersimpan: {out_dir}/{filename} ({df.count()} baris, {len(df.columns)} kolom)")



# CLEANING

def clean_data_centers(spark, dc_raw, u_raw):
    us_ref = (
        u_raw.select("State", "State code")
        .distinct()
        .filter(F.col("State code") != "US")
        .collect()
    )
    state_codes = {r["State code"] for r in us_ref}
    state_names = dict(
        sorted({r["State"]: r["State code"] for r in us_ref}.items(), key=lambda kv: -len(kv[0]))
    )

    def parse_state(address):
        if not address:
            return None
        codes = [m for m in re.findall(r"\b[A-Z]{2}\b", address) if m in state_codes]
        if codes:
            return codes[-1]
        for name, code in state_names.items():
            if re.search(rf"\b{re.escape(name)}\b", address):
                return code
        return None

    parse_state_udf = F.udf(parse_state, StringType())

    dc = (
        dc_raw.select(
            clean_str("Name").alias("name"),
            clean_str("Country").alias("country"),
            clean_str("Address").alias("address"),
            clean_str("Owner").alias("owner_raw"),
            clean_str("Users").alias("users"),
            clean_str("Investors").alias("investor"),
            F.col("`Current power (MW)`").cast("double").alias("current_power_mw"),
            F.col("`Current H100 equivalents`").cast("double").alias("current_h100e"),
        )
        .dropna(subset=["name"])
        .dropDuplicates(["name"])
        .replace(COUNTRY_ALIASES, subset=["country"])
    )

    conf = F.regexp_extract("owner_raw", r"#(\w+)", 1)
    owner_clean = F.trim(F.regexp_replace("owner_raw", r"\s*#\w+\s*$", ""))

    dc = (
        dc.withColumn(
            "owner_confidence",
            F.when(F.col("owner_raw").isNull(), "unknown")
            .when(conf == "", "unspecified")
            .otherwise(conf),
        )
        .withColumn(
            "owner_name",
            F.when(F.col("owner_raw").isNull(), F.lit("Unknown")).otherwise(owner_clean),
        )
        .replace(OWNER_ALIASES, subset=["owner_name"])
        .drop("owner_raw")
    )

    manual_map = F.create_map(
        *[x for kv in MANUAL_STATE.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
    )

    dc = (
        dc.withColumn(
            "state_from_address",
            F.when(F.col("country") == "United States", parse_state_udf("address")),
        )
        .withColumn(
            "state_code",
            F.coalesce(
                "state_from_address",
                F.when(F.col("country") == "United States", manual_map[F.col("name")]),
            ),
        )
        .withColumn(
            "state_source",
            F.when(F.col("country") != "United States", "not_applicable")
            .when(F.col("state_from_address").isNotNull(), "address")
            .when(F.col("state_code").isNotNull(), "manual")
            .otherwise("missing"),
        )
        .drop("state_from_address")
        .withColumn("current_power_mw", F.when(F.col("current_power_mw") >= 0, F.col("current_power_mw")))
        .withColumn("current_h100e", F.when(F.col("current_h100e") >= 0, F.col("current_h100e")))
        .select(
            "name", "country", "state_code", "state_source", "address",
            "owner_name", "owner_confidence", "users", "investor",
            "current_power_mw", "current_h100e",
        )
        .cache()
    )
    return dc


def clean_timelines(tl_raw):
    tl = (
        tl_raw.select(
            clean_str("Data center").alias("data_center"),
            F.to_date(F.substring(F.col("Date").cast("string"), 1, 10)).alias("date"),
            clean_str("Construction status").alias("construction_status"),
            F.col("`Buildings operational`").cast("double").alias("buildings_operational"),
            F.col("`IT power (MW)`").cast("double").alias("it_power_mw"),
            F.col("`Power (MW)`").cast("double").alias("power_mw"),
            F.col("`H100 equivalents`").cast("double").alias("h100e"),
            F.col("`Annual operating cost (2025 USD billions)`").cast("double").alias("annual_opex_usd_bn"),
        )
        .dropna(subset=["data_center", "date"])
        .dropDuplicates(["data_center", "date"])
    )

    for c in ["buildings_operational", "it_power_mw", "power_mw", "h100e", "annual_opex_usd_bn"]:
        tl = tl.withColumn(c, F.when(F.col(c) >= 0, F.col(c)))

    tl = (
        tl.withColumn("is_actual", F.col("date") <= F.to_date(F.lit(CUTOFF_DATE)))
        .withColumn("status", F.when(F.col("is_actual"), "actual").otherwise("planned"))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
    )

    w_dc = Window.partitionBy("data_center")
    tl = tl.withColumn(
        "_per_mw_raw", F.when(F.col("it_power_mw") > 0, F.col("h100e") / F.col("it_power_mw"))
    )
    global_med = tl.agg(F.percentile_approx("_per_mw_raw", 0.5)).collect()[0][0]

    tl = (
        tl.withColumn("_dc_med", F.percentile_approx("_per_mw_raw", 0.5).over(w_dc))
        .withColumn("h100e_imputed", F.col("h100e").isNull())
        .withColumn(
            "h100e",
            F.when(F.col("h100e").isNotNull(), F.col("h100e"))
            .when(F.col("it_power_mw") == 0, F.lit(0.0))
            .otherwise(F.col("it_power_mw") * F.coalesce(F.col("_dc_med"), F.lit(global_med))),
        )
        .drop("_per_mw_raw", "_dc_med")
    )

    w_locf = Window.partitionBy("data_center").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    tl = (
        tl.withColumn(
            "buildings_operational",
            F.coalesce(F.last("buildings_operational", ignorenulls=True).over(w_locf), F.lit(0.0)).cast("int"),
        )
        .withColumn("pue", F.when(F.col("it_power_mw") > 0, F.round(F.col("power_mw") / F.col("it_power_mw"), 4)))
        .withColumn("h100e_per_mw", F.when(F.col("it_power_mw") > 0, F.col("h100e") / F.col("it_power_mw")))
        .withColumn("pue_outlier", F.col("pue").isNotNull() & ((F.col("pue") < 1) | (F.col("pue") > 2)))
        .withColumn("is_zero_capacity", F.col("it_power_mw") == 0)
        .withColumn("annual_operating_cost_usd", F.col("annual_opex_usd_bn") * F.lit(1_000_000_000.0))
        .select(
            "data_center", "date", "year", "month", "status", "is_actual",
            "construction_status", "buildings_operational", "it_power_mw", "power_mw", "pue",
            "h100e", "h100e_per_mw", "h100e_imputed",
            "annual_opex_usd_bn", "annual_operating_cost_usd",
            "pue_outlier", "is_zero_capacity",
        )
        .orderBy("data_center", "date")
        .cache()
    )
    return tl


def clean_global_grid(g_raw):
    return (
        g_raw.filter((F.col("Area type") == "Country or economy") | (F.col("Area") == "World"))
        .filter(F.col("Year").between(YEAR_MIN, YEAR_MAX))
        .select(
            clean_str("Area").alias("area"),
            clean_str("ISO 3 code").alias("iso3_code"),
            clean_str("Continent").alias("continent"),
            F.when(F.col("Area") == "World", "world").otherwise("country").alias("area_type"),
            F.col("Year").cast("int").alias("year"),
            clean_str("Electricity source").alias("electricity_source"),
            F.col("`Generation (TWh)`").cast("double").alias("generation_twh"),
            F.col("`Emissions (MtCO2e)`").cast("double").alias("emissions_mtco2e"),
            F.col("`Emissions intensity (gCO2e/kWh)`").cast("double").alias("emissions_intensity_gco2_kwh"),
        )
        .dropna(subset=["area", "year", "electricity_source", "generation_twh"])
        .dropDuplicates(["area", "year", "electricity_source"])
        .withColumn("is_asean", F.coalesce(F.col("iso3_code").isin(list(ASEAN_ISO3)), F.lit(False)))
        .withColumn(
            "is_outlier",
            (F.col("generation_twh") <= 0)
            | (F.col("emissions_intensity_gco2_kwh") < 0)
            | (F.col("emissions_intensity_gco2_kwh") > 1500),
        )
        .orderBy("area", "year", "electricity_source")
        .cache()
    )


def clean_us_grid(u_raw):
    return (
        u_raw.filter(F.col("Year").between(YEAR_MIN, YEAR_MAX))
        .filter(F.col("State code") != "US")
        .select(
            clean_str("State").alias("state"),
            F.upper(clean_str("State code")).alias("state_code"),
            F.col("Year").cast("int").alias("year"),
            clean_str("Electricity source").alias("electricity_source"),
            F.col("`Generation (TWh)`").cast("double").alias("generation_twh"),
            F.col("`Emissions (MtCO2e)`").cast("double").alias("emissions_mtco2e"),
            F.col("`Emissions intensity (gCO2e/kWh)`").cast("double").alias("emissions_intensity_gco2_kwh"),
        )
        .dropna(subset=["state_code", "year", "electricity_source", "generation_twh"])
        .dropDuplicates(["state_code", "year", "electricity_source"])
        .withColumn(
            "is_outlier",
            (F.col("generation_twh") <= 0)
            | (F.col("emissions_intensity_gco2_kwh") < 0)
            | (F.col("emissions_intensity_gco2_kwh") > 1500),
        )
        .orderBy("state_code", "year", "electricity_source")
        .cache()
    )



# TRANSFORM KE MODEL OLAP 

def build_olap_tables(dc, tl, g, u):
    # dim_date
    dim_date = (
        tl.select("year")
        .union(g.select("year"))
        .union(u.select("year"))
        .dropna(subset=["year"])
        .distinct()
        .withColumn("decade", F.concat((F.floor(F.col("year") / 10) * 10).cast("string"), F.lit("s")))
        .select(F.col("year").cast("int").alias("year"), F.col("decade").cast("string").alias("decade"))
        .orderBy("year")
    )

    # dim_country
    g_countries = (
        g.filter(F.col("area_type").isin("country", "world"))
        .select(F.col("area").alias("country_name"), "iso3_code", "continent", "is_asean")
        .dropDuplicates(["country_name"])
    )
    dc_countries = dc.select(F.col("country").alias("country_name")).dropna().distinct()
    dim_country = (
        dc_countries.join(g_countries, on="country_name", how="full_outer")
        .withColumn(
            "is_asean",
            F.coalesce(F.col("is_asean"), F.col("iso3_code").isin(list(ASEAN_ISO3)), F.lit(False)),
        )
        .select("country_name", "iso3_code", "continent", "is_asean")
        .orderBy("country_name")
    )

    # dim_us_state
    dim_us_state = (
        u.select(
            F.col("state_code").cast("string").alias("state_code"),
            F.col("state").cast("string").alias("state_name"),
        )
        .dropna(subset=["state_code"])
        .dropDuplicates(["state_code"])
        .orderBy("state_code")
    )

    # dim_data_center
    dim_data_center = (
        dc.select(
            F.col("name").alias("datacenter_name"),
            F.col("country").alias("country_name"),
            F.col("state_code").alias("state_code"),
            F.col("owner_name").alias("owner_name"),
            F.col("owner_confidence").alias("owner_confidence"),
            F.col("users").alias("users"),
            F.col("investor").alias("investor"),
        )
        .dropDuplicates(["datacenter_name"])
        .orderBy("datacenter_name")
    )

    # fact_grid_country
    fact_grid_country = (
        g.filter(F.col("area_type").isin("country", "world"))
        .select(
            F.col("area").alias("country_name"),
            F.col("year").cast("int").alias("year"),
            F.col("electricity_source").alias("electricity_source"),
            F.col("generation_twh").alias("generation_twh"),
            F.col("emissions_mtco2e").alias("emissions_mtco2e"),
            F.col("emissions_intensity_gco2_kwh").alias("intensity_gco2e_kwh"),
        )
        .dropDuplicates(["country_name", "year", "electricity_source"])
        .orderBy("country_name", "year", "electricity_source")
    )

    # fact_grid_state
    fact_grid_state = (
        u.select(
            F.col("state_code").alias("state_code"),
            F.col("year").cast("int").alias("year"),
            F.col("electricity_source").alias("electricity_source"),
            F.col("generation_twh").alias("generation_twh"),
            F.col("emissions_mtco2e").alias("emissions_mtco2e"),
            F.col("emissions_intensity_gco2_kwh").alias("intensity_gco2e_kwh"),
        )
        .dropDuplicates(["state_code", "year", "electricity_source"])
        .orderBy("state_code", "year", "electricity_source")
    )

    # fact_infrastructure
    fact_infrastructure = (
        tl.join(dim_data_center.select("datacenter_name"), tl.data_center == dim_data_center.datacenter_name, how="inner")
        .select(
            F.col("datacenter_name").alias("datacenter_name"),
            F.col("date").alias("record_date"),
            F.col("is_actual").alias("is_actual"),
            F.col("construction_status").alias("construction_status"),
            F.col("buildings_operational").cast("double").alias("buildings_operational"),
            F.col("it_power_mw").alias("it_power_mw"),
            F.col("power_mw").alias("total_power_mw"),
            F.col("pue").alias("pue"),
            F.col("h100e").alias("h100e"),
            F.col("annual_operating_cost_usd").alias("annual_operating_cost_usd"),
        )
        .dropDuplicates(["datacenter_name", "record_date"])
        .orderBy("datacenter_name", "record_date")
    )

    return {
        "dim_date.csv": dim_date,
        "dim_country.csv": dim_country,
        "dim_us_state.csv": dim_us_state,
        "dim_data_center.csv": dim_data_center,
        "fact_grid_country.csv": fact_grid_country,
        "fact_grid_state.csv": fact_grid_state,
        "fact_infrastructure.csv": fact_infrastructure,
    }



def run_transform():
    os.makedirs(CLEAN_DIR, exist_ok=True)
    os.makedirs(OLAP_DIR, exist_ok=True)

    spark = (
        SparkSession.builder.appName("dc_electricity_transform")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Extract Raw
        dc_raw = read_csv(spark, "data_centers.csv")
        tl_raw = read_csv(spark, "data_center_timelines.csv")
        g_raw = read_csv(spark, "release_generation_yearly_global.csv")
        u_raw = read_csv(spark, "release_us_subnational_generation_yearly.csv")

        # Simpan ke Clean
        dc = clean_data_centers(spark, dc_raw, u_raw)
        tl = clean_timelines(tl_raw)
        g = clean_global_grid(g_raw)
        u = clean_us_grid(u_raw)

        save_csv(dc, CLEAN_DIR, "data_centers_clean.csv")
        save_csv(tl, CLEAN_DIR, "data_center_timelines_clean.csv")
        save_csv(g, CLEAN_DIR, "generation_yearly_global_clean.csv")
        save_csv(u, CLEAN_DIR, "us_subnational_generation_yearly_clean.csv")

        # Build & Simpan Tabel OLAP
        olap_tables = build_olap_tables(dc, tl, g, u)
        for filename, df_olap in olap_tables.items():
            save_csv(df_olap, OLAP_DIR, filename)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_transform()