CREATE TABLE dim_country (
  country_name      TEXT PRIMARY KEY,          
  iso3_code         TEXT,
  continent         TEXT,
  is_asean          BOOLEAN
);

CREATE TABLE dim_us_state (
  state_code        CHAR(2) PRIMARY KEY,
  state_name        TEXT NOT NULL
);

CREATE TABLE dim_data_center (
  datacenter_name   TEXT NOT NULL PRIMARY KEY,
  country_name      TEXT NOT NULL REFERENCES dim_country(country_name),
  state_code        CHAR(2)       REFERENCES dim_us_state(state_code),   -- NULL di luar AS
  owner_name		TEXT,
  owner_confidence	TEXT,
  users				TEXT,
  investor			TEXT
);

CREATE TABLE dim_date (
  year        		 INT PRIMARY KEY,
  decade			 TEXT NOT NULL
);

CREATE TABLE fact_infrastructure (
  datacenter_name       	 TEXT REFERENCES dim_data_center(datacenter_name),
  record_date     			 DATE,
  construction_status        TEXT,
  buildings_operational      NUMERIC,
  it_power_mw                NUMERIC,
  total_power_mw             NUMERIC,
  pue                        NUMERIC CHECK (pue IS NULL OR pue BETWEEN 1 AND 2),
  h100e                      NUMERIC,
  annual_operating_cost_usd	 NUMERIC,
  is_actual					 BOOLEAN NOT NULL,
  PRIMARY KEY (datacenter_name, record_date)
);

CREATE TABLE fact_grid_country (
  country_name    		 	  TEXT  REFERENCES dim_country(country_name),                         
  year          		  	  INT	REFERENCES dim_date(year),
  electricity_source		  TEXT,
  generation_twh    	      NUMERIC,
  emissions_mtco2e  	  	  NUMERIC,
  intensity_gco2e_kwh  	      NUMERIC,
  PRIMARY KEY (country_name, year, electricity_source)
);

CREATE TABLE fact_grid_state (
  state_code         	 CHAR(2) REFERENCES dim_us_state(state_code),
  year               	 INT	 REFERENCES dim_date(year),
  electricity_source 	 TEXT,
  generation_twh     	 NUMERIC,
  emissions_mtco2e   	 NUMERIC,
  intensity_gco2e_kwh    NUMERIC,
  PRIMARY KEY (state_code, year, electricity_source)
);

