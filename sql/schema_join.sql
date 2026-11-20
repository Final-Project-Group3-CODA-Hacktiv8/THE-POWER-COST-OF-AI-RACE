--KQ1
SELECT 
    i.datacenter_name,
    dc.owner_name,
    dc.country_name,
    dc.state_code,
    i.record_date,
    EXTRACT(YEAR FROM i.record_date) AS year,
    i.is_actual,
    i.total_power_mw,
    i.it_power_mw,
    i.h100e,
    i.annual_operating_cost_usd
FROM fact_infrastructure i
JOIN dim_data_center dc ON i.datacenter_name = dc.datacenter_name
WHERE EXTRACT(YEAR FROM i.record_date) BETWEEN 2021 AND 2026;

--KQ2
SELECT 
    i.datacenter_name,
    dc.country_name,
    dc.state_code,
    i.record_date,
    EXTRACT(YEAR FROM i.record_date) AS year,
    i.is_actual,
    i.total_power_mw,
    -- Menarik intensitas karbon grid sesuai tahun atau fallback ke 2025
    COALESCE(
        CASE WHEN dc.country_name = 'United States' AND dc.state_code IS NOT NULL THEN gs.intensity_gco2e_kwh ELSE gc.intensity_gco2e_kwh END,
        CASE WHEN dc.country_name = 'United States' AND dc.state_code IS NOT NULL THEN gs_2025.intensity_gco2e_kwh ELSE gc_2025.intensity_gco2e_kwh END
    ) AS grid_carbon_intensity_gco2e_kwh
FROM fact_infrastructure i
JOIN dim_data_center dc ON i.datacenter_name = dc.datacenter_name
LEFT JOIN fact_grid_state gs ON dc.state_code = gs.state_code AND EXTRACT(YEAR FROM i.record_date) = gs.year AND gs.electricity_source = 'Total generation'
LEFT JOIN fact_grid_country gc ON dc.country_name = gc.country_name AND EXTRACT(YEAR FROM i.record_date) = gc.year AND gc.electricity_source = 'Total generation'
LEFT JOIN fact_grid_state gs_2025 ON dc.state_code = gs_2025.state_code AND gs_2025.year = 2025 AND gs_2025.electricity_source = 'Total generation'
LEFT JOIN fact_grid_country gc_2025 ON dc.country_name = gc_2025.country_name AND gc_2025.year = 2025 AND gc_2025.electricity_source = 'Total generation'
WHERE EXTRACT(YEAR FROM i.record_date) BETWEEN 2021 AND 2026;

--KQ3
SELECT 
    i.datacenter_name,
    dc.owner_name,
    dc.country_name,
    dc.state_code,
    i.record_date,
    EXTRACT(YEAR FROM i.record_date) AS year,
    i.is_actual,
    i.total_power_mw,
    i.it_power_mw,
    i.pue,
    i.h100e,
    i.annual_operating_cost_usd
FROM fact_infrastructure i
JOIN dim_data_center dc ON i.datacenter_name = dc.datacenter_name
WHERE i.pue IS NOT NULL 
  AND i.is_actual = TRUE 
  AND i.it_power_mw > 0;

--KQ4
SELECT 
    i.datacenter_name,
    dc.country_name,
    dc.state_code,
    i.record_date,
    EXTRACT(YEAR FROM i.record_date) AS year,
    i.is_actual,
    i.total_power_mw,
    -- Menarik total pembangkitan wilayah (TWh) dengan fallback 2025
    COALESCE(
        CASE WHEN dc.country_name = 'United States' AND dc.state_code IS NOT NULL THEN gs.generation_twh ELSE gc.generation_twh END,
        CASE WHEN dc.country_name = 'United States' AND dc.state_code IS NOT NULL THEN gs_2025.generation_twh ELSE gc_2025.generation_twh END
    ) AS region_total_generation_twh
FROM fact_infrastructure i
JOIN dim_data_center dc ON i.datacenter_name = dc.datacenter_name
LEFT JOIN fact_grid_state gs ON dc.state_code = gs.state_code AND EXTRACT(YEAR FROM i.record_date) = gs.year AND gs.electricity_source = 'Total generation'
LEFT JOIN fact_grid_country gc ON dc.country_name = gc.country_name AND EXTRACT(YEAR FROM i.record_date) = gc.year AND gc.electricity_source = 'Total generation'
LEFT JOIN fact_grid_state gs_2025 ON dc.state_code = gs_2025.state_code AND gs_2025.year = 2025 AND gs_2025.electricity_source = 'Total generation'
LEFT JOIN fact_grid_country gc_2025 ON dc.country_name = gc_2025.country_name AND gc_2025.year = 2025 AND gc_2025.electricity_source = 'Total generation'
WHERE EXTRACT(YEAR FROM i.record_date) BETWEEN 2021 AND 2026;

--KQ5
SELECT 
    i.datacenter_name,
    dc.country_name,
    dc.state_code,
    i.record_date,
    EXTRACT(YEAR FROM i.record_date) AS year,
    i.is_actual,
    i.pue,
    i.total_power_mw,
    i.annual_operating_cost_usd,
    -- Menarik intensitas karbon untuk matriks kelayakan
    COALESCE(
        CASE WHEN dc.country_name = 'United States' AND dc.state_code IS NOT NULL THEN gs.intensity_gco2e_kwh ELSE gc.intensity_gco2e_kwh END,
        CASE WHEN dc.country_name = 'United States' AND dc.state_code IS NOT NULL THEN gs_2025.intensity_gco2e_kwh ELSE gc_2025.intensity_gco2e_kwh END
    ) AS grid_carbon_intensity_gco2e_kwh
FROM fact_infrastructure i
JOIN dim_data_center dc ON i.datacenter_name = dc.datacenter_name
LEFT JOIN fact_grid_state gs ON dc.state_code = gs.state_code AND EXTRACT(YEAR FROM i.record_date) = gs.year AND gs.electricity_source = 'Total generation'
LEFT JOIN fact_grid_country gc ON dc.country_name = gc.country_name AND EXTRACT(YEAR FROM i.record_date) = gc.year AND gc.electricity_source = 'Total generation'
LEFT JOIN fact_grid_state gs_2025 ON dc.state_code = gs_2025.state_code AND gs_2025.year = 2025 AND gs_2025.electricity_source = 'Total generation'
LEFT JOIN fact_grid_country gc_2025 ON dc.country_name = gc_2025.country_name AND gc_2025.year = 2025 AND gc_2025.electricity_source = 'Total generation'
WHERE EXTRACT(YEAR FROM i.record_date) BETWEEN 2021 AND 2026;