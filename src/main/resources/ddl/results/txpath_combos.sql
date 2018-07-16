IF OBJECT_ID('@results_schema.txpath_combos', 'U') IS NULL
CREATE TABLE @results_schema.txpath_combos
(
	analysis_id INT NOT NULL, 
	person_id BIGINT, 
	drug_era_start_date DATE NOT NULL, 
	drug_era_end_date DATE NOT NULL, 
	exposure_ordinal BIGINT NULL,
	cnt_exposures INT NULL
);
