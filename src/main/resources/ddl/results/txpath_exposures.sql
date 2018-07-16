IF OBJECT_ID('@results_schema.txpath_exposures', 'U') IS NULL
CREATE TABLE @results_schema.txpath_exposures
(
	analysis_id int NOT NULL, 
	drug_era_id bigint NOT NULL, 
	cohort_definition_id bigint NOT NULL, 
	person_id bigint NOT NULL, 
	drug_concept_id bigint NOT NULL, 
	concept_name varchar(255) NULL, 
	drug_era_start_date date NOT NULL, 
	drug_era_end_date date NOT NULL, 
	duration_in_d int NULL, 
	days_from_index int NULL, 
	exposure_ordinal bigint NULL
);
