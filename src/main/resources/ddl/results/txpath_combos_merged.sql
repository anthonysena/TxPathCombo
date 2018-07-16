IF OBJECT_ID('@results_schema.txpath_combos_merged', 'U') IS NULL
CREATE TABLE @results_schema.txpath_combos_merged
(
	analysis_id int NOT NULL, 
	sequence_id bigint NULL, 
	combo_name varchar(4000) NULL, 
	cohort_definition_id bigint NOT NULL, 
	person_id bigint NOT NULL, 
	drug_era_start_date date NOT NULL, 
	drug_era_end_date date NOT NULL, 
	exposure_ordinal bigint NULL
);
