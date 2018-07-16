IF OBJECT_ID('@results_schema.txpath_sequenced', 'U') IS NULL
CREATE TABLE @results_schema.txpath_sequenced
(
	analysis_id int NOT NULL, 
	cohort_definition_id bigint NOT NULL, 
	person_id bigint NOT NULL, 
	sequence_id bigint NULL, 
	exposure_name varchar(4000) NULL, 
	drug_era_start_date date NOT NULL, 
	drug_era_end_date date NOT NULL, 
	duration_in_d int NULL, 
	days_from_index int NULL, 
	exposure_ordinal bigint NULL
);
