IF OBJECT_ID('@results_schema.test_table', 'U') IS NULL
CREATE TABLE @results_schema.test_table
(
	analysis_id INT NOT NULL, 
	person_id BIGINT NOT NULL
);
