CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (21601782,21600381,21601664,21601744,21601461,21601560)and invalid_reason is null
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (21601782,21600381,21601664,21601744,21601461,21601560)
  and c.invalid_reason is null

) I
) C;


WITH indexCohort AS (
	select 
		cohort_definition_id, 
		subject_id as person_id,
		cohort_start_date,
		cohort_end_date
	from @resultsSchema.cohort
	where cohort_definition_id IN (@cohortDefinitionIdList)
), 
drugEras AS (
	select distinct
		de.drug_era_id
		, ic.cohort_definition_id
		, de.person_id
		, de.drug_concept_id
		, LOWER(c.concept_name) concept_name
		, de.drug_era_start_date
		, de.drug_era_end_date
		, DATEDIFF(dd, de.drug_era_start_date, de.drug_era_end_date) duration_in_d
		, DATEDIFF(dd, ic.cohort_start_date, de.drug_era_start_date) days_from_index
	FROM @cdmSchema.DRUG_ERA de
	JOIN indexCohort ic on de.PERSON_ID = ic.PERSON_ID 
        and de.drug_era_start_date >= DATEADD(dd, -30, ic.cohort_start_date) 
        and de.drug_era_start_date <= DATEADD(dd, 30, ic.cohort_start_date)  
	JOIN #Codesets cs ON cs.concept_id = de.drug_concept_id
	JOIN @cdmSchema.CONCEPT c ON cs.concept_id = c.concept_id
), 
allExposure AS (
	SELECT 
		3 analysis_id
		, de.drug_era_id
		, de.cohort_definition_id
		, de.person_id
		, de.drug_concept_id
		, de.concept_name
		, de.drug_era_start_date
		, de.drug_era_end_date
		, de.duration_in_d
		, de.days_from_index
		, RANK() over (partition by de.cohort_definition_id, de.person_id order by de.drug_era_start_date) as exposure_ordinal
	FROM drugEras de
)
SELECT 
		analysis_id
		, drug_era_id
		, cohort_definition_id
		, person_id
		, drug_concept_id
		, concept_name
		, drug_era_start_date
		, drug_era_end_date
		, duration_in_d
		, days_from_index
		, exposure_ordinal
INTO #txpath_exposures
FROM allExposure
;

DELETE FROM @resultsSchema.txpath_exposures where analysis_id = 3;

INSERT INTO @resultsSchema.txpath_exposures (
	analysis_id,
	drug_era_id,
	cohort_definition_id,
	person_id,
	drug_concept_id,
	concept_name,
	drug_era_start_date,
	drug_era_end_date,
	duration_in_d,
	days_from_index,
	exposure_ordinal
)
SELECT 
		analysis_id
		, drug_era_id
		, cohort_definition_id
		, person_id
		, drug_concept_id
		, concept_name
		, drug_era_start_date
		, drug_era_end_date
		, duration_in_d
		, days_from_index
		, exposure_ordinal
FROM #txpath_exposures
;


SELECT 
	analysis_id, 
	person_id, 
	drug_era_start_date, 
	drug_era_end_date, 
	exposure_ordinal, 
	COUNT(*) cnt_exposures
INTO #txpath_combos
FROM @resultsSchema.txpath_exposures
WHERE analysis_id = 3
GROUP BY 
	analysis_id, 
	person_id, 
	drug_era_start_date, 
	drug_era_end_date, 
	exposure_ordinal
;

DELETE FROM @resultsSchema.txpath_combos where analysis_id = 3;

INSERT INTO @resultsSchema.txpath_combos (
	analysis_id,
	person_id,
	drug_era_start_date,
	drug_era_end_date,
	exposure_ordinal,
	cnt_exposures
)
SELECT 	
	analysis_id, 
	person_id, 
	drug_era_start_date, 
	drug_era_end_date, 
	exposure_ordinal, 
	cnt_exposures
FROM #txpath_combos
;
SELECT 
	MIN(cnt_exposures) min_cnt,
	MAX(cnt_exposures) max_cnt
FROM @resultsSchema.txpath_combos;-- Utilize the summary of drug combinations found in @resultsSchema.txpath_combos
-- to create a summary of the unique drug by cohort, person and combo of drugs
/*offset:  100000*/

-- Combo #1
WITH comboResolution AS (
	select
		e1.cohort_definition_id,
		e1.person_id,
		cmb.exposure_ordinal,
		cmb.drug_era_start_date,
		cmb.drug_era_end_date,
		e1.concept_name combo_name
	from @resultsSchema.txpath_exposures e1
	
	INNER JOIN @resultsSchema.txpath_combos cmb ON 
				cmb.person_id = e1.person_id
		
		and cmb.exposure_ordinal = e1.exposure_ordinal
		
		and cmb.drug_era_start_date = e1.drug_era_start_date
		and cmb.drug_era_end_date = e1.drug_era_end_date
		
	WHERE cmb.cnt_exposures = 1
	  and e1.analysis_id = 3
), distinctSeq AS (
	SELECT DISTINCT combo_name FROM comboResolution
), seq AS (
	SELECT 
		(1 * 100000) + row_number() over (order by combo_name) as sequence_id,
		combo_name
	FROM 
		distinctSeq
)
SELECT DISTINCT
	seq.sequence_id,
	seq.combo_name,
	cr.cohort_definition_id,
	cr.person_id,
	cr.drug_era_start_date,
	cr.drug_era_end_date,
	cr.exposure_ordinal
INTO #combo_map_1
FROM comboResolution cr
INNER JOIN seq ON seq.combo_name = cr.combo_name
;
-- Utilize the summary of drug combinations found in @resultsSchema.txpath_combos
-- to create a summary of the unique drug by cohort, person and combo of drugs
/*offset:  100000*/

-- Combo #2
WITH comboResolution AS (
	select
		e1.cohort_definition_id,
		e1.person_id,
		cmb.exposure_ordinal,
		cmb.drug_era_start_date,
		cmb.drug_era_end_date,
		CASE
			WHEN (e1.concept_name < e2.concept_name) THEN e1.concept_name + ' / ' + e2.concept_name
			WHEN (e2.concept_name < e1.concept_name) THEN e2.concept_name + ' / ' + e1.concept_name
			ELSE 'combo missing'
		END
		END  combo_name
	from @resultsSchema.txpath_exposures e1
	INNER JOIN @resultsSchema.txpath_exposures e2 ON 
	e2.analysis_id = e1.analysis_id
	and e2.person_id = e1.person_id 
	and e2.drug_era_start_date = e1.drug_era_start_date 
	and e2.drug_era_end_date = e1.drug_era_end_date 
		and e2.drug_concept_id <> e1.drug_concept_id


	INNER JOIN @resultsSchema.txpath_combos cmb ON 
				cmb.person_id = e1.person_id
		 and cmb.person_id = e2.person_id
		and cmb.exposure_ordinal = e1.exposure_ordinal
		 and cmb.exposure_ordinal = e2.exposure_ordinal
		and cmb.drug_era_start_date = e1.drug_era_start_date
		and cmb.drug_era_end_date = e1.drug_era_end_date
		 and cmb.drug_era_start_date = e2.drug_era_start_date
 and cmb.drug_era_end_date = e2.drug_era_end_date
	WHERE cmb.cnt_exposures = 2
	  and e1.analysis_id = 3
), distinctSeq AS (
	SELECT DISTINCT combo_name FROM comboResolution
), seq AS (
	SELECT 
		(2 * 100000) + row_number() over (order by combo_name) as sequence_id,
		combo_name
	FROM 
		distinctSeq
)
SELECT DISTINCT
	seq.sequence_id,
	seq.combo_name,
	cr.cohort_definition_id,
	cr.person_id,
	cr.drug_era_start_date,
	cr.drug_era_end_date,
	cr.exposure_ordinal
INTO #combo_map_2
FROM comboResolution cr
INNER JOIN seq ON seq.combo_name = cr.combo_name
;
-- Utilize the summary of drug combinations found in @resultsSchema.txpath_combos
-- to create a summary of the unique drug by cohort, person and combo of drugs
/*offset:  100000*/

-- Combo #3
WITH comboResolution AS (
	select
		e1.cohort_definition_id,
		e1.person_id,
		cmb.exposure_ordinal,
		cmb.drug_era_start_date,
		cmb.drug_era_end_date,
		CASE
			WHEN (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e2.concept_name < e3.concept_name) THEN e1.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name
			WHEN (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e3.concept_name < e2.concept_name) THEN e1.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name
			WHEN (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e1.concept_name < e2.concept_name) THEN e3.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name
			WHEN (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e2.concept_name < e1.concept_name) THEN e3.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name
			WHEN (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e3.concept_name < e1.concept_name) THEN e2.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name
			WHEN (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e1.concept_name < e3.concept_name) THEN e2.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name
			ELSE 'combo missing'
		END  combo_name
	from @resultsSchema.txpath_exposures e1
	INNER JOIN @resultsSchema.txpath_exposures e2 ON 
	e2.analysis_id = e1.analysis_id
	and e2.person_id = e1.person_id 
	and e2.drug_era_start_date = e1.drug_era_start_date 
	and e2.drug_era_end_date = e1.drug_era_end_date 
		and e2.drug_concept_id <> e1.drug_concept_id

INNER JOIN @resultsSchema.txpath_exposures e3 ON 
	e3.analysis_id = e2.analysis_id
	and e3.person_id = e2.person_id 
	and e3.drug_era_start_date = e2.drug_era_start_date 
	and e3.drug_era_end_date = e2.drug_era_end_date 
		and e3.drug_concept_id <> e2.drug_concept_id
	and e3.drug_concept_id <> e1.drug_concept_id


	INNER JOIN @resultsSchema.txpath_combos cmb ON 
				cmb.person_id = e1.person_id
		 and cmb.person_id = e2.person_id and cmb.person_id = e3.person_id
		and cmb.exposure_ordinal = e1.exposure_ordinal
		 and cmb.exposure_ordinal = e2.exposure_ordinal and cmb.exposure_ordinal = e3.exposure_ordinal
		and cmb.drug_era_start_date = e1.drug_era_start_date
		and cmb.drug_era_end_date = e1.drug_era_end_date
		 and cmb.drug_era_start_date = e2.drug_era_start_date
 and cmb.drug_era_end_date = e2.drug_era_end_date and cmb.drug_era_start_date = e3.drug_era_start_date
 and cmb.drug_era_end_date = e3.drug_era_end_date
	WHERE cmb.cnt_exposures = 3
	  and e1.analysis_id = 3
), distinctSeq AS (
	SELECT DISTINCT combo_name FROM comboResolution
), seq AS (
	SELECT 
		(3 * 100000) + row_number() over (order by combo_name) as sequence_id,
		combo_name
	FROM 
		distinctSeq
)
SELECT DISTINCT
	seq.sequence_id,
	seq.combo_name,
	cr.cohort_definition_id,
	cr.person_id,
	cr.drug_era_start_date,
	cr.drug_era_end_date,
	cr.exposure_ordinal
INTO #combo_map_3
FROM comboResolution cr
INNER JOIN seq ON seq.combo_name = cr.combo_name
;
-- Utilize the summary of drug combinations found in @resultsSchema.txpath_combos
-- to create a summary of the unique drug by cohort, person and combo of drugs
/*offset:  100000*/

-- Combo #4
WITH comboResolution AS (
	select
		e1.cohort_definition_id,
		e1.person_id,
		cmb.exposure_ordinal,
		cmb.drug_era_start_date,
		cmb.drug_era_end_date,
		CASE
			WHEN (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e3.concept_name < e4.concept_name) THEN e1.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name
			WHEN (e1.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e4.concept_name < e3.concept_name) THEN e1.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name
			WHEN (e1.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e2.concept_name < e3.concept_name) THEN e1.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name
			WHEN (e4.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e2.concept_name < e3.concept_name) THEN e4.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name
			WHEN (e4.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e3.concept_name < e2.concept_name) THEN e4.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name
			WHEN (e1.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e3.concept_name < e2.concept_name) THEN e1.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name
			WHEN (e1.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e4.concept_name < e2.concept_name) THEN e1.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name
			WHEN (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e2.concept_name < e4.concept_name) THEN e1.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name
			WHEN (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e2.concept_name < e4.concept_name) THEN e3.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name
			WHEN (e3.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e4.concept_name < e2.concept_name) THEN e3.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name
			WHEN (e3.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e1.concept_name < e2.concept_name) THEN e3.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name
			WHEN (e4.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e1.concept_name < e2.concept_name) THEN e4.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name
			WHEN (e4.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e2.concept_name < e1.concept_name) THEN e4.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name
			WHEN (e3.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e2.concept_name < e1.concept_name) THEN e3.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name
			WHEN (e3.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e4.concept_name < e1.concept_name) THEN e3.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name
			WHEN (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e1.concept_name < e4.concept_name) THEN e3.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name
			WHEN (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e1.concept_name < e4.concept_name) THEN e2.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name
			WHEN (e2.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e4.concept_name < e1.concept_name) THEN e2.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name
			WHEN (e2.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e3.concept_name < e1.concept_name) THEN e2.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name
			WHEN (e4.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e3.concept_name < e1.concept_name) THEN e4.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name
			WHEN (e4.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e1.concept_name < e3.concept_name) THEN e4.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name
			WHEN (e2.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e1.concept_name < e3.concept_name) THEN e2.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name
			WHEN (e2.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e4.concept_name < e3.concept_name) THEN e2.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name
			WHEN (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e3.concept_name < e4.concept_name) THEN e2.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name
			ELSE 'combo missing'
		END  combo_name
	from @resultsSchema.txpath_exposures e1
	INNER JOIN @resultsSchema.txpath_exposures e2 ON 
	e2.analysis_id = e1.analysis_id
	and e2.person_id = e1.person_id 
	and e2.drug_era_start_date = e1.drug_era_start_date 
	and e2.drug_era_end_date = e1.drug_era_end_date 
		and e2.drug_concept_id <> e1.drug_concept_id

INNER JOIN @resultsSchema.txpath_exposures e3 ON 
	e3.analysis_id = e2.analysis_id
	and e3.person_id = e2.person_id 
	and e3.drug_era_start_date = e2.drug_era_start_date 
	and e3.drug_era_end_date = e2.drug_era_end_date 
		and e3.drug_concept_id <> e2.drug_concept_id
	and e3.drug_concept_id <> e1.drug_concept_id

INNER JOIN @resultsSchema.txpath_exposures e4 ON 
	e4.analysis_id = e3.analysis_id
	and e4.person_id = e3.person_id 
	and e4.drug_era_start_date = e3.drug_era_start_date 
	and e4.drug_era_end_date = e3.drug_era_end_date 
		and e4.drug_concept_id <> e3.drug_concept_id
	and e4.drug_concept_id <> e2.drug_concept_id
	and e4.drug_concept_id <> e1.drug_concept_id


	INNER JOIN @resultsSchema.txpath_combos cmb ON 
				cmb.person_id = e1.person_id
		 and cmb.person_id = e2.person_id and cmb.person_id = e3.person_id and cmb.person_id = e4.person_id
		and cmb.exposure_ordinal = e1.exposure_ordinal
		 and cmb.exposure_ordinal = e2.exposure_ordinal and cmb.exposure_ordinal = e3.exposure_ordinal and cmb.exposure_ordinal = e4.exposure_ordinal
		and cmb.drug_era_start_date = e1.drug_era_start_date
		and cmb.drug_era_end_date = e1.drug_era_end_date
		 and cmb.drug_era_start_date = e2.drug_era_start_date
 and cmb.drug_era_end_date = e2.drug_era_end_date and cmb.drug_era_start_date = e3.drug_era_start_date
 and cmb.drug_era_end_date = e3.drug_era_end_date and cmb.drug_era_start_date = e4.drug_era_start_date
 and cmb.drug_era_end_date = e4.drug_era_end_date
	WHERE cmb.cnt_exposures = 4
	  and e1.analysis_id = 3
), distinctSeq AS (
	SELECT DISTINCT combo_name FROM comboResolution
), seq AS (
	SELECT 
		(4 * 100000) + row_number() over (order by combo_name) as sequence_id,
		combo_name
	FROM 
		distinctSeq
)
SELECT DISTINCT
	seq.sequence_id,
	seq.combo_name,
	cr.cohort_definition_id,
	cr.person_id,
	cr.drug_era_start_date,
	cr.drug_era_end_date,
	cr.exposure_ordinal
INTO #combo_map_4
FROM comboResolution cr
INNER JOIN seq ON seq.combo_name = cr.combo_name
;
-- Utilize the summary of drug combinations found in @resultsSchema.txpath_combos
-- to create a summary of the unique drug by cohort, person and combo of drugs
/*offset:  100000*/

-- Combo #5
WITH comboResolution AS (
	select
		e1.cohort_definition_id,
		e1.person_id,
		cmb.exposure_ordinal,
		cmb.drug_era_start_date,
		cmb.drug_era_end_date,
		CASE
			WHEN (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e4.concept_name < e5.concept_name) THEN e1.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name
			WHEN (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e5.concept_name < e4.concept_name) THEN e1.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name
			WHEN (e1.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e3.concept_name < e4.concept_name) THEN e1.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name
			WHEN (e1.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e3.concept_name < e4.concept_name) THEN e1.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name
			WHEN (e5.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e3.concept_name < e4.concept_name) THEN e5.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name
			WHEN (e5.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e4.concept_name < e3.concept_name) THEN e5.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name
			WHEN (e1.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e4.concept_name < e3.concept_name) THEN e1.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name
			WHEN (e1.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e4.concept_name < e3.concept_name) THEN e1.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name
			WHEN (e1.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e5.concept_name < e3.concept_name) THEN e1.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name
			WHEN (e1.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e3.concept_name < e5.concept_name) THEN e1.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name
			WHEN (e1.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e3.concept_name < e5.concept_name) THEN e1.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name
			WHEN (e1.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e5.concept_name < e3.concept_name) THEN e1.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name
			WHEN (e1.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e2.concept_name < e3.concept_name) THEN e1.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name
			WHEN (e1.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e2.concept_name < e3.concept_name) THEN e1.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name
			WHEN (e5.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e2.concept_name < e3.concept_name) THEN e5.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name
			WHEN (e5.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e2.concept_name < e3.concept_name) THEN e5.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name
			WHEN (e4.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e2.concept_name < e3.concept_name) THEN e4.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name
			WHEN (e4.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e2.concept_name < e3.concept_name) THEN e4.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name
			WHEN (e4.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e5.concept_name < e3.concept_name) THEN e4.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name
			WHEN (e4.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e3.concept_name < e5.concept_name) THEN e4.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name
			WHEN (e4.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e2.concept_name < e5.concept_name) THEN e4.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name
			WHEN (e4.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e5.concept_name < e2.concept_name) THEN e4.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name
			WHEN (e4.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e3.concept_name < e2.concept_name) THEN e4.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name
			WHEN (e4.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e3.concept_name < e2.concept_name) THEN e4.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name
			WHEN (e5.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e3.concept_name < e2.concept_name) THEN e5.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name
			WHEN (e5.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e3.concept_name < e2.concept_name) THEN e5.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name
			WHEN (e1.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e3.concept_name < e2.concept_name) THEN e1.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name
			WHEN (e1.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e3.concept_name < e2.concept_name) THEN e1.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name
			WHEN (e1.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e5.concept_name < e2.concept_name) THEN e1.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name
			WHEN (e1.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e2.concept_name < e5.concept_name) THEN e1.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name
			WHEN (e1.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e2.concept_name < e5.concept_name) THEN e1.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name
			WHEN (e1.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e5.concept_name < e2.concept_name) THEN e1.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name
			WHEN (e1.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e4.concept_name < e2.concept_name) THEN e1.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name
			WHEN (e1.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e4.concept_name < e2.concept_name) THEN e1.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name
			WHEN (e5.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e4.concept_name < e2.concept_name) THEN e5.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name
			WHEN (e5.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e2.concept_name < e4.concept_name) THEN e5.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name
			WHEN (e1.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e2.concept_name < e4.concept_name) THEN e1.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name
			WHEN (e1.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e2.concept_name < e4.concept_name) THEN e1.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name
			WHEN (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e5.concept_name < e4.concept_name) THEN e1.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name
			WHEN (e1.concept_name < e3.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e4.concept_name < e5.concept_name) THEN e1.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name
			WHEN (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e4.concept_name < e5.concept_name) THEN e3.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name
			WHEN (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e5.concept_name < e4.concept_name) THEN e3.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name
			WHEN (e3.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e2.concept_name < e4.concept_name) THEN e3.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name
			WHEN (e3.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e2.concept_name < e4.concept_name) THEN e3.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name
			WHEN (e5.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e2.concept_name < e4.concept_name) THEN e5.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name
			WHEN (e5.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e4.concept_name < e2.concept_name) THEN e5.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name
			WHEN (e3.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e4.concept_name < e2.concept_name) THEN e3.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name
			WHEN (e3.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e4.concept_name < e2.concept_name) THEN e3.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name
			WHEN (e3.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e5.concept_name < e2.concept_name) THEN e3.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name
			WHEN (e3.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e2.concept_name < e5.concept_name) THEN e3.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name
			WHEN (e3.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e2.concept_name < e5.concept_name) THEN e3.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name
			WHEN (e3.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e5.concept_name < e2.concept_name) THEN e3.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name
			WHEN (e3.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e1.concept_name < e2.concept_name) THEN e3.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name
			WHEN (e3.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e1.concept_name < e2.concept_name) THEN e3.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name
			WHEN (e5.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e1.concept_name < e2.concept_name) THEN e5.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name
			WHEN (e5.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e1.concept_name < e2.concept_name) THEN e5.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name
			WHEN (e4.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e1.concept_name < e2.concept_name) THEN e4.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name
			WHEN (e4.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e1.concept_name < e2.concept_name) THEN e4.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name
			WHEN (e4.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e5.concept_name < e2.concept_name) THEN e4.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name
			WHEN (e4.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e1.concept_name < e2.concept_name) AND (e1.concept_name < e5.concept_name) AND (e2.concept_name < e5.concept_name) THEN e4.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name
			WHEN (e4.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e1.concept_name < e5.concept_name) THEN e4.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name
			WHEN (e4.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e5.concept_name < e1.concept_name) THEN e4.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name
			WHEN (e4.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e2.concept_name < e1.concept_name) THEN e4.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name
			WHEN (e4.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e2.concept_name < e1.concept_name) THEN e4.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name
			WHEN (e5.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e2.concept_name < e1.concept_name) THEN e5.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name
			WHEN (e5.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e2.concept_name < e1.concept_name) THEN e5.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name
			WHEN (e3.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e2.concept_name < e1.concept_name) THEN e3.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name
			WHEN (e3.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e2.concept_name < e1.concept_name) THEN e3.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name
			WHEN (e3.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e5.concept_name < e1.concept_name) THEN e3.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name
			WHEN (e3.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e1.concept_name < e5.concept_name) THEN e3.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name
			WHEN (e3.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e1.concept_name < e5.concept_name) THEN e3.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name
			WHEN (e3.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e5.concept_name < e1.concept_name) THEN e3.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name
			WHEN (e3.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e4.concept_name < e1.concept_name) THEN e3.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name
			WHEN (e3.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e4.concept_name < e1.concept_name) THEN e3.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name
			WHEN (e5.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e4.concept_name < e1.concept_name) THEN e5.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name
			WHEN (e5.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e1.concept_name < e4.concept_name) THEN e5.concept_name + ' / ' + e3.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name
			WHEN (e3.concept_name < e5.concept_name) AND (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e1.concept_name < e4.concept_name) THEN e3.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name
			WHEN (e3.concept_name < e2.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e1.concept_name < e4.concept_name) THEN e3.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name
			WHEN (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e5.concept_name < e4.concept_name) THEN e3.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name
			WHEN (e3.concept_name < e2.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e4.concept_name < e5.concept_name) THEN e3.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name
			WHEN (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e4.concept_name < e5.concept_name) THEN e2.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name
			WHEN (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e5.concept_name < e4.concept_name) THEN e2.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name
			WHEN (e2.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e1.concept_name < e4.concept_name) THEN e2.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name
			WHEN (e2.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e1.concept_name < e4.concept_name) THEN e2.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name
			WHEN (e5.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e1.concept_name < e4.concept_name) THEN e5.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name
			WHEN (e5.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e4.concept_name < e1.concept_name) THEN e5.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name
			WHEN (e2.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e4.concept_name < e1.concept_name) THEN e2.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name
			WHEN (e2.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e4.concept_name < e1.concept_name) THEN e2.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name
			WHEN (e2.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e5.concept_name < e1.concept_name) THEN e2.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name
			WHEN (e2.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e1.concept_name < e5.concept_name) THEN e2.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name
			WHEN (e2.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e1.concept_name < e5.concept_name) THEN e2.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name
			WHEN (e2.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e5.concept_name < e1.concept_name) THEN e2.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name
			WHEN (e2.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e3.concept_name < e1.concept_name) THEN e2.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name
			WHEN (e2.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e3.concept_name < e1.concept_name) THEN e2.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name
			WHEN (e5.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e3.concept_name < e1.concept_name) THEN e5.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name
			WHEN (e5.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e3.concept_name < e1.concept_name) THEN e5.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name
			WHEN (e4.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e3.concept_name < e1.concept_name) THEN e4.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name
			WHEN (e4.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e3.concept_name < e1.concept_name) THEN e4.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name
			WHEN (e4.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e5.concept_name < e1.concept_name) THEN e4.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name
			WHEN (e4.concept_name < e2.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e3.concept_name < e1.concept_name) AND (e3.concept_name < e5.concept_name) AND (e1.concept_name < e5.concept_name) THEN e4.concept_name + ' / ' + e2.concept_name + ' / ' + e3.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name
			WHEN (e4.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e3.concept_name < e5.concept_name) THEN e4.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name
			WHEN (e4.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e5.concept_name < e3.concept_name) THEN e4.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name
			WHEN (e4.concept_name < e2.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e1.concept_name < e3.concept_name) THEN e4.concept_name + ' / ' + e2.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name
			WHEN (e4.concept_name < e5.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e1.concept_name < e3.concept_name) THEN e4.concept_name + ' / ' + e5.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name
			WHEN (e5.concept_name < e4.concept_name) AND (e5.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e4.concept_name < e2.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e1.concept_name < e3.concept_name) THEN e5.concept_name + ' / ' + e4.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name
			WHEN (e5.concept_name < e2.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e1.concept_name < e3.concept_name) THEN e5.concept_name + ' / ' + e2.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name
			WHEN (e2.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e1.concept_name < e3.concept_name) THEN e2.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name
			WHEN (e2.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e1.concept_name < e3.concept_name) THEN e2.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name
			WHEN (e2.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e5.concept_name < e3.concept_name) THEN e2.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name
			WHEN (e2.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e4.concept_name < e1.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e3.concept_name < e5.concept_name) THEN e2.concept_name + ' / ' + e4.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name
			WHEN (e2.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e3.concept_name < e5.concept_name) THEN e2.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name
			WHEN (e2.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e4.concept_name < e5.concept_name) AND (e4.concept_name < e3.concept_name) AND (e5.concept_name < e3.concept_name) THEN e2.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name
			WHEN (e2.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e4.concept_name < e3.concept_name) THEN e2.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name
			WHEN (e2.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e4.concept_name < e3.concept_name) THEN e2.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name
			WHEN (e5.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e4.concept_name < e3.concept_name) THEN e5.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name + ' / ' + e4.concept_name + ' / ' + e3.concept_name
			WHEN (e5.concept_name < e2.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e3.concept_name < e4.concept_name) THEN e5.concept_name + ' / ' + e2.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name
			WHEN (e2.concept_name < e5.concept_name) AND (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e5.concept_name < e1.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e3.concept_name < e4.concept_name) THEN e2.concept_name + ' / ' + e5.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name
			WHEN (e2.concept_name < e1.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e5.concept_name < e3.concept_name) AND (e5.concept_name < e4.concept_name) AND (e3.concept_name < e4.concept_name) THEN e2.concept_name + ' / ' + e1.concept_name + ' / ' + e5.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name
			WHEN (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e5.concept_name) AND (e2.concept_name < e4.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e5.concept_name) AND (e1.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e5.concept_name < e4.concept_name) THEN e2.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name + ' / ' + e5.concept_name + ' / ' + e4.concept_name
			WHEN (e2.concept_name < e1.concept_name) AND (e2.concept_name < e3.concept_name) AND (e2.concept_name < e4.concept_name) AND (e2.concept_name < e5.concept_name) AND (e1.concept_name < e3.concept_name) AND (e1.concept_name < e4.concept_name) AND (e1.concept_name < e5.concept_name) AND (e3.concept_name < e4.concept_name) AND (e3.concept_name < e5.concept_name) AND (e4.concept_name < e5.concept_name) THEN e2.concept_name + ' / ' + e1.concept_name + ' / ' + e3.concept_name + ' / ' + e4.concept_name + ' / ' + e5.concept_name
			ELSE 'combo missing'
		END  combo_name
	from @resultsSchema.txpath_exposures e1
	INNER JOIN @resultsSchema.txpath_exposures e2 ON 
	e2.analysis_id = e1.analysis_id
	and e2.person_id = e1.person_id 
	and e2.drug_era_start_date = e1.drug_era_start_date 
	and e2.drug_era_end_date = e1.drug_era_end_date 
		and e2.drug_concept_id <> e1.drug_concept_id

INNER JOIN @resultsSchema.txpath_exposures e3 ON 
	e3.analysis_id = e2.analysis_id
	and e3.person_id = e2.person_id 
	and e3.drug_era_start_date = e2.drug_era_start_date 
	and e3.drug_era_end_date = e2.drug_era_end_date 
		and e3.drug_concept_id <> e2.drug_concept_id
	and e3.drug_concept_id <> e1.drug_concept_id

INNER JOIN @resultsSchema.txpath_exposures e4 ON 
	e4.analysis_id = e3.analysis_id
	and e4.person_id = e3.person_id 
	and e4.drug_era_start_date = e3.drug_era_start_date 
	and e4.drug_era_end_date = e3.drug_era_end_date 
		and e4.drug_concept_id <> e3.drug_concept_id
	and e4.drug_concept_id <> e2.drug_concept_id
	and e4.drug_concept_id <> e1.drug_concept_id

INNER JOIN @resultsSchema.txpath_exposures e5 ON 
	e5.analysis_id = e4.analysis_id
	and e5.person_id = e4.person_id 
	and e5.drug_era_start_date = e4.drug_era_start_date 
	and e5.drug_era_end_date = e4.drug_era_end_date 
		and e5.drug_concept_id <> e4.drug_concept_id
	and e5.drug_concept_id <> e3.drug_concept_id
	and e5.drug_concept_id <> e2.drug_concept_id
	and e5.drug_concept_id <> e1.drug_concept_id


	INNER JOIN @resultsSchema.txpath_combos cmb ON 
				cmb.person_id = e1.person_id
		 and cmb.person_id = e2.person_id and cmb.person_id = e3.person_id and cmb.person_id = e4.person_id and cmb.person_id = e5.person_id
		and cmb.exposure_ordinal = e1.exposure_ordinal
		 and cmb.exposure_ordinal = e2.exposure_ordinal and cmb.exposure_ordinal = e3.exposure_ordinal and cmb.exposure_ordinal = e4.exposure_ordinal and cmb.exposure_ordinal = e5.exposure_ordinal
		and cmb.drug_era_start_date = e1.drug_era_start_date
		and cmb.drug_era_end_date = e1.drug_era_end_date
		 and cmb.drug_era_start_date = e2.drug_era_start_date
 and cmb.drug_era_end_date = e2.drug_era_end_date and cmb.drug_era_start_date = e3.drug_era_start_date
 and cmb.drug_era_end_date = e3.drug_era_end_date and cmb.drug_era_start_date = e4.drug_era_start_date
 and cmb.drug_era_end_date = e4.drug_era_end_date and cmb.drug_era_start_date = e5.drug_era_start_date
 and cmb.drug_era_end_date = e5.drug_era_end_date
	WHERE cmb.cnt_exposures = 5
	  and e1.analysis_id = 3
), distinctSeq AS (
	SELECT DISTINCT combo_name FROM comboResolution
), seq AS (
	SELECT 
		(5 * 100000) + row_number() over (order by combo_name) as sequence_id,
		combo_name
	FROM 
		distinctSeq
)
SELECT DISTINCT
	seq.sequence_id,
	seq.combo_name,
	cr.cohort_definition_id,
	cr.person_id,
	cr.drug_era_start_date,
	cr.drug_era_end_date,
	cr.exposure_ordinal
INTO #combo_map_5
FROM comboResolution cr
INNER JOIN seq ON seq.combo_name = cr.combo_name
;
DELETE FROM @resultsSchema.txpath_combos_merged WHERE analysis_id = 3;

INSERT INTO @resultsSchema.txpath_combos_merged (
	analysis_id,
	sequence_id,
	combo_name,
	cohort_definition_id,
	person_id,
	drug_era_start_date,
	drug_era_end_date,
	exposure_ordinal
)
SELECT 3 analysis_id, sequence_id, combo_name, cohort_definition_id, person_id, drug_era_start_date, drug_era_end_date, exposure_ordinal FROM #combo_map_1
UNION ALL
SELECT 3 analysis_id, sequence_id, combo_name, cohort_definition_id, person_id, drug_era_start_date, drug_era_end_date, exposure_ordinal FROM #combo_map_2
UNION ALL
SELECT 3 analysis_id, sequence_id, combo_name, cohort_definition_id, person_id, drug_era_start_date, drug_era_end_date, exposure_ordinal FROM #combo_map_3
UNION ALL
SELECT 3 analysis_id, sequence_id, combo_name, cohort_definition_id, person_id, drug_era_start_date, drug_era_end_date, exposure_ordinal FROM #combo_map_4
UNION ALL
SELECT 3 analysis_id, sequence_id, combo_name, cohort_definition_id, person_id, drug_era_start_date, drug_era_end_date, exposure_ordinal FROM #combo_map_5
;DELETE FROM @resultsSchema.txpath_sequenced 
where analysis_id = 3;

INSERT INTO @resultsSchema.txpath_sequenced (
	analysis_id,
	cohort_definition_id,
	person_id,
	sequence_id,
	exposure_name,
	drug_era_start_date,
	drug_era_end_date,
	duration_in_d,
	days_from_index,
	exposure_ordinal
)
select distinct
	e.analysis_id,
	e.cohort_definition_id,
	e.person_id,
	m.sequence_id,
	m.combo_name exposure_name,
	e.drug_era_start_date,
	e.drug_era_end_date,
	e.duration_in_d,
	e.days_from_index,
	e.exposure_ordinal
from @resultsSchema.txpath_exposures e
inner join @resultsSchema.txpath_combos_merged m ON 
	e.analysis_id = m.analysis_id
	and e.person_id = m.person_id
	and e.cohort_definition_id = m.cohort_definition_id
	and e.exposure_ordinal = m.exposure_ordinal
	and e.drug_era_start_date = m.drug_era_start_date
	and e.drug_era_end_date = m.drug_era_end_date
WHERE e.analysis_id = 3
;
TRUNCATE TABLE #combo_map_1;
DROP TABLE #combo_map_1;

TRUNCATE TABLE #combo_map_2;
DROP TABLE #combo_map_2;

TRUNCATE TABLE #combo_map_3;
DROP TABLE #combo_map_3;

TRUNCATE TABLE #combo_map_4;
DROP TABLE #combo_map_4;

TRUNCATE TABLE #combo_map_5;
DROP TABLE #combo_map_5;


TRUNCATE TABLE #txpath_exposures;
DROP TABLE #txpath_exposures;

TRUNCATE TABLE #txpath_combos;
DROP TABLE #txpath_combos;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;
