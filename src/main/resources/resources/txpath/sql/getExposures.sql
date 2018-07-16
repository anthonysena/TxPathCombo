@codesetInserts

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
        and de.drug_era_start_date >= DATEADD(dd, @eventWindowStart, ic.cohort_start_date) 
        and de.drug_era_start_date <= DATEADD(dd, @eventWindowEnd, ic.cohort_start_date)  
	JOIN #Codesets cs ON cs.concept_id = de.drug_concept_id
	JOIN @cdmSchema.CONCEPT c ON cs.concept_id = c.concept_id
), 
allExposure AS (
	SELECT 
		@analysisId analysis_id
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

DELETE FROM @resultsSchema.txpath_exposures where analysis_id = @analysisId;

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
WHERE analysis_id = @analysisId
GROUP BY 
	analysis_id, 
	person_id, 
	drug_era_start_date, 
	drug_era_end_date, 
	exposure_ordinal
;

DELETE FROM @resultsSchema.txpath_combos where analysis_id = @analysisId;

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
