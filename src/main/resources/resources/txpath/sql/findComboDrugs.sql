-- Utilize the summary of drug combinations found in @resultsSchema.txpath_combos
-- to create a summary of the unique drug by cohort, person and combo of drugs
{DEFAULT @offset = 100000} /*offset:  @offset*/

-- Combo #@comboIndex
WITH comboResolution AS (
	select
		e1.cohort_definition_id,
		e1.person_id,
		cmb.exposure_ordinal,
		cmb.drug_era_start_date,
		cmb.drug_era_end_date,
		@comboName combo_name
	from @resultsSchema.txpath_exposures e1
	@exposureJoinClause
	INNER JOIN @resultsSchema.txpath_combos cmb ON 
		@comboJoinClause
	WHERE cmb.cnt_exposures = @comboIndex
	  and e1.analysis_id = @analysisId
), distinctSeq AS (
	SELECT DISTINCT combo_name FROM comboResolution
), seq AS (
	SELECT 
		(@comboIndex * @offset) + row_number() over (order by combo_name) as sequence_id,
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
INTO @comboMapTempTablePrefix_@comboIndex
FROM comboResolution cr
INNER JOIN seq ON seq.combo_name = cr.combo_name
;
