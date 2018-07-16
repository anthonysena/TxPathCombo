DELETE FROM @resultsSchema.txpath_sequenced 
where analysis_id = @analysisId;

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
WHERE e.analysis_id = @analysisId
;
