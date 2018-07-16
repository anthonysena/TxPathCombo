DELETE FROM @resultsSchema.txpath_combos_merged WHERE analysis_id = @analysisId;

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
@comboMapUnion
;