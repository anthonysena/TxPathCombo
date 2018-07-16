INNER JOIN @resultsSchema.txpath_exposures e@comboIndex ON 
	e@comboIndex.analysis_id = e@previousComboIndex.analysis_id
	and e@comboIndex.person_id = e@previousComboIndex.person_id 
	and e@comboIndex.drug_era_start_date = e@previousComboIndex.drug_era_start_date 
	and e@comboIndex.drug_era_end_date = e@previousComboIndex.drug_era_end_date 
	@drugInequalityClauses
