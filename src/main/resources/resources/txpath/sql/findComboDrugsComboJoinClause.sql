		cmb.person_id = e1.person_id
		@personClauses
		and cmb.exposure_ordinal = e1.exposure_ordinal
		@exposureOrdinalClauses
		and cmb.drug_era_start_date = e1.drug_era_start_date
		and cmb.drug_era_end_date = e1.drug_era_end_date
		@drugEraClauses