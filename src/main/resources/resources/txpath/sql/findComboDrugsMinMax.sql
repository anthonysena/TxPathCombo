SELECT 
	MIN(cnt_exposures) min_cnt,
	MAX(cnt_exposures) max_cnt
FROM @resultsSchema.txpath_combos;