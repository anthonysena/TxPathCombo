/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ohdsi.webapi.txpath;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.collections4.iterators.PermutationIterator;
import org.ohdsi.circe.helper.ResourceHelper;
import org.ohdsi.circe.cohortdefinition.CohortExpressionQueryBuilder;
import org.ohdsi.circe.cohortdefinition.ConceptSet;
import org.ohdsi.circe.vocabulary.ConceptSetExpression;
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlTranslate;

/**
 *
 * @author asena5
 */
public class TxPathQueryBuilder {

	private final static String CLEANUP_TEMP_TABLES_TEMPALTE = ResourceHelper.GetResourceAsString("/resources/txpath/sql/cleanupTempTables.sql");
	private final static String COMBO_DRUG_MIN_MAX_TEMPLATE = ResourceHelper.GetResourceAsString("/resources/txpath/sql/findComboDrugsMinMax.sql");
	private final static String COMBO_DRUG_TEMPLATE = ResourceHelper.GetResourceAsString("/resources/txpath/sql/findComboDrugs.sql");
	private final static String COMBO_DRUG_EXPOSURE_JOIN_CLAUSE_TEMPLATE = ResourceHelper.GetResourceAsString("/resources/txpath/sql/findComboDrugsExposureJoinClause.sql");
	private final static String COMBO_DRUG_COMBO_JOIN_CLAUSE_TEMPLATE = ResourceHelper.GetResourceAsString("/resources/txpath/sql/findComboDrugsComboJoinClause.sql");
	private final static String COMBO_DRUG_UNION_RESULTS_TEMPLATE = ResourceHelper.GetResourceAsString("/resources/txpath/sql/findComboDrugsUnionResults.sql");
	private final static String CREATE_EXPOSURE_SEQUENCES_TEMPALTE = ResourceHelper.GetResourceAsString("/resources/txpath/sql/createExposureSequences.sql");
	private final static String GET_EXPOSURES_TEMPALTE = ResourceHelper.GetResourceAsString("/resources/txpath/sql/getExposures.sql");
	private final static String COMBO_DRUG_TEMP_TABLE_SELECT = "SELECT @analysisId analysis_id, sequence_id, combo_name, cohort_definition_id, person_id, drug_era_start_date, drug_era_end_date, exposure_ordinal FROM #combo_map_%s";
	
	public static void main(String[] args) {
		String prefix = "e";
		String tableDelimiter = ".";
		String suffix = "concept_name";
		String cdmSchema = "cdmSchema";
		String resultsSchema = "resultsSchema";
		String eventWindowStart = "-30";
		String eventWindowEnd = "30";
		String cohortId = "12345";
		String comboMapTempTablePrefix = "#combo_map";
		int analysisId = 3;
		int comboLength = 5;
		int offset = 100000;
		StringBuilder sbSqlQuery = new StringBuilder();
		
		// Get all of the exposures for the selected cohort
		String jsonForCsExpression = "{\"items\" :[{\"concept\":{\"CONCEPT_ID\":21601782,\"CONCEPT_NAME\":\"AGENTS ACTING ON THE RENIN-ANGIOTENSIN SYSTEM\",\"STANDARD_CONCEPT\":\"C\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"C09\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"ATC\",\"CONCEPT_CLASS_ID\":\"ATC 2nd\",\"STANDARD_CONCEPT_CAPTION\":\"Classification\",\"INVALID_REASON_CAPTION\":\"Valid\"},\"isExcluded\":false,\"includeDescendants\":true,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":21600381,\"CONCEPT_NAME\":\"ANTIHYPERTENSIVES\",\"STANDARD_CONCEPT\":\"C\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"C02\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"ATC\",\"CONCEPT_CLASS_ID\":\"ATC 2nd\",\"STANDARD_CONCEPT_CAPTION\":\"Classification\",\"INVALID_REASON_CAPTION\":\"Valid\"},\"isExcluded\":false,\"includeDescendants\":true,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":21601664,\"CONCEPT_NAME\":\"BETA BLOCKING AGENTS\",\"STANDARD_CONCEPT\":\"C\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"C07\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"ATC\",\"CONCEPT_CLASS_ID\":\"ATC 2nd\",\"STANDARD_CONCEPT_CAPTION\":\"Classification\",\"INVALID_REASON_CAPTION\":\"Valid\"},\"isExcluded\":false,\"includeDescendants\":true,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":21601744,\"CONCEPT_NAME\":\"CALCIUM CHANNEL BLOCKERS\",\"STANDARD_CONCEPT\":\"C\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"C08\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"ATC\",\"CONCEPT_CLASS_ID\":\"ATC 2nd\",\"STANDARD_CONCEPT_CAPTION\":\"Classification\",\"INVALID_REASON_CAPTION\":\"Valid\"},\"isExcluded\":false,\"includeDescendants\":true,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":21601461,\"CONCEPT_NAME\":\"DIURETICS\",\"STANDARD_CONCEPT\":\"C\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"C03\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"ATC\",\"CONCEPT_CLASS_ID\":\"ATC 2nd\",\"STANDARD_CONCEPT_CAPTION\":\"Classification\",\"INVALID_REASON_CAPTION\":\"Valid\"},\"isExcluded\":false,\"includeDescendants\":true,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":21601560,\"CONCEPT_NAME\":\"PERIPHERAL VASODILATORS\",\"STANDARD_CONCEPT\":\"C\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"C04\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"ATC\",\"CONCEPT_CLASS_ID\":\"ATC 2nd\",\"STANDARD_CONCEPT_CAPTION\":\"Classification\",\"INVALID_REASON_CAPTION\":\"Valid\"},\"isExcluded\":false,\"includeDescendants\":true,\"includeMapped\":false}]}";
		ArrayList<ConceptSet> conceptSets = new ArrayList<ConceptSet>();
		ConceptSet cs = new ConceptSet();
		ConceptSetExpression expression = new ConceptSetExpression().fromJson(jsonForCsExpression);
		cs.expression = expression;
		conceptSets.add(cs);
		CohortExpressionQueryBuilder ceqb = new CohortExpressionQueryBuilder();
		String codesetQuery = ceqb.getCodesetQuery(conceptSets.toArray(new ConceptSet[conceptSets.size()]));
		String sql = SqlRender.renderSql(GET_EXPOSURES_TEMPALTE, 
			new String[] {"codesetInserts", "analysisId", "eventWindowStart", "eventWindowEnd"}, 
			new String[] {codesetQuery, String.valueOf(analysisId), eventWindowStart, eventWindowEnd}
		);
		
		sbSqlQuery.append(sql);
		sql = COMBO_DRUG_MIN_MAX_TEMPLATE;
		sbSqlQuery.append(sql);
		
		// Skipping for now :: we'd execute the query above to get the min/max
		// for the combo drugs in the analysis. For now, I'll hard-code this
		// and worry about it when we import the tasklet.
		//StringBuilder sbFindComboDrugs = new StringBuilder();
		ArrayList<Integer> combinationList = new ArrayList<Integer>();
		for (int comboIndex = 1; comboIndex <= comboLength; comboIndex++) {
			// Keep track of the current count of permutations of combinations
			combinationList.add(comboIndex);
			
			String comboName = "";
			String exposureJoinClause = "";
			String comboJoinClause = "";
			try {
				comboName = getComboName(combinationList, prefix, tableDelimiter, suffix);
				exposureJoinClause = getExposureJoinClause(comboIndex, prefix);
				comboJoinClause = getComboJoinClause(comboIndex, prefix);
			} catch (Exception ex) {
				Logger.getLogger(TxPathQueryBuilder.class.getName()).log(Level.SEVERE, null, ex);
			}
			
			// Create the SQL statement
			sql = SqlRender.renderSql(COMBO_DRUG_TEMPLATE, 
				new String[] {"comboMapTempTablePrefix", "offset", "analysisId", "comboIndex", "comboName", "exposureJoinClause", "comboJoinClause"}, 
				new String[] {comboMapTempTablePrefix, String.valueOf(offset), String.valueOf(analysisId), String.valueOf(comboIndex), comboName.toString(), exposureJoinClause, comboJoinClause}
			);
			
			sbSqlQuery.append(sql);
		} // End comboIterator
		
		// Union results
		String comboUnion = "";
		try {
			comboUnion = getComboMapUnionClause(comboLength, analysisId);
		} catch (Exception ex) {
			Logger.getLogger(TxPathQueryBuilder.class.getName()).log(Level.SEVERE, null, ex);
		}
		
		sbSqlQuery.append(comboUnion);
		
		// Sequence the exposures
		sql = SqlRender.renderSql(CREATE_EXPOSURE_SEQUENCES_TEMPALTE, 
			new String[] {"analysisId"}, 
			new String[] {String.valueOf(analysisId)}
		);
		sbSqlQuery.append(sql);
		
		// Create the cleanup script
		StringBuilder sbTableCleanup = new StringBuilder();
		for(Integer i : combinationList) {
			String tableName = comboMapTempTablePrefix + "_" + i.toString();
			sbTableCleanup.append("TRUNCATE TABLE " + tableName + ";\n");
			sbTableCleanup.append("DROP TABLE " + tableName + ";\n\n");
		}
		sql = SqlRender.renderSql(CLEANUP_TEMP_TABLES_TEMPALTE, 
			new String[] {"comboMapTables"}, 
			new String[] {sbTableCleanup.toString()}
		);
		sbSqlQuery.append(sql);

		
		// OHDSI-Sql
		System.out.print(sbSqlQuery.toString());
                try (PrintWriter out = new PrintWriter("compute-sequences.sql")) {
                    out.println(sbSqlQuery.toString());
                } catch (FileNotFoundException ex) {
                Logger.getLogger(TxPathQueryBuilder.class.getName()).log(Level.SEVERE, null, ex);
            }
		
		// Now transform for our environment
//		sql = SqlRender.renderSql(sbSqlQuery.toString(), 
//			new String[] {"vocabulary_database_schema", "cdmSchema", "resultsSchema", "cohortDefinitionIdList"}, 
//			new String[] {cdmSchema, cdmSchema, resultsSchema, cohortId}
//		);
//		
//		sql = SqlTranslate.translateSql(sql, "pdw");
//		System.out.print(sql);
	}
	
	private static String getResultsSchemaTable(String scriptName, String resultsSchema, String dialect) {
		String pathToResultsSqlFiles = "/ddl/results/";
		String sql = ResourceHelper.GetResourceAsString(pathToResultsSqlFiles + scriptName);
		sql = SqlRender.renderSql(sql, 
			new String[] {"results_schema"}, 
			new String[] {resultsSchema}
		);
		return SqlTranslate.translateSql(sql, dialect);
	}
	
	private static String getComboName(ArrayList<Integer> comboList, String prefix, String tableDelmiter, String suffix) throws Exception {
		// Determine the possible permutations for each iteration
		ArrayList<ComboExpression> comboExpressionList = new ArrayList<ComboExpression>();
		PermutationIterator pi = new PermutationIterator(comboList);
		int comparisonCount = 0;
		while (pi.hasNext()) {
			List<Integer> permutationItem = pi.next();
			ArrayIndexComparator comparator = new ArrayIndexComparator(permutationItem);
			Integer[] indexes = comparator.createIndexArray();
			Arrays.sort(indexes, comparator);
			String comparisons = "";
			String textDisplay = "";
			try {
				comparisons = comparisons(permutationItem, prefix, tableDelmiter, suffix);
				textDisplay = createText(permutationItem, indexes, prefix, tableDelmiter, suffix);
			} catch (Exception ex) {
				Logger.getLogger(TxPathQueryBuilder.class.getName()).log(Level.SEVERE, null, ex);
			}

			ComboExpression ce = new ComboExpression();
			ce.comparisons = comparisons;
			ce.textDisplay = textDisplay;
			comboExpressionList.add(ce);

			//System.out.println(Integer.toString(++comparisonCount) + ". PI: " + permutationItem.toString() + "; Sorted Indicies: " + Arrays.toString(indexes));
			//System.out.print(Integer.toString(++comparisonCount) +". Comparisons: " + comparisons);
			//System.out.print(" textDisplay: " + textDisplay);
			System.out.println();
		} // End while(pi.hasNext())

		// Construct the full clause from the comparisonList and textToDisplayList
		StringBuilder comboName = new StringBuilder();
		if (comboList.size() == 1) {
			comboName.append("e1.concept_name");
		} else {
			comboName.append("CASE\n");
			for(ComboExpression ce : comboExpressionList) {
				comboName.append("\t\t\tWHEN " + ce.comparisons + " THEN " + ce.textDisplay + "\n");
			}
			comboName.append("\t\t\tELSE 'combo missing'\n\t\tEND ");
		}
		
		return comboName.toString();
	}
	
	private static String getExposureJoinClause(int comboIndex, String prefix) throws Exception {
		if (comboIndex < 1) {
			throw new Exception("The comboIndex must be greater than 1");
		}
		if (comboIndex == 1) {
			return "";
		}
		
		StringBuilder sbExposureJoinClause = new StringBuilder();
		for (int i = 2; i <= comboIndex; i++) {
			// Create a string representation of all of the inequalities amongst the 
			// tables in the join
			StringBuilder sbDrugInequalityChecks = new StringBuilder();
			int previousComboIndex = i - 1;
			for (int j = (i - 1); j > 0; j--) {
				sbDrugInequalityChecks.append("\tand " + prefix + "@comboIndex.drug_concept_id <> " + prefix + String.valueOf(j) + ".drug_concept_id\n");
			}
			// Create the SQL statement
			String sql = SqlRender.renderSql(COMBO_DRUG_EXPOSURE_JOIN_CLAUSE_TEMPLATE, 
				new String[] {"drugInequalityClauses"}, 
				new String[] {sbDrugInequalityChecks.toString()}
			);
			sql = SqlRender.renderSql(sql, 
				new String[] {"comboIndex", "previousComboIndex"}, 
				new String[] {String.valueOf(i), String.valueOf(previousComboIndex)}
			);
			sbExposureJoinClause.append(sql);
		}
		
		return sbExposureJoinClause.toString();
	}
	
	private static String getComboJoinClause(int comboIndex, String prefix) throws Exception {
		StringBuilder sbPersonClauses = new StringBuilder();
		StringBuilder sbExposureOrdinalClauses = new StringBuilder();
		StringBuilder sbDrugEraClauses = new StringBuilder();
		if (comboIndex < 1) {
			throw new Exception("The comboLength must be greater than 1");
		}
		if (comboIndex > 1) {
			for (int i = 2; i <= comboIndex; i++) {
				String tablePrefix = prefix + String.valueOf(i);
				sbPersonClauses.append(" and cmb.person_id = " + tablePrefix + ".person_id");
				sbExposureOrdinalClauses.append(" and cmb.exposure_ordinal = " + tablePrefix + ".exposure_ordinal");
				sbDrugEraClauses.append(" and cmb.drug_era_start_date = " + tablePrefix + ".drug_era_start_date\n");
				sbDrugEraClauses.append(" and cmb.drug_era_end_date = " + tablePrefix + ".drug_era_end_date");
			}
		} else {
			sbPersonClauses.append("");
			sbExposureOrdinalClauses.append("");
			sbDrugEraClauses.append("");
		}
		// Create a string representation of all of the joins between
		// the persons and ordinals in the exposure tables and the combo table

		// Create the SQL statement
		String sql = SqlRender.renderSql(COMBO_DRUG_COMBO_JOIN_CLAUSE_TEMPLATE, 
			new String[] {"personClauses", "exposureOrdinalClauses", "drugEraClauses"}, 
			new String[] {sbPersonClauses.toString(), sbExposureOrdinalClauses.toString(), sbDrugEraClauses.toString()}
		);
		
		return sql;
	}
	
	private static String getComboMapUnionClause(int comboLength, int analysisId) throws Exception {
		StringBuilder sbComboMapUnion = new StringBuilder();
		if (comboLength < 1) {
			throw new Exception("The comboLength must be greater than 1");
		}
		
		for (int i = 1; i <= comboLength; i++) {
			String query = String.format(COMBO_DRUG_TEMP_TABLE_SELECT, String.valueOf(i));
			sbComboMapUnion.append(query);
			if (i < comboLength) {
				sbComboMapUnion.append("\nUNION ALL\n");
			}
		}

		// Create the SQL statement
		String sql = SqlRender.renderSql(COMBO_DRUG_UNION_RESULTS_TEMPLATE, 
			new String[] {"analysisId", "comboMapUnion"}, 
			new String[] {String.valueOf(analysisId), sbComboMapUnion.toString()}
		);
		
		return sql;
	}
	
	private static String comparisons(List<Integer> listToCompare, String prefix, String tableDelmiter, String suffix) throws Exception {
		StringBuilder sb = new StringBuilder();
		ArrayList<String> clausesList = new ArrayList<String>();
		String op = "";
		
		if (listToCompare.size() < 1) {
			throw new Exception("listToComapare must contain at least 1 element");
		}
		
		if (listToCompare.size() == 1) {
			return prefix + listToCompare.get(0).toString() + tableDelmiter + suffix;
		}
		
		// Build each clause
		for (int i = 0; i < listToCompare.size(); i++) {
			for (int j = (i + 1); j < listToCompare.size(); j++) {
				if (!Objects.equals(listToCompare.get(i), listToCompare.get(j))) {
					op = findOperator(listToCompare.get(i), listToCompare.get(j));
					StringBuilder clause = new StringBuilder();
					clause.append("(");
					clause.append(prefix + listToCompare.get(i).toString() + tableDelmiter + suffix);
					clause.append(op);
					clause.append(prefix + listToCompare.get(j).toString() + tableDelmiter + suffix);
					clause.append(")");
					clausesList.add(clause.toString());
					//clausesList.add("(" + listToCompare.get(comboIndex).toString() + op + listToCompare.get(j).toString() + ")");
				}
			}
		}
		
		// Create a single string representation of each clause
		for (int i = 0; i < clausesList.size(); i++) {
			sb.append(clausesList.get(i));
			if (i >= 0 && i < clausesList.size() - 1) {
				sb.append(" AND ");
			}
		}
		
		return sb.toString();
	}
	
	private static String findOperator(Integer num1, Integer num2) {
                return " < ";
	}
	private static String createText(List<Integer> listToCompare, Integer[] sortedIndices, String prefix, String tableDelmiter, String suffix) throws Exception {
		return createText(listToCompare, sortedIndices, prefix, tableDelmiter, suffix, "/");
	}	
	
	private static String createText(List<Integer> listToCompare, Integer[] sortedIndices, String prefix, String tableDelmiter, String suffix, String nameDelimiter) throws Exception {
		if (listToCompare.size() < 1) {
			throw new Exception("listToCompare must have at least 1 item");
		} else if (!Objects.equals(listToCompare.size(), sortedIndices.length)) {
			throw new Exception("listToCompare must be the same size as sortedInidices");
		}
		
		String[] textForDisplay = new String[listToCompare.size()];
		StringBuilder sb = new StringBuilder();
		
		// Create an array list that is sorted by preserves the original order
		for (int i = 0; i < sortedIndices.length; i++) {
			int index = sortedIndices[i];
			textForDisplay[index] = prefix + Integer.toString(listToCompare.get(index)) + tableDelmiter + suffix;
		}
		
		for (int i = 0; i < textForDisplay.length; i++) {
			sb.append(textForDisplay[i]);
			if (i >= 0 && i < textForDisplay.length - 1) {
				sb.append(" + ' " + nameDelimiter + " ' + ");
			}
		}
		
		return sb.toString();
	}

}
