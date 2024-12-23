/*
 * DQRulesetTest.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import com.amazonaws.glue.ml.dataquality.dqdl.exception.InvalidDataQualityRulesetException;
import com.amazonaws.glue.ml.dataquality.dqdl.parser.DQDLParser;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

// This is a copy of the original DQRulesetTest.
// Once the original parser is no longer being used, we should remove all associated tests.
public class DQRulesetTest {
    private static final String LINE_SEP = System.lineSeparator();
    private final DQDLParser dqdlParser = new DQDLParser();

    @Test
    void test_isPrimaryCheck() {
        String dqdl = "Rules = [ IsPrimaryKey \"colA\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("IsPrimaryKey", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_isPrimaryCheckWithMetadataAndNoSources() {
        String dqdl = "Metadata = { \"Version\": \"1.0\" } Rules = [ IsPrimaryKey \"colA\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("IsPrimaryKey", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_isPrimaryCheckWithMetadataAndNoSourcesToString() {
        String dqdl = "Metadata = { \"Version\": \"1.0\" } Rules = [ IsPrimaryKey \"colA\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);

        String dqdlFormatted =
            "Metadata = {" + LINE_SEP +
                "    \"Version\": \"1.0\"" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "Rules = [" + LINE_SEP +
                "    IsPrimaryKey \"colA\"" + LINE_SEP +
                "]";
        assertEquals(dqdlFormatted, dqRuleset.toString());
    }

    @Test
    void test_isPrimaryCheckWithMetadataAndNoSourcesAndIncorrectKey() {
        String dqdl = "Metadata = { \"VersionFoo\": \"1.0\" } Rules = [ IsPrimaryKey \"colA\" ]";
        try {
            dqdlParser.parse(dqdl);
            fail("Ruleset validation exception was expected");
        } catch (InvalidDataQualityRulesetException e) {
            assertTrue(e.getMessage().contains("Unsupported key"));
        }
    }

    @Test
    void test_isPrimaryCheckWithMetadataAndSourcesAndNoAdditionalSourcesToString() {
        String dqdl = "Metadata = { \"Version\": \"1.0\" }" + System.lineSeparator() +
            "DataSources = { \"Primary\": \"orders-table\" } " + System.lineSeparator() +
            "Rules = [ IsPrimaryKey \"colA\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);

        String dqdlFormatted =
            "Metadata = {" + LINE_SEP +
                "    \"Version\": \"1.0\"" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "DataSources = {" + LINE_SEP +
                "    \"Primary\": \"orders-table\"" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "Rules = [" + LINE_SEP +
                "    IsPrimaryKey \"colA\"" + LINE_SEP +
                "]";
        assertEquals(dqdlFormatted, dqRuleset.toString());
    }

    @Test
    void test_isPrimaryCheckWithMetadataAndSources() {
        String dqdl = "Metadata = { \"Version\": \"1.0\" }" + System.lineSeparator() +
            "DataSources = { \"Primary\": \"orders-table\", \"AdditionalDataSources\": [ \"ref-table\" ] } " + System.lineSeparator() +
            "Rules = [ IsPrimaryKey \"colA\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals("orders-table", dqRuleset.getPrimarySourceName());
        assertEquals(1, dqRuleset.getAdditionalDataSourcesNames().size());
        assertEquals("ref-table", dqRuleset.getAdditionalDataSourcesNames().get(0));
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("IsPrimaryKey", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_isPrimaryCheckWithMetadataAndSourcesToString() {
        String dqdl = "Metadata = { \"Version\": \"1.0\" }" + System.lineSeparator() +
            "DataSources = { \"Primary\": \"orders-table\", \"AdditionalDataSources\": [ \"ref-table\" ] } " + System.lineSeparator() +
            "Rules = [ IsPrimaryKey \"colA\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);

        String dqdlFormatted =
            "Metadata = {" + LINE_SEP +
                "    \"Version\": \"1.0\"" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "DataSources = {" + LINE_SEP +
                "    \"Primary\": \"orders-table\"," + LINE_SEP +
                "    \"AdditionalDataSources\": [ \"ref-table\" ]" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "Rules = [" + LINE_SEP +
                "    IsPrimaryKey \"colA\"" + LINE_SEP +
                "]";
        assertEquals(dqdlFormatted, dqRuleset.toString());
    }

    @Test
    void test_isPrimaryCheckWithMetadataAndSourcesAndIncorrectPrimarySourceKey() {
        String dqdl = "Metadata = { \"Version\": \"1.0\" }" + System.lineSeparator() +
            "DataSources = { \"PrimaryFoo\": \"orders-table\", \"AdditionalDataSources\": [ \"ref-table\" ] } " + System.lineSeparator() +
            "Rules = [ IsPrimaryKey \"colA\" ]";
        try {
            dqdlParser.parse(dqdl);
            fail("Ruleset validation exception was expected");
        } catch (InvalidDataQualityRulesetException e) {
            assertTrue(e.getMessage().contains("Unsupported key"));
        }
    }

    @Test
    void test_isPrimaryCheckWithMetadataAndSourcesAndIncorrectAdditionalSourcesKey() {
        String dqdl = "Metadata = { \"Version\": \"1.0\" }" + System.lineSeparator() +
            "DataSources = { \"Primary\": \"orders-table\", \"AdditionalDataSourcesFoo\": [ \"ref-table\" ] } " + System.lineSeparator() +
            "Rules = [ IsPrimaryKey \"colA\" ]";
        try {
            dqdlParser.parse(dqdl);
            fail("Ruleset validation exception was expected");
        } catch (InvalidDataQualityRulesetException e) {
            assertTrue(e.getMessage().contains("Unsupported key"));
        }
    }

    @Test
    void test_isPrimaryCheckWithMetadataAndSourcesAndNoPrimarySource() {
        String dqdl = "Metadata = { \"Version\": \"1.0\" }" + System.lineSeparator() +
            "DataSources = { \"AdditionalDataSources\": [ \"ref-table\" ] } " + System.lineSeparator() +
            "Rules = [ IsPrimaryKey \"colA\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertNull(dqRuleset.getPrimarySourceName());
        assertEquals(1, dqRuleset.getAdditionalDataSourcesNames().size());
        assertEquals("ref-table", dqRuleset.getAdditionalDataSourcesNames().get(0));
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("IsPrimaryKey", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_isPrimaryCheckWithMetadataAndSourcesAndNoPrimarySourceToString() {
        String dqdl = "Metadata = { \"Version\": \"1.0\" }" + System.lineSeparator() +
            "DataSources = { \"AdditionalDataSources\": [ \"ref-table\" ] } " + System.lineSeparator() +
            "Rules = [ IsPrimaryKey \"colA\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);

        String dqdlFormatted =
            "Metadata = {" + LINE_SEP +
                "    \"Version\": \"1.0\"" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "DataSources = {" + LINE_SEP +
                "    \"AdditionalDataSources\": [ \"ref-table\" ]" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "Rules = [" + LINE_SEP +
                "    IsPrimaryKey \"colA\"" + LINE_SEP +
                "]";
        assertEquals(dqdlFormatted, dqRuleset.toString());
    }

    @Test
    void test_isPrimaryCheckWithMetadataAndSourcesAndAnalyzers() {
        String dqdl = "Metadata = { \"Version\": \"1.0\" }" + LINE_SEP +
            "DataSources = { \"Primary\": \"orders-table\", \"AdditionalDataSources\": [ \"ref-table\" ] } " + LINE_SEP +
            "Rules = [ IsPrimaryKey \"colA\" ] " + LINE_SEP +
            "Analyzers = [ Completeness \"colA\" ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals("orders-table", dqRuleset.getPrimarySourceName());
        assertEquals(1, dqRuleset.getAdditionalDataSourcesNames().size());
        assertEquals("ref-table", dqRuleset.getAdditionalDataSourcesNames().get(0));
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("IsPrimaryKey", dqRuleset.getRules().get(0).getRuleType());
        assertEquals(1, dqRuleset.getAnalyzers().size());
        assertEquals("Completeness", dqRuleset.getAnalyzers().get(0).getRuleType());

        String dqdlFormatted =
                "Metadata = {" + LINE_SEP +
                "    \"Version\": \"1.0\"" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "DataSources = {" + LINE_SEP +
                "    \"Primary\": \"orders-table\"," + LINE_SEP +
                "    \"AdditionalDataSources\": [ \"ref-table\" ]" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "Rules = [" + LINE_SEP +
                "    IsPrimaryKey \"colA\"" + LINE_SEP +
                "]" + LINE_SEP + LINE_SEP +
                "Analyzers = [" + LINE_SEP +
                "    Completeness \"colA\"" + LINE_SEP +
                "]";
        assertEquals(dqdlFormatted, dqRuleset.toString());
    }

    @Test
    void test_rulesetWithAnalyzersAndEmptyOrMissingRules() {
        String dqdl1 = "Analyzers = [ RowCount, Completeness of \"col-A\" ]";
        String dqdl2 = "Rules = [] Analyzers = [ RowCount, Completeness of \"col-A\" ]";

        Arrays.asList(dqdl1, dqdl2).forEach(dqdl -> {
            DQRuleset ruleset = parseDQDL(dqdl);
            List<DQRule> dqRules = ruleset.getRules();
            List<DQAnalyzer> dqAnalyzers = ruleset.getAnalyzers();

            assertEquals(0, dqRules.size());
            assertEquals(2, dqAnalyzers.size());
            assertEquals("RowCount", dqAnalyzers.get(0).getRuleType());
            assertEquals(0, dqAnalyzers.get(0).getParameterValueMap().size());
            assertEquals("Completeness", dqAnalyzers.get(1).getRuleType());
            assertTrue(dqAnalyzers.get(1).getParameterValueMap().containsKey("TargetColumn"));
            assertEquals("col-A", dqAnalyzers.get(1).getParameterValueMap().get("TargetColumn").getValue());
        });
    }

    @Test
    void test_rulesetWithEmptyAnalyzersAndEmptyRules() {
        Arrays.asList(
            "Rules = []",
            "Analyzers = []",
            "Rules = [] Analyzers = []"
        ).forEach(ruleset -> {
            try {
                dqdlParser.parse(ruleset);
                fail("Ruleset parsing should have failed");
            } catch (InvalidDataQualityRulesetException e) {
                System.out.println(e.getMessage());
                assertTrue(e.getMessage().contains("No rules or analyzers provided"));
            }
        });
    }

    @Test
    void test_rulesetWithMetadataAndSourcesAndAnalyzersAndNoRules() {
        String dqdl =
                "Metadata = { \"Version\": \"1.0\" }" + LINE_SEP +
                "DataSources = {" +
                "    \"Primary\": \"orders-table\", " + LINE_SEP +
                "    \"AdditionalDataSources\": [ \"ref-table\" ]" + LINE_SEP +
                "}" + LINE_SEP +
                "Analyzers = [ RowCount, Completeness \"colA\", Uniqueness of col_A ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals("orders-table", dqRuleset.getPrimarySourceName());
        assertEquals(1, dqRuleset.getAdditionalDataSourcesNames().size());
        assertEquals("ref-table", dqRuleset.getAdditionalDataSourcesNames().get(0));
        assertEquals(0, dqRuleset.getRules().size());
        assertEquals(3, dqRuleset.getAnalyzers().size());
        assertEquals("RowCount", dqRuleset.getAnalyzers().get(0).getRuleType());
        assertEquals("Completeness", dqRuleset.getAnalyzers().get(1).getRuleType());
        assertEquals("Uniqueness", dqRuleset.getAnalyzers().get(2).getRuleType());

        String dqdlFormatted =
                "Metadata = {" + LINE_SEP +
                "    \"Version\": \"1.0\"" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "DataSources = {" + LINE_SEP +
                "    \"Primary\": \"orders-table\"," + LINE_SEP +
                "    \"AdditionalDataSources\": [ \"ref-table\" ]" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "Analyzers = [" + LINE_SEP +
                "    RowCount," + LINE_SEP +
                "    Completeness \"colA\"," + LINE_SEP +
                "    Uniqueness of col_A" + LINE_SEP +
                "]";
        assertEquals(dqdlFormatted, dqRuleset.toString());
    }

    @Test
    void test_rulesetWithAnalyzersAndNoRules() {
        String dqdl = "Analyzers = [ Completeness \"colA\", AllStatistics of AllColumns, Uniqueness of \"col_A\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);

        List<DQAnalyzer> analyzers = dqRuleset.getAnalyzers();
        assertEquals(3, analyzers.size());
        assertEquals("Completeness", analyzers.get(0).getRuleType());
        assertEquals("AllStatistics", analyzers.get(1).getRuleType());
        assertEquals("Uniqueness", analyzers.get(2).getRuleType());

        String dqdlFormatted =
                "Analyzers = [" + LINE_SEP +
                "    Completeness \"colA\"," + LINE_SEP +
                "    AllStatistics of AllColumns," + LINE_SEP +
                "    Uniqueness of \"col_A\"" + LINE_SEP +
                "]";
        assertEquals(dqdlFormatted, dqRuleset.toString());
    }

    @Disabled
    void test_jobStatusRuleWithEqualityCheck() {
        String dqdl = "Rules = [ JobStatus = \"SUCCEEDED\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("JobStatus", dqRuleset.getRules().get(0).getRuleType());
    }

    @Disabled
    void test_isPrimaryCheckAndJobStatus() {
        String dqdl = "Rules = [ IsPrimaryKey \"colA\", JobStatus = \"READY\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(2, dqRuleset.getRules().size());
        assertEquals("IsPrimaryKey", dqRuleset.getRules().get(0).getRuleType());
        assertEquals("JobStatus", dqRuleset.getRules().get(1).getRuleType());
    }

    @Disabled
    void test_jobStatusRuleWithSetOfStatus() {
        String dqdl = "Rules = [ JobStatus in [\"SUCCEEDED\", \"READY\" ] ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("JobStatus", dqRuleset.getRules().get(0).getRuleType());
    }

    @Disabled
    void test_jobDurationRule() {
        String dqdl = "Rules = [ JobDuration between 10 and 1000 ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("JobDuration", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_rowCountRule() {
        String dqdl = "Rules = [ RowCount = 100 ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("RowCount", dqRuleset.getRules().get(0).getRuleType());
    }

    @Disabled
    void test_fileCountRule() {
        String dqdl = "Rules = [ FileCount between 10 and 100 ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("FileCount", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_completenessRule() {
        String dqdl = "Rules = [ (Completeness \"col_1\" between 0.5 and 0.8) ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("Completeness", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_columnHasDataType() {
        String dqdl = "Rules = [ (ColumnDataType \"col_1\" = \"String\") ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnDataType", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_columnHasDataTypeWithStringSetEquality() {
        String dqdl = "Rules = [ (ColumnDataType \"col_1\" in [ \"String\", \"Boolean\" ]) ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnDataType", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_columnNamesMatchPatternRule() {
        String dqdl = "Rules = [ (ColumnNamesMatchPattern \"aws_.*_[a-zA-Z0-9]+\") ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnNamesMatchPattern", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_columnExistsRule() {
        String dqdl = "Rules = [ (ColumnExists \"load_dt\") ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnExists", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_datasetColumnCountRule() {
        String dqdl = "Rules = [ ColumnCount >= 100 ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnCount", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_columnCorrelationRule() {
        String dqdl = "Rules = [ ColumnCorrelation \"col_1\" \"col_2\" between 0.4 and 0.8 ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnCorrelation", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_isUniqueRule() {
        String dqdl = "Rules = [ (Uniqueness \"col_1\" between 0.1 and 0.2) ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("Uniqueness", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_columnValues() {
        String dqdl = "Rules = [ ColumnValues \"col_1\" between \"2022-06-01\" and \"2022-06-30\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnValues", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_dataFreshnessRuleBasedOnColumnValues() {
        String dqdl = "Rules = [ ColumnValues \"load_dt\" > (now() - 2 days) ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnValues", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_invalidRulesetThrowsException() {
        String dqdl = "Rules11 = [ ColumnValues \"load_dt\" > (now() - 1) ]";
        try {
            dqdlParser.parse(dqdl);
            fail("Ruleset validation exception was expected");
        } catch (InvalidDataQualityRulesetException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void testStringVariableResolvedCorrectly() {
        String dqdlWithVariable =
                "locationVariable = [\"YYZ14\", \"b\", \"c\"]\n" +
                        "Rules = [ ColumnValues \"Location-id\" in $locationVariable ]";
        String dqdlWithoutVariable = "Rules = [ ColumnValues \"Location-id\" in [\"YYZ14\", \"b\", \"c\"] ]";
        String ruleWithVariable = "ColumnValues \"Location-id\" in $locationVariable";
        String ruleWithoutVariable = "ColumnValues \"Location-id\" in [\"YYZ14\",\"b\",\"c\"]";
        String rulesWithVariable = "Rules = [\n    ColumnValues \"Location-id\" in $locationVariable\n]";
        String rulesWithoutVariable = "Rules = [\n    ColumnValues \"Location-id\" in [\"YYZ14\",\"b\",\"c\"]\n]";

        DQRuleset dqRulesetWithVariable = parseDQDL(dqdlWithVariable);
        DQRuleset dqRulesetWithoutVariable = parseDQDL(dqdlWithoutVariable);
        assertEquals(rulesWithVariable, dqRulesetWithVariable.toString());
        assertEquals(rulesWithoutVariable, dqRulesetWithoutVariable.toString());
        assertEquals(dqRulesetWithoutVariable.getRules().size(), dqRulesetWithVariable.getRules().size());
        assertEquals(ruleWithVariable,
                dqRulesetWithVariable.getRules().get(0).toString());
        assertEquals(ruleWithoutVariable,
                dqRulesetWithoutVariable.getRules().get(0).toString());
    }

    @Test
    public void testStringArrayVariable() {
        String dqdl =
                "str_arr = [\"a\", \"b\", \"c\"]\n" +
                        "Rules = [ ColumnValues \"order-id\" in $str_arr ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnValues \"order-id\" in $str_arr",
                dqRuleset.getRules().get(0).toString());
    }

    @Test
    public void testMultipleRulesWithStringArrayVariable() {
        String dqdl =
                "codes = [\"A1\", \"B2\", \"C3\"]\n" +
                        "statuses = [\"active\", \"pending\", \"inactive\"]\n" +
                        "Rules = [\n" +
                        "    ColumnValues \"product_code\" in $codes,\n" +
                        "    ColumnValues \"status\" in $statuses\n" +
                        "]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(2, dqRuleset.getRules().size());
        assertEquals("ColumnValues \"product_code\" in $codes",
                dqRuleset.getRules().get(0).toString());
        assertEquals("ColumnValues \"status\" in $statuses",
                dqRuleset.getRules().get(1).toString());
    }

    @Test
    public void testStringArrayVariableWithNotIn() {
        String dqdl =
                "invalid_codes = [\"X1\", \"Y2\", \"Z3\"]\n" +
                        "Rules = [ ColumnValues \"product_code\" not in $invalid_codes ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnValues \"product_code\" not in $invalid_codes",
                dqRuleset.getRules().get(0).toString());
    }

    @Test
    public void testUnusedVariable() {
        String dqdl =
                "invalid_codes = [\"X1\", \"Y2\", \"Z3\"]\n" +
                        "Rules = [ ColumnValues \"product_code\" not in [\"A1\", \"B2\", \"C3\"] ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnValues \"product_code\" not in [\"A1\",\"B2\",\"C3\"]",
                dqRuleset.getRules().get(0).toString());
    }

    @Test
    public void testMultipleVariableDefinitionsOnlyOneUsed() {
        String dqdl =
                "invalid_codes = [\"X1\", \"Y2\", \"Z3\"]\n" +
                "invalid_codes1 = [\"X1\", \"Y2\", \"Z3\"]\n" +
                        "Rules = [ ColumnValues \"product_code\" not in [\"A1\", \"B2\", \"C3\"] ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnValues \"product_code\" not in [\"A1\",\"B2\",\"C3\"]",
                dqRuleset.getRules().get(0).toString());
    }

    @Test
    public void testVariableDefinitionMissing() {
        String dqdl =
                "invalid_codes = [\"X1\", \"Y2\", \"Z3\"]\n" +
                        "invalid_codes1 = [\"X1\", \"Y2\", \"Z3\"]\n" +
                        "Rules = [ ColumnValues \"product_code\" not in $invalid_codes2 ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnValues \"product_code\" not in $invalid_codes2",
                dqRuleset.getRules().get(0).toString());
    }

    @Test
    public void testStringVariable() {
        String dqdl =
                "sqlString = \"select id from primary where age < 100\"\n" +
                        "Rules = [ CustomSql \"select id from primary where age < 100\" ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("CustomSql \"select id from primary where age < 100\"",
                dqRuleset.getRules().get(0).toString());
    }

    @Test
    void test_multipleRules() {
        String dqdl =
            "Rules = [" +
                "    Completeness \"col_1\" < 0.5, " +
                "    (Uniqueness \"col_2\" between 0.2 and 0.4) and (Completeness \"col_2\" > 0.7)," +
                "    RowCount = 100" +
                "]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        System.out.println(dqRuleset);
        assertEquals(3, dqRuleset.getRules().size());
    }

    private DQRuleset parseDQDL(String dqdl) {
        DQRuleset dqRuleset = null;
        try {
            dqRuleset = dqdlParser.parse(dqdl);
        } catch (InvalidDataQualityRulesetException e) {
            fail("Unable to parse DQDL rule set: " + e.getMessage());
        }

        return dqRuleset;
    }
}
