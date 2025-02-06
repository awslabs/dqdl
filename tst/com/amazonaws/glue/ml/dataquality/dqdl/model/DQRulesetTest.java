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
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.parameter.DQRuleParameterConstantValue;
import com.amazonaws.glue.ml.dataquality.dqdl.model.parameter.DQRuleParameterValue;
import com.amazonaws.glue.ml.dataquality.dqdl.parser.DQDLParser;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

            DQRuleParameterValue paramValue = dqAnalyzers.get(1).getParameterValueMap().get("TargetColumn");
            assertTrue(paramValue instanceof DQRuleParameterConstantValue);
            DQRuleParameterConstantValue constantValue = (DQRuleParameterConstantValue) paramValue;
            assertEquals("col-A", constantValue.getValue());
            assertTrue(constantValue.isQuoted());
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
    void test_uniquenessRule() {
        String dqdl = "Rules = [ (Uniqueness \"col_1\" between 0.1 and 0.2) ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        DQRule uniquenessRule = dqRuleset.getRules().get(0);
        assertEquals("Uniqueness", uniquenessRule.getRuleType());
        assertEquals(uniquenessRule.getParameters().get("TargetColumn"), "col_1");
    }

    @Test
    void test_uniquenessRuleMultipleColumn() {
        String dqdl = "Rules = [ (Uniqueness \"col_1\" \"col_2\" between 0.1 and 0.2) ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        DQRule uniquenessRule = dqRuleset.getRules().get(0);
        assertEquals("Uniqueness", uniquenessRule.getRuleType());
        assertEquals(uniquenessRule.getParameters().get("TargetColumn1"), "col_1");
        assertEquals(uniquenessRule.getParameters().get("TargetColumn2"), "col_2");
    }

    @Test
    void test_isUniqueRule() {
        String dqdl = "Rules = [ (IsUnique \"col_1\") ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        DQRule isUniqueRule = dqRuleset.getRules().get(0);
        assertEquals("IsUnique", isUniqueRule.getRuleType());
        assertEquals(isUniqueRule.getParameters().get("TargetColumn"), "col_1");
    }

    @Test
    void test_isUniqueRuleMultipleColumn() {
        String dqdl = "Rules = [ (IsUnique \"col_1\" \"col_2\") ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        DQRule isUniqueRule = dqRuleset.getRules().get(0);
        assertEquals("IsUnique", isUniqueRule.getRuleType());
        assertEquals(isUniqueRule.getParameters().get("TargetColumn1"), "col_1");
        assertEquals(isUniqueRule.getParameters().get("TargetColumn2"), "col_2");
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
    void testStringVariableResolvedCorrectly() {
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
        assertEquals("in $locationVariable",
                dqRulesetWithVariable.getRules().get(0).getCondition().getSortedFormattedCondition());
        assertEquals("in [\"YYZ14\",\"b\",\"c\"]",
                dqRulesetWithoutVariable.getRules().get(0).getCondition().getSortedFormattedCondition());
    }

    @Test
    void testStringArrayVariable() {
        String dqdl =
                "str_arr = [\"a\", \"b\", \"c\"]\n" +
                        "Rules = [ ColumnValues \"order-id\" in $str_arr ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnValues \"order-id\" in $str_arr",
                dqRuleset.getRules().get(0).toString());
        assertEquals("in $str_arr",
                dqRuleset.getRules().get(0).getCondition().getSortedFormattedCondition());
    }

    @Test
    void testMultipleRulesWithStringArrayVariable() {
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
        assertEquals("in $codes",
                dqRuleset.getRules().get(0).getCondition().getSortedFormattedCondition());
    }

    @Test
    void testStringArrayVariableWithNotIn() {
        String dqdl =
                "invalid_codes = [\"X1\", \"Y2\", \"Z3\"]\n" +
                        "Rules = [ ColumnValues \"product_code\" not in $invalid_codes ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnValues \"product_code\" not in $invalid_codes",
                dqRuleset.getRules().get(0).toString());
        assertEquals("not in $invalid_codes",
                dqRuleset.getRules().get(0).getCondition().getSortedFormattedCondition());
    }

    @Test
    void testUnusedVariable() {
        String dqdl =
                "invalid_codes = [\"X1\", \"Y2\", \"Z3\"]\n" +
                        "Rules = [ ColumnValues \"product_code\" not in [\"A1\", \"B2\", \"C3\"] ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnValues \"product_code\" not in [\"A1\",\"B2\",\"C3\"]",
                dqRuleset.getRules().get(0).toString());
        assertEquals("not in [\"A1\",\"B2\",\"C3\"]",
                dqRuleset.getRules().get(0).getCondition().getSortedFormattedCondition());
    }

    @Test
    void testMultipleVariableDefinitionsOnlyOneUsed() {
        String dqdl =
                "invalid_codes = [\"X1\", \"Y2\", \"Z3\"]\n" +
                "invalid_codes1 = [\"X1\", \"Y2\", \"Z3\"]\n" +
                        "Rules = [ ColumnValues \"product_code\" not in [\"A1\", \"B2\", \"C3\"] ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnValues \"product_code\" not in [\"A1\",\"B2\",\"C3\"]",
                dqRuleset.getRules().get(0).toString());
        assertEquals("not in [\"A1\",\"B2\",\"C3\"]",
                dqRuleset.getRules().get(0).getCondition().getSortedFormattedCondition());
    }

    @Test
    void testVariableDefinitionMissing() {
        String dqdl =
                "invalid_codes = [\"X1\", \"Y2\", \"Z3\"]\n" +
                        "invalid_codes1 = [\"X1\", \"Y2\", \"Z3\"]\n" +
                        "Rules = [ ColumnValues \"product_code\" not in $invalid_codes2 ]";

        try {
            dqdlParser.parse(dqdl);
            fail("InvalidDataQualityRulesetException was expected");
        } catch (InvalidDataQualityRulesetException e) {
            String errorMessage = e.getMessage();
            assertTrue(errorMessage.contains("Variable not found: invalid_codes2"),
                    "Error message should mention the missing variable");
            System.out.println("Caught expected exception: " + errorMessage);
        }
    }

    @Test
    void testStringArrayVariableWithSingleQuotes() {
        String dqdl =
                "str_arr = [\"don't\", \"won't\", \"can't\"]\n" +
                        "Rules = [ ColumnValues \"order-id\" in $str_arr ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());

        DQRule rule = dqRuleset.getRules().get(0);
        List<StringOperand> operands = ((StringBasedCondition) rule.getCondition()).getOperands();
        assertTrue(operands.get(0).formatOperand().contains("don\\'t"));
        assertTrue(operands.get(1).formatOperand().contains("won\\'t"));
        assertTrue(operands.get(2).formatOperand().contains("can\\'t"));
        assertEquals("in $str_arr", rule.getCondition().getSortedFormattedCondition());
    }

    @Test
    void testDirectStringWithSingleQuotes() {
        String dqdl = "Rules = [ ColumnValues \"order-id\" = \"don't\" ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());

        DQRule rule = dqRuleset.getRules().get(0);
        List<StringOperand> operands = ((StringBasedCondition) rule.getCondition()).getOperands();
        assertTrue(operands.get(0).formatOperand().contains("don\\'t"));
        assertEquals("= \"don't\"", rule.getCondition().getSortedFormattedCondition());
    }

    @Test
    void testDirectStringArrayWithSingleQuotes() {
        String dqdl = "Rules = [ ColumnValues \"order-id\" in [\"don't\", \"won't\", \"can't\"] ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());

        DQRule rule = dqRuleset.getRules().get(0);
        List<StringOperand> operands = ((StringBasedCondition) rule.getCondition()).getOperands();
        assertTrue(operands.get(0).formatOperand().contains("don\\'t"));
        assertTrue(operands.get(1).formatOperand().contains("won\\'t"));
        assertTrue(operands.get(2).formatOperand().contains("can\\'t"));
        assertEquals("in [\"can't\",\"don't\",\"won't\"]", rule.getCondition().getSortedFormattedCondition());
    }

    @Test
    void testStringVariable() {
        String dqdl =
                "sqlString = \"select id from primary where age < 100\"\n" +
                        "Rules = [ CustomSql $sqlString ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("select id from primary where age < 100",
                dqRule.getParameters().get("CustomSqlStatement"));
        assertEquals("CustomSql $sqlString", dqRule.toString());
    }

    @Test
    void testStringVariableBetweenAnd() {
        String dqdl =
                "sqlString = \"select id from primary where age < 100\"\n" +
                        "Rules = [ CustomSql $sqlString between 10 and 20 ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("select id from primary where age < 100",
                dqRule.getParameters().get("CustomSqlStatement"));
        assertEquals("CustomSql $sqlString between 10 and 20", dqRule.toString());
    }

    @Test
    void testStringVariableWithThreshold() {
        String dqdl =
                "sqlString = \"select Name from primary where Age > 18\"\n" +
                        "Rules = [ CustomSql $sqlString with threshold  > 3 ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("select Name from primary where Age > 18",
                dqRule.getParameters().get("CustomSqlStatement"));
        assertEquals("CustomSql $sqlString with threshold > 3",dqRule.toString());
    }

    @Test
    void testCustomSqlRuleWithInvalidVariableType() {
        String dqdl =
                "invalidSqlString = [\"X1\", \"Y2\", \"Z3\"]\n" +
                        "Rules = [ CustomSql $invalidSqlString ]";

        try {
            dqdlParser.parse(dqdl);
            fail("Expected InvalidDataQualityRulesetException was not thrown");
        } catch (InvalidDataQualityRulesetException e) {
            String errorMessage = e.getMessage();
            assertTrue(errorMessage.contains("Invalid variable type for 'invalidSqlString'"),
                    "Error message should mention the invalid variable type");
            assertTrue(errorMessage.contains("expected STRING"),
                    "Error message should mention the expected type");
            System.out.println("Caught expected exception: " + errorMessage);
        }
    }

    @Test
    void testVariableReuse() {
        String dqdl =
                "sqlString = \"select id from primary where age < 100\"\n" +
                        "Rules = [ \n" +
                        "    CustomSql $sqlString, " +
                        "    CustomSql $sqlString" +
                        "]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        DQRule dqRule1 = dqRuleset.getRules().get(0);
        DQRule dqRule2 = dqRuleset.getRules().get(1);
        assertEquals(2, dqRuleset.getRules().size());
        assertEquals("select id from primary where age < 100",
                dqRule1.getParameters().get("CustomSqlStatement"));
        assertEquals("select id from primary where age < 100",
                dqRule2.getParameters().get("CustomSqlStatement"));
        assertEquals("CustomSql $sqlString", dqRule1.toString());
        assertEquals("CustomSql $sqlString", dqRule2.toString());
    }

    @Test
    void testVariableOverriding() {
        String dqdl =
                "var = \"first value\"\n" +
                        "var = \"second value\"\n" +
                        "Rules = [ CustomSql $var ]";

        assertThrows(InvalidDataQualityRulesetException.class, () -> dqdlParser.parse(dqdl),
                "Should throw exception for variable redefinition");
    }

    @Test
    void testNestedVariables() {
        String dqdl =
                "inner_var = \"inner\"\n" +
                        "outer_var = \"outer $inner_var\"\n" +
                        "Rules = [ CustomSql $outer_var ]";

        assertThrows(InvalidDataQualityRulesetException.class, () -> dqdlParser.parse(dqdl),
                "Should throw exception for nested variables");
    }

    @Test
    void testVariableWithEmptyValue() {
        String dqdl =
                "empty_var = \"\"\n" +
                        "Rules = [ CustomSql $empty_var ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("", dqRule.getParameters().get("CustomSqlStatement"));
        assertEquals("CustomSql $empty_var", dqRule.toString());
    }

    @Test
    void testVariableInNonCustomSqlRule() throws InvalidDataQualityRulesetException {
        String dqdl =
                "column_name = \"age\"\n" +
                        "Rules = [ Mean $column_name > 3 ]";

        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("age", dqRule.getParameters().get("TargetColumn"));
        assertEquals("Mean $column_name > 3", dqRule.toString());
    }

    @Test
    void testNoStringInterpolation() {
        String dqdl =
                "id = \"ZZZ\"\n" +
                        "Rules = [ CustomSql \"select $id from primary where age < 100\" ]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("select $id from primary where age < 100",
                dqRule.getParameters().get("CustomSqlStatement"));
        assertEquals("CustomSql \"select $id from primary where age < 100\"", dqRule.toString());
    }

    @Test
    void testMeanRuleWithVariableInColumnName() {
        String dqdl =
                "column_name = \"product_rating\"\n" +
                        "Rules = [\n" +
                        "    Mean \"xy$column_name\" < 20\n" +
                        "]";

        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals("xy$column_name", dqRule.getParameters().get("TargetColumn"));
        assertEquals("Mean \"xy$column_name\" < 20", dqRule.toString());
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
