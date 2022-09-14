/*
 * DQRuleset.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.parser;

import com.amazonaws.glue.ml.dataquality.dqdl.exception.InvalidDataQualityRulesetException;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleset;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class DQRulesetTest {
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
            "Sources = { \"Primary\": \"orders-table\" } " + System.lineSeparator() +
            "Rules = [ IsPrimaryKey \"colA\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);

        String dqdlFormatted =
            "Metadata = {" + LINE_SEP +
                "    \"Version\": \"1.0\"" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "Sources = {" + LINE_SEP +
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
            "Sources = { \"Primary\": \"orders-table\", \"AdditionalSources\": [ \"ref-table\" ] } " + System.lineSeparator() +
            "Rules = [ IsPrimaryKey \"colA\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals("orders-table", dqRuleset.getPrimarySourceName());
        assertEquals(1, dqRuleset.getAdditionalSourcesNames().size());
        assertEquals("ref-table", dqRuleset.getAdditionalSourcesNames().get(0));
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("IsPrimaryKey", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_isPrimaryCheckWithMetadataAndSourcesToString() {
        String dqdl = "Metadata = { \"Version\": \"1.0\" }" + System.lineSeparator() +
            "Sources = { \"Primary\": \"orders-table\", \"AdditionalSources\": [ \"ref-table\" ] } " + System.lineSeparator() +
            "Rules = [ IsPrimaryKey \"colA\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);

        String dqdlFormatted =
            "Metadata = {" + LINE_SEP +
                "    \"Version\": \"1.0\"" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "Sources = {" + LINE_SEP +
                "    \"Primary\": \"orders-table\"," + LINE_SEP +
                "    \"AdditionalSources\": [ \"ref-table\" ]" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "Rules = [" + LINE_SEP +
                "    IsPrimaryKey \"colA\"" + LINE_SEP +
                "]";
        assertEquals(dqdlFormatted, dqRuleset.toString());
    }

    @Test
    void test_isPrimaryCheckWithMetadataAndSourcesAndIncorrectPrimarySourceKey() {
        String dqdl = "Metadata = { \"Version\": \"1.0\" }" + System.lineSeparator() +
            "Sources = { \"PrimaryFoo\": \"orders-table\", \"AdditionalSources\": [ \"ref-table\" ] } " + System.lineSeparator() +
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
            "Sources = { \"Primary\": \"orders-table\", \"AdditionalSourcesFoo\": [ \"ref-table\" ] } " + System.lineSeparator() +
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
            "Sources = { \"AdditionalSources\": [ \"ref-table\" ] } " + System.lineSeparator() +
            "Rules = [ IsPrimaryKey \"colA\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertNull(dqRuleset.getPrimarySourceName());
        assertEquals(1, dqRuleset.getAdditionalSourcesNames().size());
        assertEquals("ref-table", dqRuleset.getAdditionalSourcesNames().get(0));
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("IsPrimaryKey", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_isPrimaryCheckWithMetadataAndSourcesAndNoPrimarySourceToString() {
        String dqdl = "Metadata = { \"Version\": \"1.0\" }" + System.lineSeparator() +
            "Sources = { \"AdditionalSources\": [ \"ref-table\" ] } " + System.lineSeparator() +
            "Rules = [ IsPrimaryKey \"colA\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);

        String dqdlFormatted =
            "Metadata = {" + LINE_SEP +
                "    \"Version\": \"1.0\"" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "Sources = {" + LINE_SEP +
                "    \"AdditionalSources\": [ \"ref-table\" ]" + LINE_SEP +
                "}" + LINE_SEP + LINE_SEP +
                "Rules = [" + LINE_SEP +
                "    IsPrimaryKey \"colA\"" + LINE_SEP +
                "]";
        assertEquals(dqdlFormatted, dqRuleset.toString());
    }

    @Test
    void test_jobStatusRuleWithEqualityCheck() {
        String dqdl = "Rules = [ JobStatus = \"SUCCEEDED\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("JobStatus", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    void test_isPrimaryCheckAndJobStatus() {
        String dqdl = "Rules = [ IsPrimaryKey \"colA\", JobStatus = \"READY\" ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(2, dqRuleset.getRules().size());
        assertEquals("IsPrimaryKey", dqRuleset.getRules().get(0).getRuleType());
        assertEquals("JobStatus", dqRuleset.getRules().get(1).getRuleType());
    }

    @Test
    void test_jobStatusRuleWithSetOfStatus() {
        String dqdl = "Rules = [ JobStatus in [\"SUCCEEDED\", \"READY\" ] ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("JobStatus", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
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

    @Test
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
    void test_columnHasDataTypeWithNumberSetEquality() {
        String dqdl = "Rules = [ (ColumnDataType \"col_1\" in [ 1, 2, 3 ]) ]";
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
        String dqdl = "Rules = [ DatasetColumnsCount >= 100 ]";
        DQRuleset dqRuleset = parseDQDL(dqdl);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("DatasetColumnsCount", dqRuleset.getRules().get(0).getRuleType());
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
        String dqdl = "Rules = [ ColumnValues \"load_dt\" > (now() - 1) ]";
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
    void test_multipleRules() {
        String dqdl =
            "Rules = [" +
            "    Completeness \"col_1\" < 0.5, " +
            "    (Uniqueness \"col_2\" between 0.2 and 0.4) AND (Completeness \"col_2\" > 0.7)," +
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
