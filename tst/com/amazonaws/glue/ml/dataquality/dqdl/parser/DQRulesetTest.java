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

import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleset;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DQRulesetTest {
    DQDLParser dqdlParser = new DQDLParser();

    @Test
    public void test_isPrimaryCheck() {
        String dqdl = "rules { IsPrimaryKey \"colA\" }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("IsPrimaryKey", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_jobStatusRuleWithEqualityCheck() {
        String dqdl = "rules { JobStatus = \"SUCCEEDED\" }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("JobStatus", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_isPrimaryCheckAndJobStatus() {
        String dqdl = "rules { IsPrimaryKey \"colA\", JobStatus = \"READY\" }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(2, dqRuleset.getRules().size());
        assertEquals("IsPrimaryKey", dqRuleset.getRules().get(0).getRuleType());
        assertEquals("JobStatus", dqRuleset.getRules().get(1).getRuleType());
    }

    @Test
    public void test_jobStatusRuleWithSetOfStatus() {
        String dqdl = "rules { JobStatus in [\"SUCCEEDED\", \"READY\" ] }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("JobStatus", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_jobDurationRule() {
        String dqdl = "rules { JobDuration between 10 and 1000 }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("JobDuration", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_rowCountRule() {
        String dqdl = "rules { RowCount = 100 }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("RowCount", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_fileCountRule() {
        String dqdl = "rules { FileCount between 10 and 100 }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("FileCount", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_completenessRule() {
        String dqdl = "rules { (Completeness \"col_1\" between 0.5 and 0.8) }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("Completeness", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_columnHasDataType() {
        String dqdl = "rules { (ColumnDataType \"col_1\" = \"String\") }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnDataType", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_columnNamesMatchPatternRule() {
        String dqdl = "rules { (ColumnNamesMatchPattern \"aws_.*_[a-zA-Z0-9]+\") }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnNamesMatchPattern", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_columnExistsRule() {
        String dqdl = "rules { (ColumnExists \"load_dt\") }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnExists", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_datasetColumnCountRule() {
        String dqdl = "rules { DatasetColumnsCount >= 100 }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("DatasetColumnsCount", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_columnCorrelationRule() {
        String dqdl = "rules { ColumnCorrelation \"col_1\" \"col_2\" between 0.4 and 0.8 }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnCorrelation", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_isUniqueRule() {
        String dqdl = "rules { (Uniqueness \"col_1\" between 0.1 and 0.2) }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("Uniqueness", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_isPrimaryKey() {
        String dqdl = "rules { (IsPrimaryKey \"col_1\") }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("IsPrimaryKey", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_columnValues() {
        String dqdl = "rules { ColumnValues \"col_1\" between \"2022-06-01\" and \"2022-06-30\" }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnValues", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_dataFreshnessRuleBasedOnColumnValues() {
        String dqdl = "rules { ColumnValues \"load_dt\" > (now() - 1) }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals("ColumnValues", dqRuleset.getRules().get(0).getRuleType());
    }

    @Test
    public void test_multipleRules() {
        String dqdl =
            "rules {" +
            "    Completeness \"col_1\" < 0.5, " +
            "    (Uniqueness \"col_2\" between 0.2 and 0.4) AND (Completeness \"col_2\" > 0.7)," +
            "    RowCount = 100" +
            "}";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(3, dqRuleset.getRules().size());
    }
}
