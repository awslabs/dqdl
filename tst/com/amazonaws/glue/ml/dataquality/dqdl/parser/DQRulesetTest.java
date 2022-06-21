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
    public void test_jobStatusRuleWithEqualityCheck() {
        String dqdl = "rules { JobStatus = \"SUCCEEDED\" }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("JobStatus", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_jobStatusRuleWithSetOfStatus() {
        String dqdl = "rules { JobStatus in [\"SUCCEEDED\", \"READY\" ] }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("JobStatus", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_jobDurationRule() {
        String dqdl = "rules { JobDuration between 10 and 1000 }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("JobDuration", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_rowCountRule() {
        String dqdl = "rules { RowCount = 100 }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("RowCount", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_fileCountRule() {
        String dqdl = "rules { FileCount between 10 and 100 }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("FileCount", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_isCompleteRule() {
        String dqdl = "rules { (IsComplete \"col_1\") }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("IsComplete", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_columnHasDataType() {
        String dqdl = "rules { (ColumnHasDataType \"col_1\" \"String\") }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("ColumnHasDataType", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_columnNamesMatchPatternRule() {
        String dqdl = "rules { (ColumnNamesMatchPattern 'aws_.*_[a-zA-Z0-9]+') }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("ColumnNamesMatchPattern", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_columnExistsRule() {
        String dqdl = "rules { (ColumnExists \"load_dt\") }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("ColumnExists", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_datasetColumnCountRule() {
        String dqdl = "rules { DatasetColumnCount >= 100 }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("DatasetColumnCount", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_columnCorrelationRule() {
        String dqdl = "rules { ColumnCorrelation \"col_1\" \"col_2\" between 0.4 and 0.8 }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("ColumnCorrelation", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_isUniqueRule() {
        String dqdl = "rules { (IsUnique \"col_1\") }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("IsUnique", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_isPrimaryKey() {
        String dqdl = "rules { (IsPrimaryKey \"col_1\") }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("IsPrimaryKey", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_columnValues() {
        String dqdl = "rules { ColumnValues \"col_1\" between \"2022-06-01\" and \"2022-06-30\" }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("ColumnValues", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_dataFreshnessRuleBasedOnColumnValues() {
        String dqdl = "rules { ColumnValues \"load_dt\" > (now() - 1) }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("ColumnValues", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_multipleRules() {
        String dqdl = "rules { IsComplete \"col_1\", (IsUnique \"col_2\") AND (IsComplete \"col_2\"), RowCount = 100 }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(3, dqRuleset.getRules().size());
    }
}
