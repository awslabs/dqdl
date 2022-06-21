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
    public void test_rowCountRule() {
        String dqdl = "rules { RowCount = 100 }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("RowCount", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
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
    public void test_isUniqueRule() {
        String dqdl = "rules { (IsUnique \"col_1\") }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(1, dqRuleset.getRules().size());
        assertEquals(1, dqRuleset.getRules().get(0).getConstraints().size());
        assertEquals("IsUnique", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
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
        assertEquals("IsPrimaryKey", dqRuleset.getRules().get(0).getConstraints().get(0).getConstraintType());
    }

    @Test
    public void test_multipleRules() {
        String dqdl = "rules { IsComplete \"col_1\", (IsUnique \"col_2\") AND (IsComplete \"col_2\"), RowCount = 100 }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(3, dqRuleset.getRules().size());
    }
}
