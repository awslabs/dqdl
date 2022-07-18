/*
 * DQRuleTest.java
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/*
 * The purpose of this test is to ensure that parsing a rule and
 * converting it back to a string yields the original raw rule.
 */
class DQRuleTest {
    DQDLParser parser = new DQDLParser();

    @ParameterizedTest
    @MethodSource("provideRawRules")
    void test_ruleParsingAndGenerating(String rule) {
        try {
            DQRuleset dqRuleset = parser.parse(String.format("Rules = [ %s ]", rule));
            assertEquals(1, dqRuleset.getRules().size());
            DQRule dqRule = dqRuleset.getRules().get(0);
            String dqRuleAsString = dqRule.toString();
            assertEquals(rule, dqRuleAsString);
        } catch (InvalidDataQualityRulesetException e) {
            fail(e.getMessage());
        }
    }

    private static Stream<Arguments> provideRawRules() {
        return Stream.of(
            Arguments.of("IsPrimaryKey \"colA\""),
            Arguments.of("JobStatus = \"SUCCEEDED\""),
            Arguments.of("JobStatus in [\"SUCCEEDED\",\"READY\"]"),
            Arguments.of("JobDuration between 10 and 1000"),
            Arguments.of("JobDuration between -10 and 1000"),
            Arguments.of("RowCount = 100"),
            Arguments.of("RowCount = -100"),
            Arguments.of("FileCount between 10 and 100"),
            Arguments.of("FileCount between -10000 and -1000"),
            Arguments.of("Completeness \"col_1\" between 0.5 and 0.8"),
            Arguments.of("IsComplete \"col_1\""),
            Arguments.of("Completeness \"col_1\" between -0.5 and -0.4"),
            Arguments.of("ColumnDataType \"col_1\" = \"String\""),
            Arguments.of("ColumnNamesMatchPattern \"aws_.*_[a-zA-Z0-9]+\""),
            Arguments.of("ColumnExists \"load_dt\""),
            Arguments.of("DatasetColumnsCount >= 100"),
            Arguments.of("DatasetColumnsCount > -100.123456"),
            Arguments.of("ColumnCorrelation \"col_1\" \"col_2\" between 0.4 and 0.8"),
            Arguments.of("ColumnCorrelation \"col_1\" \"col_2\" between -0.44444 and 0.888888"),
            Arguments.of("Uniqueness \"col_1\" between 0.1 and 0.2"),
            Arguments.of("IsUnique \"col_1\""),
            Arguments.of("Uniqueness \"col_1\" between -0.00000001 and 0.00000000000002"),
            Arguments.of("ColumnValues \"col_1\" between \"2022-06-01\" and \"2022-06-30\""),
            Arguments.of("ColumnValues \"load_dt\" > (now()-1)"),
            Arguments.of("Sum \"col_A-B.C\" > 100.0"),
            Arguments.of("Sum \"col_A-B.C\" > -100.0"),
            Arguments.of("Mean \"col_A-B.CD\" between 10 and 20"),
            Arguments.of("Mean \"col_A-B.CD\" between -20 and -10"),
            Arguments.of("StandardDeviation \"col_A-B.CD\" <= 10.0"),
            Arguments.of("StandardDeviation \"col_A-B.CD\" <= -10000.0")
        );
    }

    @Test
    void test_toStringIgnoresSpacesOnlyThreshold() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("TargetColumn", "\"colA\"");
        String whitespaceThreshold = "     ";
        DQRule dqRule = new DQRule(
            "IsPrimaryKey", parameters, whitespaceThreshold,
            DQRuleLogicalOperator.AND, Collections.emptyList()
        );
        String dqRuleAsString = dqRule.toString();
        assertEquals("IsPrimaryKey \"colA\"", dqRuleAsString);
    }

    @Test
    void test_toStringIgnoresTabsOnlyThreshold() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("TargetColumn", "\"colA\"");
        String whitespaceThreshold = "\t\t";
        DQRule dqRule = new DQRule(
            "IsPrimaryKey", parameters, whitespaceThreshold,
            DQRuleLogicalOperator.AND, Collections.emptyList()
        );
        String dqRuleAsString = dqRule.toString();
        assertEquals("IsPrimaryKey \"colA\"", dqRuleAsString);
    }

    @Test
    void test_toStringIgnoresMixedWhitespaceThreshold() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("TargetColumn", "\"colA\"");
        String whitespaceThreshold = "\t\t  \t \t \n   \t";
        DQRule dqRule = new DQRule(
            "IsPrimaryKey", parameters, whitespaceThreshold,
            DQRuleLogicalOperator.AND, Collections.emptyList()
        );
        String dqRuleAsString = dqRule.toString();
        assertEquals("IsPrimaryKey \"colA\"", dqRuleAsString);
    }

    @Test
    void test_nullParametersAreCorrectlyHandled() {
        Map<String, String> parameters = null;
        String threshold = "=100";
        DQRule dqRule = new DQRule(
            "JobDuration", parameters, threshold,
            DQRuleLogicalOperator.AND, Collections.emptyList()
        );
        String dqRuleAsString = dqRule.toString();
        assertEquals("JobDuration = 100", dqRuleAsString);
    }

    @Test
    void test_nullNestedRulesAreCorrectlyHandled() {
        Map<String, String> parameters = null;
        String threshold = "=100";
        List<DQRule> nestedRules = null;
        DQRule dqRule = new DQRule(
            "JobDuration", parameters, threshold,
            DQRuleLogicalOperator.AND, nestedRules
        );
        String dqRuleAsString = dqRule.toString();
        assertEquals("JobDuration = 100", dqRuleAsString);
    }

    @Test
    void test_uppercaseBetweenAndLowercaseAndProducesCorrectString() {
        Map<String, String> parameters = null;
        String threshold = "BETWEEN10and20";
        List<DQRule> nestedRules = null;
        DQRule dqRule = new DQRule(
            "JobDuration", parameters, threshold,
            DQRuleLogicalOperator.AND, nestedRules
        );
        String dqRuleAsString = dqRule.toString();
        assertEquals("JobDuration between 10 and 20", dqRuleAsString);
    }

    @Test
    void test_lowercaseBetweenAndUppercaseAndProducesCorrectString() {
        Map<String, String> parameters = null;
        String threshold = "between10AND20";
        List<DQRule> nestedRules = null;
        DQRule dqRule = new DQRule(
            "JobDuration", parameters, threshold,
            DQRuleLogicalOperator.AND, nestedRules
        );
        String dqRuleAsString = dqRule.toString();
        assertEquals("JobDuration between 10 and 20", dqRuleAsString);
    }
}
