/*
 * ExpressionTest.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.expression;

import com.amazonaws.glue.ml.dataquality.dqdl.exception.InvalidDataQualityRulesetException;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRule;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleset;
import com.amazonaws.glue.ml.dataquality.dqdl.parser.DQDLParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public class ExpressionTest {
    private final DQDLParser parser = new DQDLParser();

    private static Stream<Arguments> provideRulesWithNumericConditions() {
        return Stream.of(
            Arguments.of("Completeness \"colA\" between 0.4 and 0.9", 0.5, true),
            Arguments.of("Completeness \"colA\" between 0.4 and 0.9", 0.3, false),
            Arguments.of("Completeness \"colA\" between 0.4 and 0.9", 0.91, false),
            Arguments.of("ColumnCorrelation \"colA\" \"colB\" between -0.2 and 1.0", 0.9, true),
            Arguments.of("ColumnCorrelation \"colA\" \"colB\" between -0.2 and 1.0", -0.19, true),
            Arguments.of("ColumnCorrelation \"colA\" \"colB\" between -0.2 and 1.0", -0.2001, false),
            Arguments.of("Completeness \"colA\" >= 0.4", 0.4, true),
            Arguments.of("Completeness \"colA\" >= 0.4", 0.39, false),
            Arguments.of("Completeness \"colA\" >= 0.4", 1.0, true),
            Arguments.of("Uniqueness \"colA\" > 0.4", 0.41, true),
            Arguments.of("Uniqueness \"colA\" > 0.4", 0.4, false),
            Arguments.of("Uniqueness \"colA\" > 0.4", -0.4, false),
            Arguments.of("UniqueValueRatio \"colA\" < -0.4", 100.9, false),
            Arguments.of("UniqueValueRatio \"colA\" < -0.4", -0.5, true),
            Arguments.of("UniqueValueRatio \"colA\" < -0.4", -0.41, true),
            Arguments.of("Entropy \"colA\" <= 0.678", 0.679, false),
            Arguments.of("Entropy \"colA\" <= 0.678", 0.677, true),
            Arguments.of("Entropy \"colA\" <= 0.678", -0.1, true),
            Arguments.of("StandardDeviation \"colA\" = 10.0", 10.0, true),
            Arguments.of("StandardDeviation \"colA\" = -10000.0", -10000.0, true),
            Arguments.of("StandardDeviation \"colA\" = 99.34", 99.35, false)
        );
    }

    private static Stream<Arguments> provideRulesWithNumericThresholdConditions() {
        return Stream.of(
            Arguments.of("ColumnValues \"colA\" in [ \"A\", \"B\"] with threshold between 0.4 and 0.9", 0.5, true),
            Arguments.of("ColumnValues \"colA\" in [ \"A\", \"B\"] with threshold > 0.6", 0.59, false),
            Arguments.of("ColumnValues \"colA\" in [ \"A\", \"B\"] with threshold >= 0.5", 0.5, true),
            Arguments.of("ColumnValues \"colA\" in [ \"A\", \"B\"] with threshold < 0.333", 0.334, false),
            Arguments.of("ColumnValues \"colA\" in [ \"A\", \"B\"] with threshold <= 0.333", 0.3, true),
            Arguments.of("ColumnValues \"colA\" in [ \"A\", \"B\"] with threshold = 0.2", 0.2, true),
            Arguments.of("ColumnValues \"colA\" matches \"[a-zA-Z]\" with threshold between 0.4 and 0.9", 0.5, true),
            Arguments.of("ColumnValues \"colA\" matches \"[a-zA-Z]\" with threshold > 0.6", 0.59, false),
            Arguments.of("ColumnValues \"colA\" matches \"[a-zA-Z]\" with threshold >= 0.5", 0.5, true),
            Arguments.of("ColumnValues \"colA\" matches \"[a-zA-Z]\" with threshold < 0.333", 0.332, true),
            Arguments.of("ColumnValues \"colA\" matches \"[a-zA-Z]\" with threshold <= 0.333", 0.3, true),
            Arguments.of("ColumnValues \"colA\" matches \"[a-zA-Z]\" with threshold = 0.2", 0.2, true),
            Arguments.of("ColumnValues \"Customer_ID\" in [1,2,3,4,5,6,7,8,9] with threshold > 0.98", 0.979, false)
        );
    }

    @ParameterizedTest
    @MethodSource("provideRulesWithNumericConditions")
    void test_ruleParsingAndVerifyingConditionExpression(String rule, Double metric, Boolean shouldRulePass) {
        try {
            DQRuleset dqRuleset = parser.parse(String.format("Rules = [ %s ]", rule));
            assertEquals(1, dqRuleset.getRules().size());

            DQRule dqRule = dqRuleset.getRules().get(0);
            assertEquals(DoubleNumericExpression.class, dqRule.getExpression().getClass());

            DoubleNumericExpression expression = (DoubleNumericExpression) dqRule.getExpression();
            assertEquals(shouldRulePass, expression.evaluate(metric));
        } catch (InvalidDataQualityRulesetException e) {
            fail(e.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("provideRulesWithNumericThresholdConditions")
    void test_ruleParsingAndVerifyingThresholdConditionExpression(String rule, Double metric, Boolean shouldRulePass) {
        try {
            DQRuleset dqRuleset = parser.parse(String.format("Rules = [ %s ]", rule));
            assertEquals(1, dqRuleset.getRules().size());

            DQRule dqRule = dqRuleset.getRules().get(0);
            assertNotNull(dqRule.getExpression());
            assertEquals(DoubleNumericExpression.class, dqRule.getThreshold().getClass());
            DoubleNumericExpression expression = (DoubleNumericExpression) dqRule.getThreshold();
            assertEquals(shouldRulePass, expression.evaluate(metric));
        } catch (InvalidDataQualityRulesetException e) {
            fail(e.getMessage());
        }
    }
}
