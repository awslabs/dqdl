/*
 * ConditionTest.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition;

import com.amazonaws.glue.ml.dataquality.dqdl.exception.InvalidDataQualityRulesetException;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.date.DateBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.duration.DurationBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number.NumberBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRule;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleset;
import com.amazonaws.glue.ml.dataquality.dqdl.parser.DQDLParser;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number.NumericOperandTest.testEvaluator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ConditionTest {
    private final DQDLParser updatedParser = new DQDLParser();

    private static Stream<Arguments> provideRulesWithNumberBasedConditions() {
        return Stream.of(
            Arguments.of("Completeness \"colA\" between 0.4 and 0.9", 0.5, true),
            Arguments.of("Completeness \"colA\" between 0.4 and 0.9", 0.4, false),
            Arguments.of("Completeness \"colA\" between 0.4 and 0.9", 0.3, false),
            Arguments.of("Completeness \"colA\" between 0.4 and 0.9", 0.9, false),
            Arguments.of("Completeness \"colA\" between 0.4 and 0.9", 0.91, false),
            Arguments.of("Completeness \"colA\" not between 0.4 and 0.9", 0.5, false),
            Arguments.of("Completeness \"colA\" not between 0.4 and 0.9", 0.4, true),
            Arguments.of("Completeness \"colA\" not between 0.4 and 0.9", 0.3, true),
            Arguments.of("Completeness \"colA\" not between 0.4 and 0.9", 0.9, true),
            Arguments.of("Completeness \"colA\" not between 0.4 and 0.9", 0.91, true),
            Arguments.of("Completeness \"colA\" not in [0.4, 0.9]", 0.91, true),
            Arguments.of("ColumnCorrelation \"colA\" \"colB\" between -0.2 and 1.0", 0.9, true),
            Arguments.of("ColumnCorrelation \"colA\" \"colB\" between -0.2 and 1.0", -0.19, true),
            Arguments.of("ColumnCorrelation \"colA\" \"colB\" between -0.2 and 1.0", -0.2001, false),
            Arguments.of("ColumnCorrelation \"colA\" \"colB\" not between -0.2 and 1.0", -0.2, true),
            Arguments.of("ColumnCorrelation \"colA\" \"colB\" not between -0.2 and 1.0", -0.19, false),
            Arguments.of("ColumnCorrelation \"colA\" \"colB\" not between -0.2 and 1.0", -0.21, true),
            Arguments.of("ColumnCorrelation \"colA\" \"colB\" not in [-0.2, 1.0]", -0.2001, true),
            Arguments.of("ColumnLength \"colA\" in [1, 2, 3]", 4.0, false),
            Arguments.of("ColumnLength \"colA\" not in [1, 2, 3]", 4.0, true),
            Arguments.of("ColumnLength \"colA\" in [1, 2, 3, 4]", 4.0, true),
            Arguments.of("ColumnLength \"colA\" not in [1, 2, 3, 4]", 4.0, false),
            Arguments.of("ColumnValues \"colA\" in [1, 2, 3, 4]", 3.999999, false),
            Arguments.of("ColumnValues \"colA\" in [1, 2, 3, 4]", 3.999, false),
            Arguments.of("Completeness \"colA\" >= 0.4", 0.4, true),
            Arguments.of("Completeness \"colA\" >= 0.4", 0.39, false),
            Arguments.of("Completeness \"colA\" >= 0.4", 1.0, true),
            Arguments.of("Completeness \"colA\" = 1.0", 0.9999999999999999, false),
            Arguments.of("Completeness \"colA\" != 1.0", 0.9999999999999999, true),
            Arguments.of("DatasetMatch \"reference\" \"colA\" not between 0.1 and 1.0", 1.0, true),
            Arguments.of("DistinctValuesCount \"colA\" not between 0.1 and 1.0", 1.0, true),
            Arguments.of("Entropy \"colA\" <= 0.678", 0.679, false),
            Arguments.of("Entropy \"colA\" <= 0.678", 0.677, true),
            Arguments.of("Entropy \"colA\" <= 0.678", -0.1, true),
            Arguments.of("Entropy \"colA\" != 0.678", 0.678, false),
            Arguments.of("Mean \"colA\" != 10.0", 10.0, false),
            Arguments.of("RowCount != 10.0", 10.0, false),
            Arguments.of("Mean \"colA\" != 10.0", 10.0, false),
            Arguments.of("StandardDeviation \"colA\" = 10.0", 10.0, true),
            Arguments.of("StandardDeviation \"colA\" = -10000.0", -10000.0, true),
            Arguments.of("StandardDeviation \"colA\" = 99.34", 99.35, false),
            Arguments.of("StandardDeviation \"colA\" != 10.0", 10.0, false),
            Arguments.of("StandardDeviation \"colA\" != -10000.0", -10000.0, false),
            Arguments.of("StandardDeviation \"colA\" != 99.34", 99.35, true),
            Arguments.of("Sum \"colA\" not in [5.0, 10.0]", 10.0, false),
            Arguments.of("Uniqueness \"colA\" > 0.4", 0.41, true),
            Arguments.of("Uniqueness \"colA\" > 0.4", 0.4, false),
            Arguments.of("Uniqueness \"colA\" > 0.4", -0.4, false),
            Arguments.of("Uniqueness \"colA\" != 0.4", -0.4, true),
            Arguments.of("Uniqueness \"colA\" = 1.0", 0.9999999999999999, false),
            Arguments.of("Uniqueness \"colA\" != 1.0", 0.9999999999999999, true),
            // 16 decimal places is the limit to which Doubles .equals works
            Arguments.of("Uniqueness \"colA\" = 0.9999999999999998", 0.9999999999999999, false),
            Arguments.of("Uniqueness \"colA\" not between 0.1 and 0.5", 0.5, true),
            Arguments.of("Uniqueness \"colA\" not in [0.1, 0.1, 0.5]", 0.3, true),
            Arguments.of("UniqueValueRatio \"colA\" < -0.4", 100.9, false),
            Arguments.of("UniqueValueRatio \"colA\" < -0.4", -0.5, true),
            Arguments.of("UniqueValueRatio \"colA\" < -0.4", -0.41, true),
            Arguments.of("UniqueValueRatio \"colA\" not between -0.5 and -0.4", -0.41, false),
            Arguments.of("UniqueValueRatio \"colA\" not between -0.4 and -0.5", -0.41, true)
        );
    }

    private static Stream<Arguments> provideRulesWithNumberBasedThresholdConditions() {
        return Stream.of(
            Arguments.of("ColumnValues \"colA\" in [ \"A\", \"B\"] with threshold between 0.4 and 0.9", 0.5, true),
            Arguments.of("ColumnValues \"colA\" in [ \"A\", \"B\"] with threshold not between 0.4 and 0.9", 0.5, false),
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
            Arguments.of("ColumnValues \"colA\" matches \"[a-zA-Z]\" with threshold != 0.2", 0.2, false),
            Arguments.of("ColumnValues \"Customer_ID\" in [1,2,3,4,5,6,7,8,9] with threshold > 0.98", 0.979, false)
        );
    }

    private static Stream<Arguments> provideRulesWithDateBasedThresholdConditions() {
        return Stream.of(
            // With static dates
            Arguments.of("ColumnValues \"colA\" in [ \"2022-01-01\", \"2022-12-31\" ]"),
            Arguments.of("ColumnValues \"colA\" not in [ \"2022-01-01\", \"2022-12-31\" ]"),
            Arguments.of("ColumnValues \"colA\" >= \"2022-01-01\" ]"),
            Arguments.of("ColumnValues \"colA\" >  \"2022-01-01\" ]"),
            Arguments.of("ColumnValues \"colA\" <= \"2022-01-01\" ]"),
            Arguments.of("ColumnValues \"colA\" <  \"2022-01-01\" ]"),
            Arguments.of("ColumnValues \"colA\" between \"2022-01-01\" and \"2022-12-31\""),
            Arguments.of("ColumnValues \"colA\" not between \"2022-01-01\" and \"2022-12-31\""),
            // With dynamic expressions
            Arguments.of("ColumnValues \"colA\" in [ (now() - 14 days), (now() - 7 days), \"2022-01-01\" ]"),
            Arguments.of("ColumnValues \"colA\" not in [ (now() - 14 days), (now() - 7 days), \"2022-01-01\" ]"),
            Arguments.of("ColumnValues \"colA\" >= now() ]"),
            Arguments.of("ColumnValues \"colA\" >  (now() - 12 hours) ]"),
            Arguments.of("ColumnValues \"colA\" <= (now() + 3 days) ]"),
            Arguments.of("ColumnValues \"colA\" <  (now() + 72 hours) ]"),
            Arguments.of("ColumnValues \"colA\" between (now() - 14 days) and now()"),
            Arguments.of("ColumnValues \"colA\" not between (now() - 14 days) and now()")
        );
    }

    private static Stream<Arguments> provideRulesWithDurationBasedThresholdConditions() {
        return Stream.of(
            Arguments.of("DataFreshness \"colA\" in [ 3 hours, 12 hours, 1 days ]"),
            Arguments.of("DataFreshness \"colA\" not in [ 3 hours, 12 hours, 1 days ]"),
            Arguments.of("DataFreshness \"colA\" >= 12 hours"),
            Arguments.of("DataFreshness \"colA\" >  2 days"),
            Arguments.of("DataFreshness \"colA\" <= 2 hours"),
            Arguments.of("DataFreshness \"colA\" <  6 hours"),
            Arguments.of("DataFreshness \"colA\" between 6 hours and 12 hours"),
            Arguments.of("DataFreshness \"colA\" not between 6 hours and 12 hours")
        );
    }

    @ParameterizedTest
    @MethodSource("provideRulesWithNumberBasedConditions")
    void test_ruleParsingAndVerifyingNumberBasedCondition(String rule, Double metric, Boolean shouldRulePass) {
        try {
            DQRuleset dqRuleset = updatedParser.parse(String.format("Rules = [ %s ]", rule));
            assertEquals(1, dqRuleset.getRules().size());

            DQRule dqRule = dqRuleset.getRules().get(0);
            assertEquals(NumberBasedCondition.class, dqRule.getCondition().getClass());

            NumberBasedCondition condition = (NumberBasedCondition) dqRule.getCondition();
            assertTrue(dqRule.toString().contains(condition.getFormattedCondition()));
            assertEquals(shouldRulePass, condition.evaluate(metric, dqRule, testEvaluator));
        } catch (InvalidDataQualityRulesetException e) {
            fail(e.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("provideRulesWithNumberBasedThresholdConditions")
    void test_ruleParsingAndVerifyingNumberBasedThresholdCondition(String rule, Double metric, Boolean shouldRulePass) {
        try {
            DQRuleset dqRuleset = updatedParser.parse(String.format("Rules = [ %s ]", rule));
            assertEquals(1, dqRuleset.getRules().size());

            DQRule dqRule = dqRuleset.getRules().get(0);
            assertNotNull(dqRule.getCondition());
            assertEquals(NumberBasedCondition.class, dqRule.getThresholdCondition().getClass());

            NumberBasedCondition thresholdCondition = (NumberBasedCondition) dqRule.getThresholdCondition();
            assertTrue(dqRule.toString().contains(thresholdCondition.getFormattedCondition()));
            assertEquals(shouldRulePass, thresholdCondition.evaluate(metric, dqRule, testEvaluator));
        } catch (InvalidDataQualityRulesetException e) {
            fail(e.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("provideRulesWithDateBasedThresholdConditions")
    void test_ruleParsingAndVerifyingDateBasedCondition(String rule) {
        try {
            DQRuleset dqRuleset = updatedParser.parse(String.format("Rules = [ %s ]", rule));
            assertEquals(1, dqRuleset.getRules().size());

            DQRule dqRule = dqRuleset.getRules().get(0);
            assertNotNull(dqRule.getCondition());
            assertEquals(DateBasedCondition.class, dqRule.getCondition().getClass());

            DateBasedCondition condition = (DateBasedCondition) dqRule.getCondition();
            assertTrue(dqRule.toString().contains(condition.getFormattedCondition()));

            try {
                condition.evaluate(0.0, dqRule, testEvaluator);
                fail("Expected date condition to throw UnsupportedOperationException");
            } catch (UnsupportedOperationException e) {
                // pass
            }

        } catch (InvalidDataQualityRulesetException e) {
            fail(e.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("provideRulesWithDurationBasedThresholdConditions")
    void test_ruleParsingAndVerifyingDurationBasedCondition(String rule) {
        try {
            DQRuleset dqRuleset = updatedParser.parse(String.format("Rules = [ %s ]", rule));
            assertEquals(1, dqRuleset.getRules().size());

            DQRule dqRule = dqRuleset.getRules().get(0);
            assertNotNull(dqRule.getCondition());
            assertEquals(DurationBasedCondition.class, dqRule.getCondition().getClass());

            DurationBasedCondition condition = (DurationBasedCondition) dqRule.getCondition();
            assertTrue(dqRule.toString().contains(condition.getFormattedCondition()));
        } catch (InvalidDataQualityRulesetException e) {
            fail(e.getMessage());
        }
    }
}
