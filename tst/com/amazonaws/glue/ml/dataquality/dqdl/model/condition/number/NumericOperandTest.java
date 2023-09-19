/*
 * NumericOperandTest.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number;

import com.amazonaws.glue.ml.dataquality.dqdl.exception.InvalidDataQualityRulesetException;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRule;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleset;
import com.amazonaws.glue.ml.dataquality.dqdl.parser.DQDLParser;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NumericOperandTest {
    private static final String MULTIPLY_FUNCTION_NAME = "multiply";
    private static final String AVG_FUNCTION_NAME = "avg";

    private final DQDLParser parser = new DQDLParser();
    private final OperandEvaluator evaluator = new OperandEvaluator() {
        @Override
        public Double evaluate(DQRule rule, String functionName, List<Double> operands) {
            if (MULTIPLY_FUNCTION_NAME.equals(functionName)) {
                return operands.stream().reduce(1.0, (a, b) -> a * b);
            } else if (AVG_FUNCTION_NAME.equals(functionName)) {
                return operands.stream().reduce(0.0, (a, b) -> a + b) / operands.size();
            } else {
                throw new RuntimeException("Function not supported");
            }
        }
    };

    @Test
    public void test_functionCallWorksWithAtomicNumberOperands() throws InvalidDataQualityRulesetException {
        String rule = "RowCount = multiply(1,2,3)";
        DQRuleset ruleset = parser.parse(String.format("Rules = [ %s ]", rule));

        assertNotNull(ruleset);
        assertEquals(1, ruleset.getRules().size());
        DQRule dqRule = ruleset.getRules().get(0);

        assertEquals(NumberBasedCondition.class, dqRule.getCondition().getClass());
        NumberBasedCondition condition = (NumberBasedCondition) dqRule.getCondition();

        assertTrue(condition.evaluate(6.0, dqRule, evaluator));
        assertFalse(condition.evaluate(3.0, dqRule, evaluator));
    }

    @Test
    public void test_functionCallWorksWithNestedFunctionCallOperands() throws InvalidDataQualityRulesetException {
        String rule = "RowCount = multiply(avg(2,4), avg(10,20))";
        DQRuleset ruleset = parser.parse(String.format("Rules = [ %s ]", rule));

        assertNotNull(ruleset);
        assertEquals(1, ruleset.getRules().size());

        DQRule dqRule = ruleset.getRules().get(0);

        assertEquals(NumberBasedCondition.class, dqRule.getCondition().getClass());

        NumberBasedCondition condition = (NumberBasedCondition) dqRule.getCondition();
        assertTrue(condition.evaluate(45.0, dqRule, evaluator));
        assertFalse(condition.evaluate(40.0, dqRule, evaluator));
    }

    @Test
    public void test_functionCallWorksInBinaryExpression() throws InvalidDataQualityRulesetException {
        String rule = "RowCount = 2.0 * multiply(avg(2,4), avg(10,20))";
        DQRuleset ruleset = parser.parse(String.format("Rules = [ %s ]", rule));

        assertNotNull(ruleset);
        assertEquals(1, ruleset.getRules().size());

        DQRule dqRule = ruleset.getRules().get(0);

        assertEquals(NumberBasedCondition.class, dqRule.getCondition().getClass());

        NumberBasedCondition condition = (NumberBasedCondition) dqRule.getCondition();
        assertTrue(condition.evaluate(90.0, dqRule, evaluator));
        assertFalse(condition.evaluate(45.0, dqRule, evaluator));
    }

    @Test
    public void test_functionCallWorksInSimpleBinaryExpression() throws InvalidDataQualityRulesetException {
        String rule = "RowCount = 8.0 * (5.0 - (1.0 + (4.0 / 2.0)))";
        DQRuleset ruleset = parser.parse(String.format("Rules = [ %s ]", rule));

        assertNotNull(ruleset);
        assertEquals(1, ruleset.getRules().size());

        DQRule dqRule = ruleset.getRules().get(0);

        assertEquals(NumberBasedCondition.class, dqRule.getCondition().getClass());

        NumberBasedCondition condition = (NumberBasedCondition) dqRule.getCondition();
        assertTrue(condition.evaluate(16.0, dqRule, evaluator));
        assertFalse(condition.evaluate(8.0, dqRule, evaluator));
    }
}
