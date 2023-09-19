/*
 * BinaryExpressionOperand.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number;

import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRule;

/*
 * A BinaryExpressionOperand is a numerical expression that consists of two operands and an operator.
 * The operands can themselves be binary expression operands or atomic number operands or function call operands.
 * The operator can be one of: +, -, /, *
 * The purpose of this operand is for combining with a dynamic function call operand to create dynamic rule thresholds.
 */
public class BinaryExpressionOperand extends NumericOperand {
    private final String operator;
    private final NumericOperand operand1;
    private final NumericOperand operand2;

    public BinaryExpressionOperand(final String operand,
                                   final String operator,
                                   final NumericOperand operand1,
                                   final NumericOperand operand2,
                                   final boolean isParenthesized) {
        super(operand, isParenthesized);
        this.operator = operator;
        this.operand1 = operand1;
        this.operand2 = operand2;
    }

    public Double evaluate(DQRule dqRule, OperandEvaluator evaluator) {
        Double operand1Evaluated = operand1.evaluate(dqRule, evaluator);
        Double operand2Evaluated = operand2.evaluate(dqRule, evaluator);

        switch (operator) {
            case "+":
                return operand1Evaluated + operand2Evaluated;
            case "-":
                return operand1Evaluated - operand2Evaluated;
            case "/":
                return operand1Evaluated / operand2Evaluated;
            case "*":
                return operand1Evaluated * operand2Evaluated;
            default:
                throw new IllegalArgumentException("Bad operator");
        }
    }

    @Override
    public String toString() {
        String formatted = String.format("%s %s %s",
            this.operand1.toString(), this.operator, this.operand2.toString());
        if (this.isParenthesized()) {
            return String.format("(%s)", formatted);
        } else {
            return formatted;
        }
    }
}
