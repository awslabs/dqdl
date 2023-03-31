/*
 * DoubleNumericExpression.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.expression;

import java.util.List;

public class DoubleNumericExpression extends NumericExpression<Double> {
    public DoubleNumericExpression(final String expressionAsString,
                                   final ExpressionOperator operator,
                                   final List<Double> operands) {
        super(expressionAsString, operator, operands);
    }

    @Override
    public Boolean evaluate(Double metric) {
        if (operands == null) return false;

        switch (operator) {
            case BETWEEN:
                if (operands.size() != 2) return false;
                else return metric > operands.get(0) && metric < operands.get(1);
            case GREATER_THAN_EQUAL_TO:
                if (operands.size() != 1) return false;
                else return metric >= operands.get(0);
            case GREATER_THAN:
                if (operands.size() != 1) return false;
                else return metric > operands.get(0);
            case LESS_THAN_EQUAL_TO:
                if (operands.size() != 1) return false;
                else return metric <= operands.get(0);
            case LESS_THAN:
                if (operands.size() != 1) return false;
                else return metric < operands.get(0);
            case EQUALS:
                if (operands.size() != 1) return false;
                else return metric.equals(operands.get(0));
            default:
                return false;
        }
    }
}
