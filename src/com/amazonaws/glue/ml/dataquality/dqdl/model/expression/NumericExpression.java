/*
 * NumericExpression.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.expression;

import java.util.List;

public abstract class NumericExpression<T> extends Expression {
    protected final ExpressionOperator operator;
    protected final List<T> operands;

    public NumericExpression(final String expressionAsString,
                             final ExpressionOperator operator,
                             final List<T> operands) {
        super(expressionAsString);
        this.operator = operator;
        this.operands = operands;
    }

    public abstract Boolean evaluate(T metric);
}
