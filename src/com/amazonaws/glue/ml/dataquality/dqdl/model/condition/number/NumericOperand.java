/*
 * NumericOperand.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number;

import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRule;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

@AllArgsConstructor
@Getter
public abstract class NumericOperand implements Serializable {
    private final String operand;
    private final boolean isParenthesized;

    public NumericOperand(final String operand) {
        this.operand = operand;
        isParenthesized = false;
    }

    public abstract Double evaluate(DQRule dqRule, OperandEvaluator evaluator);
}
