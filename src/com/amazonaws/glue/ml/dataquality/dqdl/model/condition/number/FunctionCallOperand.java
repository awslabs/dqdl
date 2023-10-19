/*
 * FunctionCallOperand.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number;

import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

/*
 * A Function Call operand is a special operand that takes operands as parameters returns a number.
 * The parameters can themselves be function call operands, or atomic number operands or binary expression operands.
 * Each function must be implemented by an instance of "OperandEvaluator", provided at the time of evaluation.
 * Through the use of function call operands, we introduce the concept of dynamic rules in DQDL.
 */
@Getter
public class FunctionCallOperand extends NumericOperand {
    private final String functionName;
    private final List<NumericOperand> operands;

    public FunctionCallOperand(final String operand,
                               final String functionName,
                               final List<NumericOperand> operands) {
        super(operand);
        this.functionName = functionName;
        this.operands = operands;
    }

    @Override
    public String toString() {
        String params = this.operands.stream().map(NumericOperand::toString).collect(Collectors.joining(","));
        String formatted = String.format("%s(%s)", this.functionName, params);
        if (this.isParenthesized()) {
            return String.format("(%s)", formatted);
        } else {
            return formatted;
        }
    }
}
