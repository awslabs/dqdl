/*
 * AtomicNumberOperand.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number;

/*
 * Atomic number operands are decimal numbers like 1.0, 3.14 etc that can be used in number based conditions.
 * They are used for defining static thresholds on rules.
 */
public class AtomicNumberOperand extends NumericOperand {
    public AtomicNumberOperand(final String operand) {
        super(operand);
    }

    @Override
    public String toString() {
        if (this.isParenthesized()) {
            return String.format("(%s)", getOperand());
        } else {
            return getOperand();
        }
    }
}
