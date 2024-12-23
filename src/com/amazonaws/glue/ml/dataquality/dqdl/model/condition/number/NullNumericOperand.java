/*
 * NullNumericOperand.java
 *
 * Copyright (c) 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number;

public class NullNumericOperand extends NumericOperand {

    public NullNumericOperand(final String operand) {
        super(operand.toUpperCase());
    }

    @Override
    public String toString() {
        return getOperand();
    }
}
