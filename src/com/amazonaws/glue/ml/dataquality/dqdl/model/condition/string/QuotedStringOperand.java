/*
 * QuotedStringOperand.java
 *
 * Copyright (c) 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string;

public class QuotedStringOperand extends StringOperand {
    public QuotedStringOperand(final String operand) {
        super(operand);
    }

    @Override
    public String formatOperand() {
        return "\"" + getOperand() + "\"";
    }
}
