/*
 * KeywordStringOperand.java
 *
 * Copyright (c) 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class KeywordStringOperand extends StringOperand {
    final Keyword operand;

    public KeywordStringOperand(final Keyword operand) {
        super(operand.toString());
        this.operand = operand;
    }

    @Override
    public String formatOperand() {
        return getOperand().toString();
    }
}
