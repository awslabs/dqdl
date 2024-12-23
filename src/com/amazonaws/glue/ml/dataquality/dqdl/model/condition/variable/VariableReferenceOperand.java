/*
 * VariableReferenceOperand.java
 *
 * Copyright (c) 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.variable;

import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringOperand;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = true)
public class VariableReferenceOperand extends StringOperand {
    private final String variableName;

    public VariableReferenceOperand(String variableName) {
        super(variableName);
        this.variableName = variableName;
    }

    @Override
    public String formatOperand() {
        return "$" + variableName;
    }
}
