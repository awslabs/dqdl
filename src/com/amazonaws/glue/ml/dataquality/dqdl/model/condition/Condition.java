/*
 * Expression.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;

@Getter
@EqualsAndHashCode
public class Condition implements Serializable {
    protected final String conditionAsString;

    public Condition(final String conditionAsString) {
        this.conditionAsString = conditionAsString;
    }

    public String getFormattedCondition() {
        return this.conditionAsString;
    }
}
