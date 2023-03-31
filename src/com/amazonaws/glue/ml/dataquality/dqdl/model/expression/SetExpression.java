/*
 * SetExpression.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.expression;

import lombok.Getter;

import java.util.List;

@Getter
public abstract class SetExpression<T> extends Expression {
    private final List<T> items;

    public SetExpression(final String expressionAsString,
                         final List<T> items) {
        super(expressionAsString);
        this.items = items;
    }
}
