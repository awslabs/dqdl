/*
 * StringSetExpression.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.expression;

import java.util.List;

public class StringSetExpression extends SetExpression<String> {
    public StringSetExpression(final String expressionAsString,
                               final List<String> items) {
        super(expressionAsString, items);
    }
}
