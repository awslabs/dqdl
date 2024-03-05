/*
 * NullNumericOperand.java
 *
 * Copyright (c) 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.date;

import java.time.LocalDateTime;

public class NullDateExpression extends DateExpression {

    @Override
    public String getFormattedExpression() {
        return "NULL";
    }

    @Override
    public LocalDateTime getEvaluatedExpression() {
        return null;
    }
}
