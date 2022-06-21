/*
 * DQConstraint.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;
import java.util.Optional;

@AllArgsConstructor
@Getter
public class DQConstraint {
    private final String constraintType;
    private final Map<String, String> parameters;
    private final Optional<String> thresholdExpression;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(constraintType);
        if (parameters.size() > 0) {
            sb.append("| ");
            parameters.forEach((a, b) -> sb.append(String.format("%s = %s,", a, b)));
        }

        if (thresholdExpression.isPresent()) {
            sb.append("| ");
            sb.append(thresholdExpression.get());
        }

        return sb.toString();
    }
}
