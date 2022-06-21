/*
 * DQRule.java
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

import java.util.List;

@AllArgsConstructor
@Getter
public class DQRule {
    private final List<DQConstraint> constraints;
    private final DQConstraintOperator operator;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        constraints.forEach(constraint -> sb.append(constraint.toString()).append("\n"));
        return "DQRule {" + "\n" + sb.toString() + "}";
    }
}
