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
import java.util.Map;

@AllArgsConstructor
@Getter
public class DQRule {
    private final String ruleType;
    private final Map<String, String> parameters;
    private final String thresholdExpression;
    private final DQRuleLogicalOperator operator;
    private final List<DQRule> nestedRules;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ruleType);
        if (parameters != null && parameters.size() > 0) {
            sb.append(" | ");
            parameters.forEach((k, v) -> {
                sb.append(String.format("%s = %s", k, v)).append(", ");
            });
        }
        if (!"".equals(thresholdExpression)) {
            sb.append(" | ").append(thresholdExpression);
        }

        if (nestedRules.size() > 0) {
            sb.append("\n");
            nestedRules.forEach(sb::append);
        }

        return sb.toString();
    }
}
