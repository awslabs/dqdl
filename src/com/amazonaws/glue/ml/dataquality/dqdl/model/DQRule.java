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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.glue.ml.dataquality.dqdl.util.StringUtils.isBlank;

@AllArgsConstructor
@Getter
public class DQRule {
    private final String ruleType;
    private final Map<String, String> parameters;
    private final String thresholdExpression;
    private final DQRuleLogicalOperator operator;
    private final List<DQRule> nestedRules;

    private static final String KEYWORD_BETWEEN = "between";
    private static final String KEYWORD_AND = "and";

    public DQRule(final String ruleType,
                  final Map<String, String> parameters,
                  final String thresholdExpression) {
        this.ruleType = ruleType;
        this.parameters = parameters;
        this.thresholdExpression = thresholdExpression;
        this.operator = DQRuleLogicalOperator.AND;
        this.nestedRules = new ArrayList<>();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (nestedRules == null || nestedRules.isEmpty()) {
            sb.append(ruleType);

            if (parameters != null) {
                parameters.values().forEach(p -> sb.append(" ").append("\"").append(p).append("\""));
            }

            if (!isBlank(thresholdExpression)) {
                sb.append(" ");

                String thresholdExpressionLower = thresholdExpression.toLowerCase();
                if (thresholdExpressionLower.contains(KEYWORD_BETWEEN)) {
                    String[] parts = thresholdExpressionLower.split(KEYWORD_BETWEEN)[1].split(KEYWORD_AND);
                    sb.append(KEYWORD_BETWEEN + " ").append(parts[0]).append(" " + KEYWORD_AND + " ").append(parts[1]);
                } else {
                    Pattern comparatorPattern = Pattern.compile("(in|>=|<=|>|<|=)(.*)");
                    Matcher m = comparatorPattern.matcher(thresholdExpression);
                    if (m.find()) {
                        sb.append(m.group(1)).append(" ").append(m.group(2));
                    } else {
                        sb.append(thresholdExpression);
                    }
                }
            }

            return sb.toString();
        } else {
            for (int i = 0; i < nestedRules.size(); i++) {
                sb.append("(").append(nestedRules.get(i).toString()).append(")");
                if (i != nestedRules.size() - 1) {
                    sb.append(" ").append(operator.toString()).append(" ");
                }
            }
        }

        return sb.toString();
    }
}
