/*
 * DQRule.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.updated;

import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleLogicalOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.Condition;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.amazonaws.glue.ml.dataquality.dqdl.util.StringUtils.isBlank;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class DQRule implements Serializable {
    private final String ruleType;
    private final Map<String, String> parameters;
    private final Condition condition;
    private final Condition thresholdCondition;
    private final DQRuleLogicalOperator operator;
    private final List<DQRule> nestedRules;

    public DQRule(final String ruleType,
                  final Map<String, String> parameters,
                  final Condition condition) {
        this.ruleType = ruleType;
        this.parameters = parameters;
        this.condition = condition;
        this.thresholdCondition = null;
        this.operator = DQRuleLogicalOperator.AND;
        this.nestedRules = new ArrayList<>();
    }

    public DQRule(final String ruleType,
                  final Map<String, String> parameters,
                  final Condition condition,
                  final Condition thresholdCondition) {
        this.ruleType = ruleType;
        this.parameters = parameters;
        this.condition = condition;
        this.thresholdCondition = thresholdCondition;
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

            if (condition != null) {
                String formattedCondition = condition.getFormattedCondition();
                if (!isBlank(formattedCondition)) sb.append(" ").append(condition.getFormattedCondition());
            }

            if (thresholdCondition != null) {
                String formattedCondition = thresholdCondition.getFormattedCondition();
                if (!isBlank(formattedCondition)) sb.append(" with threshold ").append(formattedCondition);
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
