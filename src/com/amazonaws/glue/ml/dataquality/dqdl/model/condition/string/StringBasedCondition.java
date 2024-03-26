/*
 * StringBasedCondition.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string;

import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.Condition;
import com.amazonaws.glue.ml.dataquality.dqdl.util.StringUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

@Getter
@EqualsAndHashCode(callSuper = true)
public class StringBasedCondition extends Condition {
    private final StringBasedConditionOperator operator;
    private final List<StringOperand> operands;

    public StringBasedCondition(final String conditionAsString,
                                final StringBasedConditionOperator operator,
                                final List<StringOperand> operands) {
        super(conditionAsString);
        this.operator = operator;
        this.operands = operands;
    }

    @Override
    public String getFormattedCondition() {
        if (StringUtils.isBlank(conditionAsString)) return "";

        switch (operator) {
            case MATCHES:
                return String.format("matches %s", operands.get(0).formatOperand());
            case NOT_MATCHES:
                return String.format("not matches %s", operands.get(0).formatOperand());
            case EQUALS:
                return String.format("= %s", operands.get(0).formatOperand());
            case NOT_EQUALS:
                return String.format("!= %s", operands.get(0).formatOperand());
            case IN: {
                List<String> formattedOperands = getFormattedOperands();
                return String.format("in [%s]", String.join(",", formattedOperands));
            }
            case NOT_IN: {
                List<String> formattedOperands = getFormattedOperands();
                return String.format("not in [%s]", String.join(",", formattedOperands));
            }
            default:
                break;
        }

        return "";
    }

    @Override
    public String getSortedFormattedCondition() {
        if (StringUtils.isBlank(conditionAsString)) return "";

        switch (operator) {
            case IN:
                return String.format("in [%s]", String.join(",", getSortedFormattedOperands()));
            case NOT_IN:
                return String.format("not in [%s]", String.join(",", getSortedFormattedOperands()));
            default:
                return getFormattedCondition();
        }
    }

    private List<String> getFormattedOperands() {
        List<String> formattedOperands = operands.stream()
            .map(StringOperand::formatOperand)
            .collect(Collectors.toList());
        return formattedOperands;
    }

    private List<String> getSortedFormattedOperands() {
        List<String> formattedOperands = operands.stream()
                .map(StringOperand::formatOperand)
                .sorted()
                .collect(Collectors.toList());
        return formattedOperands;
    }
}
