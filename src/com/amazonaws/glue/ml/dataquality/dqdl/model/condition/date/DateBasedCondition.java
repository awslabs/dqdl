/*
 * DateBasedCondition.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.date;

import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.Condition;
import com.amazonaws.glue.ml.dataquality.dqdl.util.StringUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

@Getter
@EqualsAndHashCode(callSuper = true)
public class DateBasedCondition extends Condition {
    private final DateBasedConditionOperator operator;
    private final List<DateExpression> operands;

    public DateBasedCondition(final String conditionAsString,
                              final DateBasedConditionOperator operator,
                              final List<DateExpression> operands) {
        super(conditionAsString);
        this.operator = operator;
        this.operands = operands;
    }

    @Override
    public String getFormattedCondition() {
        if (StringUtils.isBlank(conditionAsString)) return "";

        switch (operator) {
            case BETWEEN:
                return String.format("between %s and %s",
                    operands.get(0).getFormattedExpression(),
                    operands.get(1).getFormattedExpression()
                );
            case NOT_BETWEEN:
                return String.format("not between %s and %s",
                    operands.get(0).getFormattedExpression(),
                    operands.get(1).getFormattedExpression()
                );
            case GREATER_THAN:
                return String.format("> %s", operands.get(0).getFormattedExpression());
            case GREATER_THAN_EQUAL_TO:
                return String.format(">= %s", operands.get(0).getFormattedExpression());
            case LESS_THAN:
                return String.format("< %s", operands.get(0).getFormattedExpression());
            case LESS_THAN_EQUAL_TO:
                return String.format("<= %s", operands.get(0).getFormattedExpression());
            case EQUALS:
                return String.format("= %s", operands.get(0).getFormattedExpression());
            case NOT_EQUALS:
                return String.format("!= %s", operands.get(0).getFormattedExpression());
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
            .map(DateExpression::getFormattedExpression)
            .collect(Collectors.toList());
        return formattedOperands;
    }

    private List<String> getSortedFormattedOperands() {
        List<String> formattedOperands = operands.stream()
                .map(DateExpression::getFormattedExpression)
                .sorted()
                .collect(Collectors.toList());
        return formattedOperands;
    }
}
