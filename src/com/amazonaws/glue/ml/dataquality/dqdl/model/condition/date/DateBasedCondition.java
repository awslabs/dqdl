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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Getter
@EqualsAndHashCode(callSuper = true)
public class DateBasedCondition extends Condition {
    private final DateBasedConditionOperator operator;
    private final List<String> operands;

    public DateBasedCondition(final String conditionAsString,
                              final DateBasedConditionOperator operator,
                              final List<String> operands) {
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
                    formatOperand(operands.get(0)), formatOperand(operands.get(1)));
            case GREATER_THAN:
                return String.format("> %s", formatOperand(operands.get(0)));
            case GREATER_THAN_EQUAL_TO:
                return String.format(">= %s", formatOperand(operands.get(0)));
            case LESS_THAN:
                return String.format("< %s", formatOperand(operands.get(0)));
            case LESS_THAN_EQUAL_TO:
                return String.format("<= %s", formatOperand(operands.get(0)));
            case EQUALS:
                return String.format("= %s", formatOperand(operands.get(0)));
            case IN: {
                List<String> formattedOperands = operands.stream()
                    .map(this::formatOperand)
                    .collect(Collectors.toList());
                return String.format("in [%s]", String.join(",", formattedOperands));
            }
            default:
                break;
        }

        return "";
    }

    private String formatOperand(String operand) {
        String timeComparatorRegex = "(\\d*)(days|hours)";
        String dateExpressionComparatorRegex = "\\(now\\(\\)([-|+])" + timeComparatorRegex;
        String dateRegex = "\\d{4}-\\d{2}-\\d{2}";

        Pattern timePattern = Pattern.compile(timeComparatorRegex);
        Pattern dateExpressionPattern = Pattern.compile(dateExpressionComparatorRegex);
        Pattern datePattern = Pattern.compile(dateRegex);

        Matcher mTime = timePattern.matcher(operand);
        Matcher mDateExpression = dateExpressionPattern.matcher(operand);
        Matcher mDate = datePattern.matcher(operand);

        if (mDateExpression.find()) {
            return String.format("(now() %s %s %s)",
                mDateExpression.group(1), mDateExpression.group(2), mDateExpression.group(3));
        } else if (mTime.find()) {
            return String.format("%s %s", mTime.group(1), mTime.group(2));
        } else if (mDate.find()) {
            return "\"" + operand + "\"";
        } else {
            return operand;
        }
    }
}
