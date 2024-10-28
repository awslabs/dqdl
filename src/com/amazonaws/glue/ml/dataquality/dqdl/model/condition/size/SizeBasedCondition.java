/*
 * SizeBasedCondition.java
 *
 * Copyright (c) 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.size;

import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.Condition;
import com.amazonaws.glue.ml.dataquality.dqdl.util.StringUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

@Getter
@EqualsAndHashCode(callSuper = true)
public class SizeBasedCondition extends Condition {
    private final SizeBasedConditionOperator operator;
    private final List<Size> operands;

    public SizeBasedCondition(final String conditionAsString,
                                  final SizeBasedConditionOperator operator,
                                  final List<Size> operands) {
        super(conditionAsString);
        this.operator = operator;
        this.operands = operands;
    }

    @Override
    public String getFormattedCondition() {
        if (this.operands.isEmpty()) return "";

        switch (operator) {
            case BETWEEN:
                return String.format("between %s and %s",
                    operands.get(0).getFormattedSize(),
                    operands.get(1).getFormattedSize());
            case NOT_BETWEEN:
                return String.format("not between %s and %s",
                    operands.get(0).getFormattedSize(),
                    operands.get(1).getFormattedSize());
            case GREATER_THAN:
                return String.format("> %s", operands.get(0).getFormattedSize());
            case GREATER_THAN_EQUAL_TO:
                return String.format(">= %s", operands.get(0).getFormattedSize());
            case LESS_THAN:
                return String.format("< %s", operands.get(0).getFormattedSize());
            case LESS_THAN_EQUAL_TO:
                return String.format("<= %s", operands.get(0).getFormattedSize());
            case EQUALS:
                return String.format("= %s", operands.get(0).getFormattedSize());
            case NOT_EQUALS:
                return String.format("!= %s", operands.get(0).getFormattedSize());
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
        return operands.stream().map(Size::getFormattedSize).collect(Collectors.toList());
    }

    private List<String> getSortedFormattedOperands() {
        return operands.stream().map(Size::getFormattedSize).sorted().collect(Collectors.toList());
    }
}
