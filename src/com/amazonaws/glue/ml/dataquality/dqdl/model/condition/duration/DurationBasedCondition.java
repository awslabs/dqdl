/*
 * DurationBasedCondition.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.duration;

import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.Condition;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

@Getter
@EqualsAndHashCode(callSuper = true)
public class DurationBasedCondition extends Condition {
    private final DurationBasedConditionOperator operator;
    private final List<Duration> operands;

    public DurationBasedCondition(final String conditionAsString,
                                  final DurationBasedConditionOperator operator,
                                  final List<Duration> operands) {
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
                    operands.get(0).getFormattedDuration(),
                    operands.get(1).getFormattedDuration());
            case NOT_BETWEEN:
                return String.format("not between %s and %s",
                    operands.get(0).getFormattedDuration(),
                    operands.get(1).getFormattedDuration());
            case GREATER_THAN:
                return String.format("> %s", operands.get(0).getFormattedDuration());
            case GREATER_THAN_EQUAL_TO:
                return String.format(">= %s", operands.get(0).getFormattedDuration());
            case LESS_THAN:
                return String.format("< %s", operands.get(0).getFormattedDuration());
            case LESS_THAN_EQUAL_TO:
                return String.format("<= %s", operands.get(0).getFormattedDuration());
            case EQUALS:
                return String.format("= %s", operands.get(0).getFormattedDuration());
            case NOT_EQUALS:
                return String.format("!= %s", operands.get(0).getFormattedDuration());
            case IN: {
                List<String> formattedOperands = getFormattedOperands();
                return String.format("in [%s]", String.join(", ", formattedOperands));
            }
            case NOT_IN: {
                List<String> formattedOperands = getFormattedOperands();
                return String.format("not in [%s]", String.join(", ", formattedOperands));
            }
            default:
                break;
        }

        return "";
    }

    private List<String> getFormattedOperands() {
        return operands.stream().map(Duration::getFormattedDuration).collect(Collectors.toList());
    }
}
