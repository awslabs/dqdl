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
    private final List<String> operands;

    public StringBasedCondition(final String conditionAsString,
                                final StringBasedConditionOperator operator,
                                final List<String> operands) {
        super(conditionAsString);
        this.operator = operator;
        this.operands = operands;
    }

    @Override
    public String getFormattedCondition() {
        if (StringUtils.isBlank(conditionAsString)) return "";

        switch (operator) {
            case MATCHES:
                return String.format("matches %s", formatOperand(operands.get(0)));
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
        return "\"" + operand + "\"";
    }
}
