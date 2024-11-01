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
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.variable.VariableReferenceOperand;
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
    private final List<StringOperand> unresolvedOperands;

    public StringBasedCondition(final String conditionAsString,
                                final StringBasedConditionOperator operator,
                                final List<StringOperand> operands) {
        this(conditionAsString, operator, operands, null);
    }

    public StringBasedCondition(final String conditionAsString,
                                final StringBasedConditionOperator operator,
                                final List<StringOperand> operands,
                                final List<StringOperand> unresolvedOperands) {
        super(conditionAsString);
        this.operator = operator;
        this.operands = operands;
        this.unresolvedOperands = unresolvedOperands;
    }


    @Override
    public String getFormattedCondition() {
        if (StringUtils.isBlank(conditionAsString)) return "";

        List<StringOperand> effectiveOperands = getEffectiveOperands();

        switch (operator) {
            case MATCHES:
                return String.format("matches %s", effectiveOperands.get(0).formatOperand());
            case NOT_MATCHES:
                return String.format("not matches %s", effectiveOperands.get(0).formatOperand());
            case EQUALS:
                return String.format("= %s", effectiveOperands.get(0).formatOperand());
            case NOT_EQUALS:
                return String.format("!= %s", effectiveOperands.get(0).formatOperand());
            case IN:
                return formatInCondition(false, false);
            case NOT_IN:
                return formatInCondition(true, false);
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
                return formatInCondition(false, true);
            case NOT_IN:
                return formatInCondition(true, true);
            default:
                return getFormattedCondition();
        }
    }

    private String formatInCondition(boolean isNot, boolean sorted) {
        List<StringOperand> effectiveOperands = getEffectiveOperands();
        List<String> formattedOperands = sorted
                ? getSortedFormattedOperands(effectiveOperands) : getFormattedOperands(effectiveOperands);
        String operandStr;
        if (formattedOperands.size() == 1 && effectiveOperands.get(0) instanceof VariableReferenceOperand) {
            operandStr = formattedOperands.get(0);
        } else {
            operandStr = "[" + String.join(",", formattedOperands) + "]";
        }
        return String.format("%sin %s", isNot ? "not " : "", operandStr);
    }

    private List<String> getFormattedOperands(List<StringOperand> operands) {
        return operands.stream()
                .map(StringOperand::formatOperand)
                .collect(Collectors.toList());
    }

    private List<String> getSortedFormattedOperands(List<StringOperand> operands) {
        return operands.stream()
                .map(StringOperand::formatOperand)
                .sorted()
                .collect(Collectors.toList());
    }

    private List<StringOperand> getEffectiveOperands() {
        return unresolvedOperands != null ? unresolvedOperands : operands;
    }
}
