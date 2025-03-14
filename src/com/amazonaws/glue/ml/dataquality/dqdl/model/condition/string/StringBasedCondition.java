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
import java.util.Locale;
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
        return getFormattedConditionHelper(false);
    }

    public String getFormattedCondition(boolean lowerCase) {
        return getFormattedConditionHelper(lowerCase);
    }

    private String getFormattedConditionHelper(boolean lowerCase) {
        if (StringUtils.isBlank(conditionAsString)) return "";

        List<StringOperand> effectiveOperands = getEffectiveOperands();

        switch (operator) {
            case MATCHES:
                return String.format("matches %s", handleOperandFormatting(effectiveOperands.get(0), lowerCase));
            case NOT_MATCHES:
                return String.format("not matches %s", handleOperandFormatting(effectiveOperands.get(0), lowerCase));
            case EQUALS:
                return String.format("= %s", handleOperandFormatting(effectiveOperands.get(0), lowerCase));
            case NOT_EQUALS:
                return String.format("!= %s", handleOperandFormatting(effectiveOperands.get(0), lowerCase));
            case IN:
                return formatInConditionWithVariables(false, lowerCase);
            case NOT_IN:
                return formatInConditionWithVariables(true, lowerCase);
            default:
                break;
        }

        return "";
    }

    @Override
    public String getSortedFormattedCondition() {
        return getSortedFormattedConditionHelper(false);
    }

    public String getSortedFormattedCondition(boolean lowerCase) {
        return getSortedFormattedConditionHelper(lowerCase);
    }

    private String getSortedFormattedConditionHelper(boolean lowerCase) {
        if (StringUtils.isBlank(conditionAsString)) return "";

        switch (operator) {
            case IN:
                return formatInConditionWithValues(false, true, lowerCase);
            case NOT_IN:
                return formatInConditionWithValues(true, true, lowerCase);
            case EQUALS:
                return String.format("= %s", handleOperandFormatting(operands.get(0), lowerCase));
            case NOT_EQUALS:
                return String.format("!= %s", handleOperandFormatting(operands.get(0), lowerCase));
            case MATCHES:
                return String.format("matches %s", handleOperandFormatting(operands.get(0), lowerCase));
            case NOT_MATCHES:
                return String.format("not matches %s", handleOperandFormatting(operands.get(0), lowerCase));
            default:
                return getFormattedCondition(lowerCase);
        }
    }

    private List<String> getFormattedOperands(List<StringOperand> operands, boolean lowerCase) {
        return operands.stream()
                .map(op -> handleOperandFormatting(op, lowerCase))
                .collect(Collectors.toList());
    }

    private List<String> getSortedFormattedOperands(List<StringOperand> operands, boolean lowerCase) {
        return operands.stream()
                .map(op -> handleOperandFormatting(op, lowerCase))
                .sorted()
                .collect(Collectors.toList());
    }

    private List<StringOperand> getEffectiveOperands() {
        return unresolvedOperands != null ? unresolvedOperands : operands;
    }

    private String formatInConditionWithVariables(boolean isNot, boolean lowerCase) {
        List<StringOperand> effectiveOperands = getEffectiveOperands();
        List<String> formattedOperands = getFormattedOperands(effectiveOperands, lowerCase);
        String operandStr;
        if (effectiveOperands.size() == 1 && effectiveOperands.get(0) instanceof VariableReferenceOperand) {
            operandStr = formattedOperands.get(0);
        } else {
            operandStr = "[" + String.join(",", formattedOperands) + "]";
        }
        return String.format("%sin %s", isNot ? "not " : "", operandStr);
    }

    private String formatInConditionWithValues(boolean isNot, boolean sorted, boolean lowerCase) {
        List<String> formattedOperands = sorted
                ? getSortedFormattedOperands(operands, lowerCase)
                : getFormattedOperands(operands, lowerCase);

        String operandStr = "[" + String.join(",", formattedOperands) + "]";
        return String.format("%sin %s", isNot ? "not " : "", operandStr);
    }

    private String handleOperandFormatting(StringOperand operand, boolean lowerCase) {
        if (operand instanceof KeywordStringOperand) {
            return operand.formatOperand();
        } else if (operand instanceof VariableReferenceOperand) {
            return operand.formatOperand();
        } else if (operand instanceof  QuotedStringOperand) {
            return lowerCase ? operand.formatOperand().toLowerCase(Locale.ROOT) : operand.formatOperand();
        } else {
            return operand.formatOperand();
        }
    }

}
