/*
 * NumberBasedCondition.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number;

import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRule;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.Condition;
import com.amazonaws.glue.ml.dataquality.dqdl.util.StringUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.text.DecimalFormat;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class NumberBasedCondition extends Condition {
    private final NumberBasedConditionOperator operator;
    private final List<NumericOperand> operands;

    private static final DecimalFormat OP_FORMAT = new DecimalFormat("#.###");

    public NumberBasedCondition(final String conditionAsString,
                                final NumberBasedConditionOperator operator,
                                final List<NumericOperand> operands) {
        super(conditionAsString);
        this.operator = operator;
        this.operands = operands;
    }

    public Boolean evaluate(Double metric, DQRule dqRule, OperandEvaluator evaluator) {
        if (operands == null) return false;

        List<Double> operandsAsDouble = operands.stream()
            .map(operand -> evaluator.evaluate(dqRule, operand)).collect(Collectors.toList());


        log.info(String.format("Evaluating condition for rule: %s", dqRule));
        List<String> formatOps = operandsAsDouble.stream().map(OP_FORMAT::format).collect(Collectors.toList());
        String formatMetric = OP_FORMAT.format(metric);

        switch (operator) {
            case BETWEEN:
                if (operands.size() != 2) return false;
                else {
                    boolean result = metric > operandsAsDouble.get(0) && metric < operandsAsDouble.get(1);
                    log.info("{} between {} and {}? {}", formatMetric, formatOps.get(0), formatOps.get(1), result);
                    return result;
                }
            case GREATER_THAN_EQUAL_TO:
                if (operands.size() != 1) return false;
                else {
                    boolean result = metric >= operandsAsDouble.get(0);
                    log.info("{} >= {}? {}", formatMetric, formatOps.get(0), result);
                    return result;
                }
            case GREATER_THAN:
                if (operands.size() != 1) return false;
                else {
                    boolean result = metric > operandsAsDouble.get(0);
                    log.info("{} > {}? {}", formatMetric, formatOps.get(0), result);
                    return result;
                }
            case LESS_THAN_EQUAL_TO:
                if (operands.size() != 1) return false;
                else {
                    boolean result = metric <= operandsAsDouble.get(0);
                    log.info("{} <= {}? {}", formatMetric, formatOps.get(0), result);
                    return result;
                }
            case LESS_THAN:
                if (operands.size() != 1) return false;
                else {
                    boolean result = metric < operandsAsDouble.get(0);
                    log.info("{} < {}? {}", formatMetric, formatOps.get(0), result);
                    return result;
                }
            case EQUALS:
                if (operands.size() != 1) return false;
                else {
                    boolean result = metric.equals(operandsAsDouble.get(0));
                    log.info("{} == {}? {}", formatMetric, formatOps.get(0), result);
                    return result;
                }
            case IN:
                boolean result = operandsAsDouble.contains(metric);
                log.info("{} in {}? {}", formatMetric, formatOps, result);
                return result;
            default:
                log.error("Unknown operator");
                return false;
        }
    }

    @Override
    public String getFormattedCondition() {
        if (StringUtils.isBlank(conditionAsString)) return "";

        switch (operator) {
            case BETWEEN:
                return String.format("between %s and %s", operands.get(0).toString(), operands.get(1).toString());
            case GREATER_THAN:
                return String.format("> %s", operands.get(0).toString());
            case GREATER_THAN_EQUAL_TO:
                return String.format(">= %s", operands.get(0).toString());
            case LESS_THAN:
                return String.format("< %s", operands.get(0).toString());
            case LESS_THAN_EQUAL_TO:
                return String.format("<= %s", operands.get(0).toString());
            case EQUALS:
                return String.format("= %s", operands.get(0).toString());
            case IN:
                return String.format("in [%s]",
                    operands.stream()
                        .map(NumericOperand::toString)
                        .collect(Collectors.joining(","))
                );
            default:
                break;
        }

        return "";
    }
}
