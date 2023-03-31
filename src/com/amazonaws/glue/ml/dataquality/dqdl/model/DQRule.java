/*
 * DQRule.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import com.amazonaws.glue.ml.dataquality.dqdl.model.expression.Expression;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.amazonaws.glue.ml.dataquality.dqdl.util.StringUtils.isBlank;

@AllArgsConstructor
@Getter
public class DQRule implements Serializable {
    private final String ruleType;
    private final Map<String, String> parameters;
    private final String thresholdExpression; // TODO: Remove once the typed Expression is fully integrated
    private final String hasThresholdExpression; // TODO: Remove once the typed Expression is fully integrated
    private final DQRuleLogicalOperator operator;
    private final List<DQRule> nestedRules;
    private final Expression expression;
    private final Expression threshold;

    private static final String EMPTY_STRING = "";
    private static final String KEYWORD_BETWEEN = "between";
    private static final String KEYWORD_AND = "and";
    private static final String KEYWORD_IN = "in";
    private static final String COMMA_DELIM = ",";
    private static final Character LBRAC_CHAR = '[';
    private static final Character RBRAC_CHAR = ']';

    public DQRule(final String ruleType,
                  final Map<String, String> parameters,
                  final Expression expression) {
        this.ruleType = ruleType;
        this.parameters = parameters;
        this.expression = expression;
        this.threshold = new Expression("");
        this.operator = DQRuleLogicalOperator.AND;
        this.nestedRules = new ArrayList<>();
        this.thresholdExpression = expression.getExpressionAsString();
        this.hasThresholdExpression = threshold.getExpressionAsString();
    }

    public DQRule(final String ruleType,
                  final Map<String, String> parameters,
                  final Expression expression,
                  final Expression hasThresholdExpression) {
        this.ruleType = ruleType;
        this.parameters = parameters;
        this.expression = expression;
        this.threshold = hasThresholdExpression;
        this.operator = DQRuleLogicalOperator.AND;
        this.nestedRules = new ArrayList<>();
        this.thresholdExpression = expression.getExpressionAsString();
        this.hasThresholdExpression = hasThresholdExpression.getExpressionAsString();
    }

    public DQRule(final String ruleType,
                  final Map<String, String> parameters,
                  final String thresholdExpression) {
        this.ruleType = ruleType;
        this.parameters = parameters;
        this.thresholdExpression = thresholdExpression;
        this.hasThresholdExpression = "";
        this.operator = DQRuleLogicalOperator.AND;
        this.nestedRules = new ArrayList<>();
        this.expression = new Expression(this.thresholdExpression);
        this.threshold = new Expression(this.hasThresholdExpression);
    }

    public DQRule(final String ruleType,
                  final Map<String, String> parameters,
                  final String thresholdExpression,
                  final String hasThresholdExpression,
                  final DQRuleLogicalOperator operator,
                  final List<DQRule> nestedRules) {
        this.ruleType = ruleType;
        this.parameters = parameters;
        this.thresholdExpression = thresholdExpression;
        this.hasThresholdExpression = hasThresholdExpression;
        this.operator = operator;
        this.nestedRules = nestedRules;
        this.expression = new Expression(this.thresholdExpression);
        this.threshold = new Expression(this.hasThresholdExpression);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (nestedRules == null || nestedRules.isEmpty()) {
            sb.append(ruleType);

            if (parameters != null) {
                parameters.values().forEach(p -> sb.append(" ").append("\"").append(p).append("\""));
            }

            if (!isBlank(thresholdExpression)) {
                sb.append(formatThreshold(thresholdExpression));
            }

            if (!isBlank(hasThresholdExpression)) {
                sb.append(" with threshold").append(formatThreshold(hasThresholdExpression));
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

    private String formatThreshold(String threshold) {
        StringBuilder sb = new StringBuilder();
        sb.append(" ");

        if (threshold.startsWith(KEYWORD_BETWEEN)) {
            String[] parts = threshold.replaceFirst(KEYWORD_BETWEEN, EMPTY_STRING).split(KEYWORD_AND);
            sb.append(KEYWORD_BETWEEN + " ")
                .append(formatDateExpression(parts[0]))
                .append(" " + KEYWORD_AND + " ")
                .append(formatDateExpression(parts[1]));
        } else if (threshold.startsWith(KEYWORD_IN)) {
            String[] parts = threshold
                .replaceFirst(KEYWORD_IN, EMPTY_STRING)
                .replaceFirst(Pattern.quote(Character.toString(LBRAC_CHAR)), EMPTY_STRING)
                .replaceFirst(Pattern.quote(Character.toString(RBRAC_CHAR)), EMPTY_STRING)
                .split(COMMA_DELIM);

            sb.append(KEYWORD_IN).append(" ").append(LBRAC_CHAR);
            sb.append(
                Arrays.stream(parts)
                    .map(this::formatDateExpression)
                    .collect(Collectors.joining(COMMA_DELIM))
            );
            sb.append(RBRAC_CHAR);
        } else {
            Pattern comparatorPattern = Pattern.compile("(matches|>=|<=|>|<|=)(.*)");
            Matcher m = comparatorPattern.matcher(threshold);
            if (m.find()) {
                sb.append(m.group(1))
                    .append(" ")
                    .append(formatDateExpression(m.group(2)));
            } else {
                sb.append(threshold);
            }
        }
        return sb.toString();
    }

    private String formatDateExpression(String expression) {
        String timeComparatorRegex = "(\\d*)(days|hours)";
        String dateExpressionComparatorRegex = "\\(now\\(\\)([-|+])" + timeComparatorRegex;

        Pattern timePattern = Pattern.compile(timeComparatorRegex);
        Pattern dateExpressionPattern = Pattern.compile(dateExpressionComparatorRegex);

        Matcher mTime = timePattern.matcher(expression);
        Matcher mDateExpression = dateExpressionPattern.matcher(expression);

        if (mDateExpression.find()) {
            return String.format("(now() %s %s %s)",
                mDateExpression.group(1), mDateExpression.group(2), mDateExpression.group(3));
        } else if (mTime.find()) {
            return String.format("%s %s", mTime.group(1), mTime.group(2));
        } else {
            return expression;
        }
    }
}
