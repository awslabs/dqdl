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

import lombok.AllArgsConstructor;
import lombok.Getter;

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
public class DQRule {
    private final String ruleType;
    private final Map<String, String> parameters;
    private final String thresholdExpression;
    private final DQRuleLogicalOperator operator;
    private final List<DQRule> nestedRules;

    private static final String EMPTY_STRING = "";
    private static final String KEYWORD_BETWEEN = "between";
    private static final String KEYWORD_AND = "and";
    private static final String KEYWORD_IN = "in";
    private static final String COMMA_DELIM = ",";
    private static final Character LBRAC_CHAR = '[';
    private static final Character RBRAC_CHAR = ']';

    public DQRule(final String ruleType,
                  final Map<String, String> parameters,
                  final String thresholdExpression) {
        this.ruleType = ruleType;
        this.parameters = parameters;
        this.thresholdExpression = thresholdExpression;
        this.operator = DQRuleLogicalOperator.AND;
        this.nestedRules = new ArrayList<>();
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
                sb.append(" ");

                if (thresholdExpression.startsWith(KEYWORD_BETWEEN)) {
                    String[] parts = thresholdExpression.replaceFirst(KEYWORD_BETWEEN, EMPTY_STRING).split(KEYWORD_AND);
                    sb.append(KEYWORD_BETWEEN + " ")
                      .append(formatDateExpression(parts[0]))
                      .append(" " + KEYWORD_AND + " ")
                      .append(formatDateExpression(parts[1]));
                } else if (thresholdExpression.startsWith(KEYWORD_IN)) {
                    String[] parts = thresholdExpression
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
                    Matcher m = comparatorPattern.matcher(thresholdExpression);
                    if (m.find()) {
                        sb.append(m.group(1))
                          .append(" ")
                          .append(formatDateExpression(m.group(2)));
                    } else {
                        sb.append(thresholdExpression);
                    }
                }
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

    private String formatDateExpression(String expression) {
        Pattern comparatorPattern = Pattern.compile("\\(now\\(\\)([-|+])(\\d*)([^\\s]*)\\)");
        Matcher m = comparatorPattern.matcher(expression);
        if (m.find()) {
            return String.format("(now() %s %s %s)", m.group(1), m.group(2), m.group(3));
        } else {
            return expression;
        }
    }
}
