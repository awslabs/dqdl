/*
 * DQRule.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.Condition;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.glue.ml.dataquality.dqdl.util.StringUtils.isBlank;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@Builder(toBuilder = true, access = AccessLevel.PRIVATE)
public class DQRule implements Serializable, HasRuleTypeAndParameters {
    private final String ruleType;
    private final Map<String, String> parameters;
    private final Map<String, DQRuleParameterValue> parameterValueMap;
    private final Condition condition;
    private final Condition thresholdCondition;
    private final DQRuleLogicalOperator operator;
    private final List<DQRule> nestedRules;
    private final String whereClause;
    private Boolean isExcludedAtRowLevelInCompositeRules = false;
    private Map<String, String> tags;

    // Adding this constructor so as to not break the Data Quality ETL package.
    public DQRule(final String ruleType,
                  final Map<String, String> parameters,
                  final Condition condition,
                  final Condition thresholdCondition,
                  final DQRuleLogicalOperator operator,
                  final List<DQRule> nestedRules,
                  final String whereClause) {
        this.ruleType = ruleType;
        this.parameters = parameters;
        this.parameterValueMap = DQRuleParameterValue.createParameterValueMap(parameters);
        this.condition = condition;
        this.thresholdCondition = thresholdCondition;
        this.operator = operator;
        this.nestedRules = nestedRules;
        this.whereClause = whereClause;
    }

    public DQRule(final String ruleType,
                  final Map<String, String> parameters,
                  final Condition condition,
                  final Condition thresholdCondition,
                  final DQRuleLogicalOperator operator,
                  final List<DQRule> nestedRules) {
        this.ruleType = ruleType;
        this.parameters = parameters;
        this.parameterValueMap = DQRuleParameterValue.createParameterValueMap(parameters);
        this.condition = condition;
        this.thresholdCondition = thresholdCondition;
        this.operator = operator;
        this.nestedRules = nestedRules;
        this.whereClause = null;
    }

    public DQRule(final String ruleType,
                  final Map<String, String> parameters,
                  final Condition condition) {
        this.ruleType = ruleType;
        this.parameters = parameters;
        this.parameterValueMap = DQRuleParameterValue.createParameterValueMap(parameters);
        this.condition = condition;
        this.thresholdCondition = null;
        this.operator = DQRuleLogicalOperator.AND;
        this.nestedRules = new ArrayList<>();
        this.whereClause = null;
    }

    // Can't overload the constructor above, due to type erasure
    public static DQRule createFromParameterValueMap(final DQRuleType ruleType,
                                                     final LinkedHashMap<String, DQRuleParameterValue> parameters,
                                                     final Condition condition) {
        return createFromParameterValueMap(ruleType, parameters, condition, null, null, null);
    }

    public DQRule(final String ruleType,
                  final Map<String, String> parameters,
                  final Condition condition,
                  final Condition thresholdCondition) {
        this.ruleType = ruleType;
        this.parameters = parameters;
        this.parameterValueMap = DQRuleParameterValue.createParameterValueMap(parameters);
        this.condition = condition;
        this.thresholdCondition = thresholdCondition;
        this.operator = DQRuleLogicalOperator.AND;
        this.nestedRules = new ArrayList<>();
        this.whereClause = null;
    }

    // Can't overload the constructor above, due to type erasure
    public static DQRule createFromParameterValueMap(final DQRuleType ruleType,
                                                     final LinkedHashMap<String, DQRuleParameterValue> parameters,
                                                     final Condition condition,
                                                     final Condition thresholdCondition,
                                                     final String whereClause,
                                                     final Map<String, String> tags) {
        DQRuleLogicalOperator operator = DQRuleLogicalOperator.AND;
        List<DQRule> nestedRules = new ArrayList<>();

        return new DQRule(
            ruleType.getRuleTypeName(),
            DQRuleParameterValue.createParameterMap(parameters),
            parameters,
            condition,
            thresholdCondition,
            operator,
            nestedRules,
            whereClause,
            ruleType.isExcludedAtRowLevelInCompositeRules(),
            tags
        );
    }

    public DQRule withNestedRules(final List<DQRule> nestedRules) {
        return this.toBuilder().nestedRules(nestedRules).build();
    }

    public DQRule withCondition(final Condition condition) {
        return this.toBuilder().condition(condition).build();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (nestedRules == null || nestedRules.isEmpty()) {
            sb.append(ruleType);

            if (parameterValueMap != null) {
                parameterValueMap.values().forEach(p -> sb.append(" ").append(p.toString()));
            }

            if (condition != null) {
                String formattedCondition = condition.getFormattedCondition();
                if (!isBlank(formattedCondition)) sb.append(" ").append(condition.getFormattedCondition());
            }

            // where clause syntax should go before threshold
            if (whereClause != null) {
                if (!isBlank(whereClause)) sb.append(" where ").append("\"").append(whereClause).append("\"");
            }

            if (thresholdCondition != null) {
                String formattedCondition = thresholdCondition.getFormattedCondition();
                if (!isBlank(formattedCondition)) sb.append(" with threshold ").append(formattedCondition);
            }

            if (tags != null && !tags.isEmpty()) {
                sb.append(" ");
                for (Map.Entry<String, String> entry : tags.entrySet()) {
                    sb.append("with \"")
                            .append(entry.getKey())
                            .append("\" = \"")
                            .append(entry.getValue())
                            .append("\" ");
                }
            }

            return sb.toString().trim();
        } else {
            boolean canBeFlattened = usesSameOperator(operator);

            if (canBeFlattened) {
                List<DQRule> flattenedListOfRules = getNestedRulesAsFlattenedList();
                for (int i = 0; i < flattenedListOfRules.size(); i++) {
                    sb.append("(").append(flattenedListOfRules.get(i).toString()).append(")");
                    if (i != flattenedListOfRules.size() - 1) {
                        sb.append(" ").append(operator.toString()).append(" ");
                    }
                }
            } else {
                for (int i = 0; i < nestedRules.size(); i++) {
                    sb.append("(").append(nestedRules.get(i).toString()).append(")");
                    if (i != nestedRules.size() - 1) {
                        sb.append(" ").append(operator.toString()).append(" ");
                    }
                }
            }
        }

        return sb.toString();
    }

    /*
     * This function checks if the same operator is used across all the nested rules.
     * Example: (RuleA) or (RuleB) or (RuleC) / (RuleA) and (RuleB) and (RuleC)
     *
     * If that is the case, in order to maintain backwards compatibility, we will update
     * toString() method so that we do not add additional parentheses.
     */
    private boolean usesSameOperator(DQRuleLogicalOperator op) {
        if (nestedRules.isEmpty()) return true;
        if (operator != op) return false;

        for (DQRule nestedRule : nestedRules) {
            if (!nestedRule.usesSameOperator(op)) {
                return false;
            }
        }

        return true;
    }

    // Package private, in order to make it accessible to the tests
    List<DQRule> getNestedRulesAsFlattenedList() {
        List<DQRule> ret = new ArrayList<>();
        if (nestedRules.isEmpty()) {
            ret.add(this);
        } else {
            for (DQRule nestedRule: nestedRules) {
                List<DQRule> nestedRet = nestedRule.getNestedRulesAsFlattenedList();
                ret.addAll(nestedRet);
            }
        }
        return ret;
    }
}
