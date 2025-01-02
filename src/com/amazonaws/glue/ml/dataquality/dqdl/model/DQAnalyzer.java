/*
 * DQAnalyzer.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import com.amazonaws.glue.ml.dataquality.dqdl.model.parameter.DQRuleParameterValue;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.Map;

@AllArgsConstructor
@Getter
public class DQAnalyzer implements HasRuleTypeAndParameters {
    private final String ruleType;
    private final Map<String, String> parameters;
    private final Map<String, DQRuleParameterValue> parameterValueMap;

    public DQAnalyzer(final String ruleType,
                      final Map<String, String> parameters) {
        this.ruleType = ruleType;
        this.parameters = parameters;
        this.parameterValueMap = DQRuleParameterValue.createParameterValueMap(this.parameters);
    }

    public static DQAnalyzer createFromValueMap(final String ruleType,
                                                final LinkedHashMap<String, DQRuleParameterValue> parameters) {
        return new DQAnalyzer(ruleType, DQRuleParameterValue.createParameterMap(parameters), parameters);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ruleType);

        if (parameterValueMap != null) {
            parameterValueMap.values().forEach(p -> sb.append(" ").append(p.toString()));
        }

        return sb.toString();
    }
}
