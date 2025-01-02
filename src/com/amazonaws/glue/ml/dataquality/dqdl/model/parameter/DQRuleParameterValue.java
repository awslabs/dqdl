/*
 * DQRuleParameterValue.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.parameter;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Getter
@EqualsAndHashCode
public abstract class DQRuleParameterValue implements Serializable {
    protected static final String EMPTY_CONNECTOR = "";

    protected final String connectorWord;

    protected DQRuleParameterValue(String connectorWord) {
        this.connectorWord = connectorWord != null ? connectorWord : EMPTY_CONNECTOR;
    }

    public abstract String toString();

    public static Map<String, DQRuleParameterValue> createParameterValueMap(Map<String, String> parameters) {
        Map<String, DQRuleParameterValue> map = new HashMap<>();
        if (parameters == null) return map;

        parameters.forEach((k, v) -> map.put(k, new DQRuleParameterConstantValue(v, true)));
        return map;
    }

    public static Map<String, String> createParameterMap(Map<String, DQRuleParameterValue> parameters) {
        Map<String, String> paramValuesAsStringsMap = new LinkedHashMap<>();
        parameters.forEach((k, v) -> {
            if (v instanceof DQRuleParameterConstantValue) {
                paramValuesAsStringsMap.put(k, ((DQRuleParameterConstantValue) v).getValue());
            } else if (v instanceof DQRuleParameterVariableValue) {
                paramValuesAsStringsMap.put(k, ((DQRuleParameterVariableValue) v).getUnresolvedValue());
            }
        });
        return paramValuesAsStringsMap;
    }
}
