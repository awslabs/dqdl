/*
 * DQRuleParameterValue.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class DQRuleParameterValue implements Serializable {
    private static final String EMPTY_CONNECTOR = "";

    private final String value;
    private final boolean isQuoted;

    // We could use an Optional here, instead of resorting to an empty string.
    // But this needs to be serializable for Spark.
    // Optional has presented problems in that regard.
    private final String connectorWord;

    public DQRuleParameterValue(final String value) {
        this.value = value;
        this.isQuoted = false;
        this.connectorWord = EMPTY_CONNECTOR;
    }

    public DQRuleParameterValue(final String value, final boolean isQuoted) {
        this.value = value;
        this.isQuoted = isQuoted;
        this.connectorWord = EMPTY_CONNECTOR;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (!EMPTY_CONNECTOR.equals(connectorWord)) sb.append(connectorWord).append(" ");
        String surroundBy = isQuoted ? "\"" : "";
        sb.append(surroundBy).append(value).append(surroundBy);
        return sb.toString();
    }

    public static Map<String, DQRuleParameterValue> createParameterValueMap(Map<String, String> parameters) {
        Map<String, DQRuleParameterValue> map = new HashMap<>();
        if (parameters == null) return map;

        // Add quotes when converting from the map of string values, and do not use connector word.
        // This is to maintain backwards compatibility.
        boolean isQuoted = true;
        parameters.forEach((k, v) -> map.put(k, new DQRuleParameterValue(v, isQuoted)));

        return map;
    }

    public static Map<String, String> createParameterMap(Map<String, DQRuleParameterValue> parameters) {
        Map<String, String> paramValuesAsStringsMap = new LinkedHashMap<>();
        parameters.forEach((k, v) -> paramValuesAsStringsMap.put(k, v.getValue()));
        return paramValuesAsStringsMap;
    }
}
