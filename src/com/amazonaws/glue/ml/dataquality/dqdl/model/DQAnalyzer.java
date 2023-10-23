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

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;

@AllArgsConstructor
@Getter
public class DQAnalyzer implements HasRuleTypeAndParameters {
    private final String ruleType;
    private final Map<String, String> parameters;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ruleType);

        if (parameters != null) {
            parameters.values().forEach(p -> sb.append(" ").append("\"").append(p).append("\""));
        }

        return sb.toString();
    }
}
