/*
 * DQRuleParameterConstantValue.java
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

@Getter
@EqualsAndHashCode(callSuper = true)
public class DQRuleParameterConstantValue extends DQRuleParameterValue {
    private final String value;
    private final boolean isQuoted;
    private final String unresolvedValue;

    public DQRuleParameterConstantValue(String value, boolean isQuoted) {
        this(value, isQuoted, EMPTY_CONNECTOR, null);
    }

    public DQRuleParameterConstantValue(String value, boolean isQuoted, String connectorWord) {
        this(value, isQuoted, connectorWord, null);
    }

    public DQRuleParameterConstantValue(String value, boolean isQuoted, String connectorWord, String unresolvedValue) {
        super(connectorWord);
        this.value = value;
        this.isQuoted = isQuoted;
        this.unresolvedValue = unresolvedValue;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (!EMPTY_CONNECTOR.equals(connectorWord)) {
            sb.append(connectorWord).append(" ");
        }
        if (unresolvedValue != null) {
            return sb.append(unresolvedValue).toString();
        }
        String surroundBy = isQuoted ? "\"" : "";
        return sb.append(surroundBy).append(value).append(surroundBy).toString();
    }
}
