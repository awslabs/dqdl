/*
 * DQRuleParameterVariableValue.java
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
public class DQRuleParameterVariableValue extends DQRuleParameterValue {
    private final String unresolvedValue;

    public DQRuleParameterVariableValue(String unresolvedValue, String connectorWord) {
        super(connectorWord);
        this.unresolvedValue = unresolvedValue;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (!EMPTY_CONNECTOR.equals(connectorWord)) {
            sb.append(connectorWord).append(" ");
        }
        sb.append("$").append(unresolvedValue);
        return sb.toString();
    }
}
