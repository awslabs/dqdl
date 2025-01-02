/*
 * VariableResolutionResult.java
 *
 * Copyright (c) 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.variable;

import com.amazonaws.glue.ml.dataquality.dqdl.model.parameter.DQRuleParameterValue;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.Condition;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.LinkedHashMap;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class VariableResolutionResult {
    private final LinkedHashMap<String, DQRuleParameterValue> resolvedParameters;
    private final Condition resolvedCondition;
    private final Condition resolvedThresholdCondition;
}
