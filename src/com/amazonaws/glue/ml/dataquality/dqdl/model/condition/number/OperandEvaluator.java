/*
 * OperandEvaluator.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number;

import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRule;

import java.io.Serializable;
import java.util.List;

public abstract class OperandEvaluator implements Serializable {
    public abstract Double evaluate(DQRule rule, String functionName, List<Double> operands);
}
