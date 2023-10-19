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

/**
 * Class encapsulates implementation logic for resolving NumericOperand to a number (double).
 */
public abstract class OperandEvaluator implements Serializable {

    // resolve operand to number
    public abstract Double evaluate(DQRule rule, NumericOperand operand);
}
