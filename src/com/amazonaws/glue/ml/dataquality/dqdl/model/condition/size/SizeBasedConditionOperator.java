/*
 * SizeBasedConditionOperator.java
 *
 * Copyright (c) 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.size;

public enum SizeBasedConditionOperator {
    BETWEEN,
    NOT_BETWEEN,
    GREATER_THAN,
    GREATER_THAN_EQUAL_TO,
    LESS_THAN,
    LESS_THAN_EQUAL_TO,
    EQUALS,
    NOT_EQUALS,
    IN,
    NOT_IN
}