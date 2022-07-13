/*
 * DataQualityRulesetNotValidException.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.exception;

public class DataQualityRulesetNotValidException extends Exception {
    public DataQualityRulesetNotValidException(String message) {
        super(message);
    }
}