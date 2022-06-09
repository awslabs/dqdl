/*
 * DQRule.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class DQRule {
    private final String ruleType;

    @Override
    public String toString() {
        return "DQRule{" +
            "ruleType='" + ruleType + '\'' +
            '}';
    }
}

