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

import java.util.Optional;

@AllArgsConstructor
@Getter
public class DQRule {
    private final String ruleType;
    private final Optional<String> target;
    private final Optional<Integer> expectedRowCount;

    @Override
    public String toString() {
        return "DQRule{" +
            "ruleType='" + ruleType + '\'' + "," +
            (target.map(s -> ("target='" + s + '\'' + ",")).orElse("")) +
            (expectedRowCount.map(s -> ("expectedRowCount='" + s + '\'' + ",")).orElse("")) +
            '}';
    }
}

