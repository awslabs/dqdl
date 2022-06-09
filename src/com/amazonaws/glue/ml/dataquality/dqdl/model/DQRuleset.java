/*
 * DQRuleset.java
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

import java.util.List;

@AllArgsConstructor
@Getter
public class DQRuleset {
    private final List<DQRule> rules;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Rules { ").append("\n");
        rules.forEach(dqRule -> sb.append(dqRule.toString()).append("\n"));
        sb.append("}");
        return sb.toString();
    }
}

