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
import java.util.stream.Collectors;

@AllArgsConstructor
@Getter
public class DQRuleset {
    private final List<DQRule> rules;

    @Override
    public String toString() {
        return "Rules = [" + System.lineSeparator() +
            rules.stream()
                .map(i -> "    " + i)
                .collect(Collectors.joining("," + System.lineSeparator())) +
            System.lineSeparator() + "]";
    }
}
