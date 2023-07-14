/*
 * DQRuleParameter.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class DQRuleParameter {
    private final String type;
    private final String name;
    private final String description;
    private final boolean isVarArg;

    public DQRuleParameter(@JsonProperty("type") String type,
                           @JsonProperty("name") String name,
                           @JsonProperty("description") String description,
                           @JsonProperty("is_var_arg") boolean isVarArg) {
        this.type = type;
        this.name = name;
        this.description = description;
        this.isVarArg = isVarArg;
    }
}
