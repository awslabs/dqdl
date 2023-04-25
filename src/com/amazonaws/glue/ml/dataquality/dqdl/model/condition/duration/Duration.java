/*
 * Duration.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.duration;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class Duration implements Serializable {
    private final Integer amount;
    private final DurationUnit unit;

    public String getFormattedDuration() {
        return String.format("%s %s", amount, unit.name().toLowerCase());
    }
}
