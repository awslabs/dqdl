/*
 * DQVariable.java
 *
 * Copyright (c) 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class DQVariable<T> implements Serializable {

    public enum VariableType {
        NUMBER,
        STRING,
        DATE,
        DURATION,
        NUMBER_ARRAY,
        STRING_ARRAY,
        DATE_ARRAY,
        DURATION_ARRAY
    }

    private final String name;
    private final VariableType type;
    private final T value;

    @Override
    public String toString() {
        if (value instanceof List) {
            return String.format("%s = %s", name, formatArray((List<?>) value));
        }
        return String.format("%s = %s", name, formatValue(value));
    }

    private String formatValue(T val) {
        if (val == null) return "null";
        if (type == VariableType.STRING) return "\"" + val + "\"";
        return val.toString();
    }

    private String formatArray(List<?> list) {
        return "[" + list.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", ")) + "]";
    }
}
