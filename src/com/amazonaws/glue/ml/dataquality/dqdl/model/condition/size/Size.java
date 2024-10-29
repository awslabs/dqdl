/*
 * Size.java
 *
 * Copyright (c) 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.size;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;

@Getter
@EqualsAndHashCode
public class Size implements Serializable, Comparable<Size> {
    private final Integer amount;
    private final SizeUnit unit;
    private final Long bytes;

    public Size(final Integer amount, final SizeUnit unit) {
        this.amount = amount;
        this.unit = unit;
        this.bytes = convertBytes(amount, unit);
    }

    public String getFormattedSize() {
        return String.format("%s %s", amount, unit.name().toUpperCase());
    }

    private Long convertBytes(Integer bytes, SizeUnit unit) {
        switch (unit) {
            case KB:
                return bytes * 1024L;
            case MB:
                return bytes * 1024L * 1024L;
            case GB:
                return bytes * 1024L * 1024L * 1024L;
            case TB:
                return bytes * 1024L * 1024L * 1024L * 1024L;
            default:
                return Long.valueOf(bytes);
        }
    }

    @Override
    public int compareTo(Size other) {
        return Long.compare(this.getBytes(), other.getBytes());
    }
}
