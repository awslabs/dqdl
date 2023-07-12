/*
 * DurationTest.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.duration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DurationTest {
    private static Stream<Arguments> provideDurationsWithExpectedFormattedStrings() {
        return Stream.of(
            Arguments.of(new Duration(1, DurationUnit.DAYS), "1 days"),
            Arguments.of(new Duration(10, DurationUnit.DAYS), "10 days"),
            Arguments.of(new Duration(2, DurationUnit.HOURS), "2 hours"),
            Arguments.of(new Duration(20, DurationUnit.HOURS), "20 hours")
        );
    }

    @ParameterizedTest
    @MethodSource("provideDurationsWithExpectedFormattedStrings")
    public void test_correctlyFormatsDuration(Duration duration, String expectedFormattedString) {
        assertEquals(expectedFormattedString, duration.getFormattedDuration());
    }
}
