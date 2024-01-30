/*
 * DurationBasedConditionTest.java
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

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DurationBasedConditionTest {
    private static Stream<Arguments> provideDurationConditionsWithExpectedFormattedStrings() {
        return Stream.of(
            Arguments.of(
                new DurationBasedCondition(
                    "between3hoursand4days",
                    DurationBasedConditionOperator.BETWEEN,
                    Arrays.asList(
                        new Duration(3, DurationUnit.HOURS),
                        new Duration(4, DurationUnit.DAYS)
                    )
                ),
                "between 3 hours and 4 days"
            ),
            Arguments.of(
                new DurationBasedCondition(
                    "notbetween3hoursand4days",
                    DurationBasedConditionOperator.NOT_BETWEEN,
                    Arrays.asList(
                        new Duration(3, DurationUnit.HOURS),
                        new Duration(4, DurationUnit.DAYS)
                    )
                ),
                "not between 3 hours and 4 days"
            ),
            Arguments.of(
                new DurationBasedCondition(
                    ">256hours",
                    DurationBasedConditionOperator.GREATER_THAN,
                    Collections.singletonList(new Duration(256, DurationUnit.HOURS))
                ),
                "> 256 hours"
            ),
            Arguments.of(
                new DurationBasedCondition(
                    ">=2days",
                    DurationBasedConditionOperator.GREATER_THAN_EQUAL_TO,
                    Collections.singletonList(new Duration(2, DurationUnit.DAYS))
                ),
                ">= 2 days"
            ),
            Arguments.of(
                new DurationBasedCondition(
                    "<25000hours",
                    DurationBasedConditionOperator.LESS_THAN,
                    Collections.singletonList(new Duration(25000, DurationUnit.HOURS))
                ),
                "< 25000 hours"
            ),
            Arguments.of(
                new DurationBasedCondition(
                    "<=24days",
                    DurationBasedConditionOperator.LESS_THAN_EQUAL_TO,
                    Collections.singletonList(new Duration(24, DurationUnit.DAYS))
                ),
                "<= 24 days"
            ),
            Arguments.of(
                new DurationBasedCondition(
                    "=10days",
                    DurationBasedConditionOperator.EQUALS,
                    Collections.singletonList(new Duration(10, DurationUnit.DAYS))
                ),
                "= 10 days"
            ),
            Arguments.of(
                new DurationBasedCondition(
                    "!=10days",
                    DurationBasedConditionOperator.NOT_EQUALS,
                    Collections.singletonList(new Duration(10, DurationUnit.DAYS))
                ),
                "!= 10 days"
            ),
            Arguments.of(
                new DurationBasedCondition(
                    "in[3hours,4days,96hours,7days]",
                    DurationBasedConditionOperator.IN,
                    Arrays.asList(
                        new Duration(3, DurationUnit.HOURS),
                        new Duration(4, DurationUnit.DAYS),
                        new Duration(96, DurationUnit.HOURS),
                        new Duration(7, DurationUnit.DAYS)
                    )
                ),
                "in [3 hours, 4 days, 96 hours, 7 days]"
            ),
            Arguments.of(
                new DurationBasedCondition(
                    "notin[3hours,4days,96hours,7days]",
                    DurationBasedConditionOperator.NOT_IN,
                    Arrays.asList(
                        new Duration(3, DurationUnit.HOURS),
                        new Duration(4, DurationUnit.DAYS),
                        new Duration(96, DurationUnit.HOURS),
                        new Duration(7, DurationUnit.DAYS)
                    )
                ),
                "not in [3 hours, 4 days, 96 hours, 7 days]"
            )
        );
    }

    @ParameterizedTest
    @MethodSource("provideDurationConditionsWithExpectedFormattedStrings")
    public void test_correctlyFormatsDuration(DurationBasedCondition condition,
                                              String expectedFormattedString) {
        assertEquals(expectedFormattedString, condition.getFormattedCondition());
    }
}
