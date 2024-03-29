/*
 * NumberBasedConditionTest.java
 *
 * Copyright (c) 2024 Amazon.com, Inc. or its affiliates. All rights reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NumberBasedConditionTest {
    private static Stream<Arguments> provideNumberConditionsWithExpectedFormattedStrings() {
        return Stream.of(
                Arguments.of(
                        new NumberBasedCondition(
                                "in[15,10,20,5]",
                                NumberBasedConditionOperator.IN,
                                Arrays.asList(
                                        new AtomicNumberOperand("15"),
                                        new AtomicNumberOperand("10"),
                                        new AtomicNumberOperand("20"),
                                        new AtomicNumberOperand("5")
                                )
                        ),
                        "in [15,10,20,5]",
                        "in [5,10,15,20]"
                ),
                Arguments.of(
                        new NumberBasedCondition(
                                "in[1.5,1.0,2.0,0.5]",
                                NumberBasedConditionOperator.IN,
                                Arrays.asList(
                                        new AtomicNumberOperand("1.5"),
                                        new AtomicNumberOperand("1.0"),
                                        new AtomicNumberOperand("2.0"),
                                        new AtomicNumberOperand("0.5")
                                )
                        ),
                        "in [1.5,1.0,2.0,0.5]",
                        "in [0.5,1.0,1.5,2.0]"
                ),
                Arguments.of(
                        new NumberBasedCondition(
                                "notin[15,10,20,5]",
                                NumberBasedConditionOperator.NOT_IN,
                                Arrays.asList(
                                        new AtomicNumberOperand("15"),
                                        new AtomicNumberOperand("10"),
                                        new AtomicNumberOperand("20"),
                                        new AtomicNumberOperand("5")
                                )
                        ),
                        "not in [15,10,20,5]",
                        "not in [5,10,15,20]"
                ),
                Arguments.of(
                        new NumberBasedCondition(
                                "in[15,10,NULL,20,5]",
                                NumberBasedConditionOperator.IN,
                                Arrays.asList(
                                        new AtomicNumberOperand("15"),
                                        new AtomicNumberOperand("10"),
                                        new NullNumericOperand("NULL"),
                                        new AtomicNumberOperand("20"),
                                        new AtomicNumberOperand("5")
                                )
                        ),
                        "in [15,10,NULL,20,5]",
                        "in [5,10,15,20,NULL]"
                ),
                // We don't limit customers from adding multiple NULL keywords
                Arguments.of(
                        new NumberBasedCondition(
                                "in[15,10,NULL,NULL,5]",
                                NumberBasedConditionOperator.IN,
                                Arrays.asList(
                                        new AtomicNumberOperand("15"),
                                        new AtomicNumberOperand("10"),
                                        new NullNumericOperand("NULL"),
                                        new NullNumericOperand("NULL"),
                                        new AtomicNumberOperand("5")
                                )
                        ),
                        "in [15,10,NULL,NULL,5]",
                        "in [5,10,15,NULL,NULL]"
                )
        );
    }

    @ParameterizedTest
    @MethodSource("provideNumberConditionsWithExpectedFormattedStrings")
    public void test_correctlyFormatsNumber(NumberBasedCondition condition,
                                              String expectedFormattedString,
                                              String expectedSortedFormattedString) {
        assertEquals(expectedFormattedString, condition.getFormattedCondition());
        assertEquals(expectedSortedFormattedString, condition.getSortedFormattedCondition());
    }
}
