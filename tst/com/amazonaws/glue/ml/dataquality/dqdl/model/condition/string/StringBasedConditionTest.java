/*
 * StringBasedConditionTest.java
 *
 * Copyright (c) 2024 Amazon.com, Inc. or its affiliates. All rights reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import static com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.Keyword.EMPTY;
import static com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.Keyword.NULL;
import static com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.Keyword.WHITESPACES_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringBasedConditionTest {

    private static Stream<Arguments> provideStringConditionsWithExpectedFormattedStrings() {
        return Stream.of(
                Arguments.of(
                        new StringBasedCondition(
                                "in[\"d\",\"a\",\"c\",\"b\"]",
                                StringBasedConditionOperator.IN,
                                Arrays.asList(
                                        new QuotedStringOperand("d"),
                                        new QuotedStringOperand("a"),
                                        new QuotedStringOperand("b"),
                                        new QuotedStringOperand("c")
                                )
                        ),
                        "in [\"d\",\"a\",\"b\",\"c\"]",
                        "in [\"a\",\"b\",\"c\",\"d\"]"
                ),
                Arguments.of(
                        new StringBasedCondition(
                                "notin[\"d\",\"a\",\"c\",\"b\"]",
                                StringBasedConditionOperator.NOT_IN,
                                Arrays.asList(
                                        new QuotedStringOperand("d"),
                                        new QuotedStringOperand("a"),
                                        new QuotedStringOperand("b"),
                                        new QuotedStringOperand("c")
                                )
                        ),
                        "not in [\"d\",\"a\",\"b\",\"c\"]",
                        "not in [\"a\",\"b\",\"c\",\"d\"]"
                ),
                // Test for Keyword values
                Arguments.of(
                        new StringBasedCondition(
                                "in[\"z\",\"a\",WHITESPACES_ONLY,EMPTY,\"c\",NULL]",
                                StringBasedConditionOperator.IN,
                                Arrays.asList(
                                        new QuotedStringOperand("z"),
                                        new QuotedStringOperand("a"),
                                        new KeywordStringOperand(WHITESPACES_ONLY),
                                        new KeywordStringOperand(EMPTY),
                                        new QuotedStringOperand("c"),
                                        new KeywordStringOperand(NULL)
                                )
                        ),
                        // verifying behavior that quoted strings will be sorted before keywords
                        "in [\"z\",\"a\",WHITESPACES_ONLY,EMPTY,\"c\",NULL]",
                        "in [\"a\",\"c\",\"z\",EMPTY,NULL,WHITESPACES_ONLY]"
                )
        );
    }

    @ParameterizedTest
    @MethodSource("provideStringConditionsWithExpectedFormattedStrings")
    public void test_correctlyFormatsString(StringBasedCondition condition,
                                              String expectedFormattedString,
                                              String expectedSortedFormattedString) {
        assertEquals(expectedFormattedString, condition.getFormattedCondition());
        assertEquals(expectedSortedFormattedString, condition.getSortedFormattedCondition());
    }
}
