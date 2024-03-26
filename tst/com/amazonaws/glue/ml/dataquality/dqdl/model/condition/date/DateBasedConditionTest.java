/*
 * DateBasedConditionTest.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.date;

import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.duration.Duration;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.duration.DurationUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DateBasedConditionTest {
    private static Stream<Arguments> provideDateBasedConditionsWithExpectedFormattedStrings() {
        return Stream.of(
            Arguments.of(
                new DateBasedCondition(
                    "between\"2023-01-01\"and\"2023-12-31\"",
                    DateBasedConditionOperator.BETWEEN,
                    Arrays.asList(
                        new DateExpression.StaticDate("2023-01-01"), new DateExpression.StaticDate("2023-12-31")
                    )
                ),
                "between \"2023-01-01\" and \"2023-12-31\"",
                "between \"2023-01-01\" and \"2023-12-31\""
            ),
            Arguments.of(
                new DateBasedCondition(
                    "notbetween\"2023-01-01\"and\"2023-12-31\"",
                    DateBasedConditionOperator.NOT_BETWEEN,
                    Arrays.asList(
                        new DateExpression.StaticDate("2023-01-01"), new DateExpression.StaticDate("2023-12-31")
                    )
                ),
                "not between \"2023-01-01\" and \"2023-12-31\"",
                "not between \"2023-01-01\" and \"2023-12-31\""
            ),
            Arguments.of(
                new DateBasedCondition(
                    "between(now()-4days)and(now()+72hours)",
                    DateBasedConditionOperator.BETWEEN,
                    Arrays.asList(
                        new DateExpression.CurrentDateExpression(
                            DateExpression.DateExpressionOperator.MINUS,new Duration(4, DurationUnit.DAYS)
                        ),
                        new DateExpression.CurrentDateExpression(
                            DateExpression.DateExpressionOperator.PLUS, new Duration(72, DurationUnit.HOURS)
                        )
                    )
                ),
                "between (now() - 4 days) and (now() + 72 hours)",
                "between (now() - 4 days) and (now() + 72 hours)"
            ),
            Arguments.of(
                new DateBasedCondition(
                    "notbetween(now()-4days)and(now()+72hours)",
                    DateBasedConditionOperator.NOT_BETWEEN,
                    Arrays.asList(
                        new DateExpression.CurrentDateExpression(
                            DateExpression.DateExpressionOperator.MINUS,new Duration(4, DurationUnit.DAYS)
                        ),
                        new DateExpression.CurrentDateExpression(
                            DateExpression.DateExpressionOperator.PLUS, new Duration(72, DurationUnit.HOURS)
                        )
                    )
                ),
                "not between (now() - 4 days) and (now() + 72 hours)",
                "not between (now() - 4 days) and (now() + 72 hours)"
            ),
            Arguments.of(
                new DateBasedCondition(
                    ">\"2023-01-01\"",
                    DateBasedConditionOperator.GREATER_THAN,
                    Collections.singletonList(new DateExpression.StaticDate("2023-01-01"))
                ),
                "> \"2023-01-01\"",
                "> \"2023-01-01\""
            ),
            Arguments.of(
                new DateBasedCondition(
                    ">now()",
                    DateBasedConditionOperator.GREATER_THAN,
                    Collections.singletonList(new DateExpression.CurrentDate())
                ),
                "> now()",
                "> now()"
            ),
            Arguments.of(
                new DateBasedCondition(
                    ">=\"2023-01-01\"",
                    DateBasedConditionOperator.GREATER_THAN_EQUAL_TO,
                    Collections.singletonList(new DateExpression.StaticDate("2023-01-01"))
                ),
                ">= \"2023-01-01\"",
                ">= \"2023-01-01\""
            ),
            Arguments.of(
                new DateBasedCondition(
                    ">=(now()-2days)",
                    DateBasedConditionOperator.GREATER_THAN_EQUAL_TO,
                    Collections.singletonList(
                        new DateExpression.CurrentDateExpression(
                            DateExpression.DateExpressionOperator.MINUS, new Duration(2, DurationUnit.DAYS)
                        )
                    )
                ),
                ">= (now() - 2 days)",
                ">= (now() - 2 days)"
            ),
            Arguments.of(
                new DateBasedCondition(
                    "<now()",
                    DateBasedConditionOperator.LESS_THAN,
                    Collections.singletonList(new DateExpression.CurrentDate())
                ),
                "< now()",
                "< now()"
            ),
            Arguments.of(
                new DateBasedCondition(
                    "<(now()+100days)",
                    DateBasedConditionOperator.LESS_THAN,
                    Collections.singletonList(
                        new DateExpression.CurrentDateExpression(
                            DateExpression.DateExpressionOperator.PLUS, new Duration(100, DurationUnit.DAYS)
                        )
                    )
                ),
                "< (now() + 100 days)",
                "< (now() + 100 days)"
            ),
            Arguments.of(
                new DateBasedCondition(
                    "<=\"2023-01-01\"",
                    DateBasedConditionOperator.LESS_THAN_EQUAL_TO,
                    Collections.singletonList(new DateExpression.StaticDate("2023-01-01"))
                ),
                "<= \"2023-01-01\"",
                "<= \"2023-01-01\""
            ),
            Arguments.of(
                new DateBasedCondition(
                    "<=now()",
                    DateBasedConditionOperator.LESS_THAN_EQUAL_TO,
                    Collections.singletonList(new DateExpression.CurrentDate())
                ),
                "<= now()",
                "<= now()"
            ),
            Arguments.of(
                new DateBasedCondition(
                    ">=\"2023-01-01\"",
                    DateBasedConditionOperator.EQUALS,
                    Collections.singletonList(new DateExpression.StaticDate("2023-01-01"))
                ),
                "= \"2023-01-01\"",
                "= \"2023-01-01\""
            ),
            Arguments.of(
                new DateBasedCondition(
                    "!=\"2023-01-01\"",
                    DateBasedConditionOperator.NOT_EQUALS,
                    Collections.singletonList(new DateExpression.StaticDate("2023-01-01"))
                ),
                "!= \"2023-01-01\"",
                "!= \"2023-01-01\""
            ),
            Arguments.of(
                new DateBasedCondition(
                    ">=(now()-2days)",
                    DateBasedConditionOperator.EQUALS,
                    Collections.singletonList(
                        new DateExpression.CurrentDateExpression(
                            DateExpression.DateExpressionOperator.MINUS, new Duration(2, DurationUnit.DAYS)
                        )
                    )
                ),
                "= (now() - 2 days)",
                "= (now() - 2 days)"
            ),
            Arguments.of(
                new DateBasedCondition(
                    "in[\"2023-01-01\",now(),(now()-2days),(now()+72hours)]",
                    DateBasedConditionOperator.IN,
                    Arrays.asList(
                        new DateExpression.StaticDate("2023-01-01"),
                        new DateExpression.CurrentDate(),
                        new DateExpression.CurrentDateExpression(
                            DateExpression.DateExpressionOperator.MINUS, new Duration(2, DurationUnit.DAYS)
                        ),
                        new DateExpression.CurrentDateExpression(
                            DateExpression.DateExpressionOperator.PLUS, new Duration(72, DurationUnit.HOURS)
                        )
                    )
                ),
                "in [\"2023-01-01\",now(),(now() - 2 days),(now() + 72 hours)]",
                "in [\"2023-01-01\",(now() + 72 hours),(now() - 2 days),now()]"
            ),
            Arguments.of(
                    new DateBasedCondition(
                            "in[\"2023-01-01\",\"2022-01-01\",\"2021-01-01\",\"2020-01-01\"]",
                            DateBasedConditionOperator.IN,
                            Arrays.asList(
                                    new DateExpression.StaticDate("2023-01-01"),
                                    new DateExpression.StaticDate("2022-01-01"),
                                    new DateExpression.StaticDate("2021-01-01"),
                                    new DateExpression.StaticDate("2020-01-01")
                            )
                    ),
                    "in [\"2023-01-01\",\"2022-01-01\",\"2021-01-01\",\"2020-01-01\"]",
                    "in [\"2020-01-01\",\"2021-01-01\",\"2022-01-01\",\"2023-01-01\"]"
            ),
            Arguments.of(
                new DateBasedCondition(
                    "notin[\"2023-01-01\",now(),(now()-2days),(now()+72hours)]",
                    DateBasedConditionOperator.NOT_IN,
                    Arrays.asList(
                        new DateExpression.StaticDate("2023-01-01"),
                        new DateExpression.CurrentDate(),
                        new DateExpression.CurrentDateExpression(
                            DateExpression.DateExpressionOperator.MINUS, new Duration(2, DurationUnit.DAYS)
                        ),
                        new DateExpression.CurrentDateExpression(
                            DateExpression.DateExpressionOperator.PLUS, new Duration(72, DurationUnit.HOURS)
                        )
                    )
                ),
                "not in [\"2023-01-01\",now(),(now() - 2 days),(now() + 72 hours)]",
                "not in [\"2023-01-01\",(now() + 72 hours),(now() - 2 days),now()]"
            ),
            Arguments.of(
                    new DateBasedCondition(
                            "notin[\"2023-01-01\",\"2022-01-01\",\"2021-01-01\",\"2020-01-01\"]",
                            DateBasedConditionOperator.NOT_IN,
                            Arrays.asList(
                                    new DateExpression.StaticDate("2023-01-01"),
                                    new DateExpression.StaticDate("2022-01-01"),
                                    new DateExpression.StaticDate("2021-01-01"),
                                    new DateExpression.StaticDate("2020-01-01")
                            )
                    ),
                    "not in [\"2023-01-01\",\"2022-01-01\",\"2021-01-01\",\"2020-01-01\"]",
                    "not in [\"2020-01-01\",\"2021-01-01\",\"2022-01-01\",\"2023-01-01\"]"
            )
        );
    }

    @ParameterizedTest
    @MethodSource("provideDateBasedConditionsWithExpectedFormattedStrings")
    public void test_correctlyFormatsDuration(DateBasedCondition condition,
                                              String expectedFormattedString,
                                              String expectedSortedFormattedString) {
        assertEquals(expectedFormattedString, condition.getFormattedCondition());
        assertEquals(expectedSortedFormattedString, condition.getSortedFormattedCondition());
    }
}
