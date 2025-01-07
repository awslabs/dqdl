/*
 * DateExpressionTest.java
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
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DateExpressionTest {
    @Test
    public void test_staticDateFormattedExpression() {
        String dt = "2023-01-01";
        DateExpression.StaticDate staticDate = new DateExpression.StaticDate("2023-01-01");
        assertEquals("\"" + dt + "\"", staticDate.getFormattedExpression());
    }

    @Test
    public void test_staticDateEvaluatedExpression() {
        String dt = "2023-01-01";
        DateExpression.StaticDate staticDate = new DateExpression.StaticDate("2023-01-01");
        assertTrue(staticDate.getEvaluatedExpression().toString().contains(dt));
    }

    @Test
    public void test_currentDateFormattedExpression() {
        DateExpression.CurrentDate currentDate = new DateExpression.CurrentDate();
        assertTrue(currentDate.getFormattedExpression().contains("now"));
    }

    @Test
    public void test_currentDateEvaluatedExpression() {
        DateExpression.CurrentDate currentDate = new DateExpression.CurrentDate();
        LocalDateTime dt = LocalDateTime.now(ZoneOffset.UTC);
        assertEquals(
            dt.toString().substring(0, 10),
            currentDate.getEvaluatedExpression().toString().substring(0, 10)
        );
    }

    @Test
    public void test_currentDateExpressionFormattedExpression() {
        DurationUnit unit = DurationUnit.HOURS;
        int amount = 24;
        Duration duration = new Duration(amount, unit);

        DateExpression.DateExpressionOperator operator =
            DateExpression.DateExpressionOperator.PLUS;

        DateExpression.CurrentDateExpression currentDateExpression =
            new DateExpression.CurrentDateExpression(operator, duration);

        assertTrue(
            currentDateExpression.getFormattedExpression().contains(
                String.format("%s %s", amount, unit.toString().toLowerCase())
            )
        );
    }

    @Test
    public void test_currentDateExpressionEvaluatedExpressionForMinutes() {
        DurationUnit unit = DurationUnit.MINUTES;
        int amount = 24;
        Duration duration = new Duration(amount, unit);

        DateExpression.DateExpressionOperator operator =
                DateExpression.DateExpressionOperator.PLUS;

        LocalDateTime currentDate = LocalDateTime.now(ZoneOffset.UTC).withSecond(0).withNano(0);
        DateExpression.CurrentDateExpression currentDateExpression =
                new DateExpression.CurrentDateExpression(operator, duration);

        long minutesDiff = ChronoUnit.MINUTES.between(
                currentDate, currentDateExpression.getEvaluatedExpression()
        );

        assertEquals(amount, minutesDiff);
    }

    @Test
    public void test_currentDateExpressionEvaluatedExpressionForHours() {
        DurationUnit unit = DurationUnit.HOURS;
        int amount = 24;
        Duration duration = new Duration(amount, unit);

        DateExpression.DateExpressionOperator operator =
            DateExpression.DateExpressionOperator.PLUS;

        LocalDateTime currentDate = LocalDateTime.now(ZoneOffset.UTC).withSecond(0).withNano(0);
        DateExpression.CurrentDateExpression currentDateExpression =
            new DateExpression.CurrentDateExpression(operator, duration);

        long hoursDiff = ChronoUnit.HOURS.between(
            currentDate, currentDateExpression.getEvaluatedExpression()
        );

        assertEquals(amount, hoursDiff);

        long minutesDiff = ChronoUnit.MINUTES.between(
            currentDate, currentDateExpression.getEvaluatedExpression()
        );

        assertEquals(amount * 60, minutesDiff);
    }

    @Test
    public void test_currentDateExpressionEvaluatedExpressionForDays() {
        DurationUnit unit = DurationUnit.DAYS;
        int amount = 2;
        Duration duration = new Duration(amount, unit);

        DateExpression.DateExpressionOperator operator =
            DateExpression.DateExpressionOperator.MINUS;

        LocalDateTime currentDate = LocalDateTime.now(ZoneOffset.UTC);
        DateExpression.CurrentDateExpression currentDateExpression =
            new DateExpression.CurrentDateExpression(operator, duration);

        long hoursDiff = ChronoUnit.HOURS.between(
            currentDate, currentDateExpression.getEvaluatedExpression()
        );

        assertTrue(amount * 24 + hoursDiff <= 1);

        long minutesDiff = ChronoUnit.MINUTES.between(
            currentDate, currentDateExpression.getEvaluatedExpression()
        );

        assertTrue(amount * 24 * 60 + minutesDiff <= 1);
    }
}
