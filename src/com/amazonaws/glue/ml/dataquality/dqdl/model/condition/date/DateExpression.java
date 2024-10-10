/*
 * DateExpression.java
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
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;
import java.time.LocalDateTime;

@EqualsAndHashCode
public abstract class DateExpression implements Serializable {
    public abstract String getFormattedExpression();
    public abstract LocalDateTime getEvaluatedExpression();

    @AllArgsConstructor
    public static class StaticDate extends DateExpression {
        private final String date;

        @Override
        public String getFormattedExpression() {
            return "\"" + date + "\"";
        }

        @Override
        public LocalDateTime getEvaluatedExpression() {
            return LocalDateTime.parse(date + "T00:00:00");
        }
    }

    public static class CurrentDate extends DateExpression {
        @Override
        public String getFormattedExpression() {
            return "now()";
        }

        @Override
        public LocalDateTime getEvaluatedExpression() {
            return LocalDateTime.now();
        }
    }

    public enum DateExpressionOperator {
        MINUS,
        PLUS
    }

    @Getter
    @AllArgsConstructor
    public static class CurrentDateExpression extends DateExpression {
        private final DateExpressionOperator operator;
        private final Duration duration;

        @Override
        public String getFormattedExpression() {
            switch (operator) {
                case MINUS:
                    return String.format("(now() - %s)", duration.getFormattedDuration());
                case PLUS:
                    return String.format("(now() + %s)", duration.getFormattedDuration());
                default:
                    return "";
            }
        }

        @Override
        public LocalDateTime getEvaluatedExpression() {
            int hours = duration.getUnit().equals(DurationUnit.DAYS)
                ? duration.getAmount() * 24
                : duration.getAmount();

            LocalDateTime dt = LocalDateTime.now().withMinute(0).withSecond(0).withNano(0);
            switch (operator) {
                case MINUS:
                    return dt.minusHours(hours);
                case PLUS:
                    return dt.plusHours(hours);
                default:
                    return dt;
            }
        }
    }
}
