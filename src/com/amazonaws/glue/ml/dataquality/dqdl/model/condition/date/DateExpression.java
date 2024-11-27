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
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

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
            return LocalDateTime.now(ZoneOffset.UTC);
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
            switch (duration.getUnit()) {
                case MINUTES:
                    return evaluateMinutes(
                            operator,
                            duration.getAmount(),
                            LocalDateTime.now(ZoneOffset.UTC)
                    );
                case HOURS:
                    return evaluateMinutes(
                            operator,
                            duration.getAmount() * 60,
                            LocalDateTime.now(ZoneOffset.UTC).withMinute(0)
                    );
                case DAYS:
                    return evaluateMinutes(
                            operator,
                            duration.getAmount() * 60 * 24,
                            LocalDateTime.now(ZoneOffset.UTC).withMinute(0)
                    );
                default:
                    throw new RuntimeException("Unsupported duration unit: " + duration.getUnit());
            }
        }

        private LocalDateTime evaluateMinutes(DateExpressionOperator operator, int minutes, LocalDateTime dt) {
            dt = dt.withSecond(0).withNano(0);
            switch (operator) {
                case MINUS:
                    return dt.minusMinutes(minutes);
                case PLUS:
                    return dt.plusMinutes(minutes);
                default:
                    return dt;
            }
        }
    }
}
