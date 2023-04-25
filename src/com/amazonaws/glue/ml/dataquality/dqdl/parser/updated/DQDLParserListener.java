/*
 * DQDLParserListener.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.parser.updated;

import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleLogicalOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleType;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.Condition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.date.DateBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.date.DateBasedConditionOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.date.DateExpression;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.duration.Duration;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.duration.DurationBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.duration.DurationBasedConditionOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.duration.DurationUnit;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number.NumberBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number.NumberBasedConditionOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringBasedConditionOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.updated.DQRule;
import com.amazonaws.glue.ml.dataquality.dqdl.model.updated.DQRuleset;
import com.amazonaws.glue.ml.dataquality.dqdl.parser.DQDLErrorListener;
import com.amazonaws.glue.ml.dataquality.dqdl.util.Either;
import com.amazonaws.glue.ml.dataquality.dqdl.updated.DataQualityDefinitionLanguageUpdatedBaseListener;
import com.amazonaws.glue.ml.dataquality.dqdl.updated.DataQualityDefinitionLanguageUpdatedParser;
import org.antlr.v4.runtime.RuleContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DQDLParserListener extends DataQualityDefinitionLanguageUpdatedBaseListener {
    private final DQDLErrorListener errorListener;
    private final List<String> errorMessages = new ArrayList<>();
    private final Map<String, String> metadata = new HashMap<>();

    private String primarySource;
    private List<String> additionalSources;
    private final List<DQRule> dqRules = new ArrayList<>();

    private static final String METADATA_VERSION_KEY = "Version";
    private static final Set<String> ALLOWED_METADATA_KEYS;

    private static final String PRIMARY_SOURCE_KEY = "Primary";
    private static final String ADDITIONAL_SOURCES_KEY = "AdditionalDataSources";
    private static final Set<String> ALLOWED_SOURCES_KEYS;

    static {
        ALLOWED_METADATA_KEYS = new HashSet<>();
        ALLOWED_METADATA_KEYS.add(METADATA_VERSION_KEY);

        ALLOWED_SOURCES_KEYS = new HashSet<>();
        ALLOWED_SOURCES_KEYS.add(PRIMARY_SOURCE_KEY);
        ALLOWED_SOURCES_KEYS.add(ADDITIONAL_SOURCES_KEY);
    }

    public DQDLParserListener(DQDLErrorListener errorListener) {
        this.errorListener = errorListener;
    }

    public Either<List<String>, DQRuleset> getParsedRuleset() {
        if (errorMessages.isEmpty() && errorListener.getErrorMessages().isEmpty()) {
            return Either.fromRight(new DQRuleset(metadata, primarySource, additionalSources, dqRules));
        } else {
            List<String> allErrorMessages = new ArrayList<>();
            allErrorMessages.addAll(errorMessages);
            allErrorMessages.addAll(errorListener.getErrorMessages());

            return Either.fromLeft(allErrorMessages);
        }
    }

    @Override
    public void enterMetadata(DataQualityDefinitionLanguageUpdatedParser.MetadataContext ctx) {
        for (DataQualityDefinitionLanguageUpdatedParser.PairContext pairContext
            : ctx.dictionary().pair()) {
            String key = removeEscapes(removeQuotes(pairContext.QUOTED_STRING().getText()));
            if (!ALLOWED_METADATA_KEYS.contains(key)) {
                errorMessages.add("Unsupported key provided in Metadata section");
                return;
            }

            String value = pairContext.pairValue().getText().replaceAll("\"", "");
            metadata.put(key, value);
        }
    }

    @Override
    public void enterRules(DataQualityDefinitionLanguageUpdatedParser.RulesContext ctx) {
        if (ctx.dqRules() == null) {
            errorMessages.add("No rules provided.");
        }
    }

    @Override
    public void enterDataSources(DataQualityDefinitionLanguageUpdatedParser.DataSourcesContext ctx) {
        for (DataQualityDefinitionLanguageUpdatedParser.PairContext pairContext
            : ctx.dictionary().pair()) {
            String key = removeEscapes(removeQuotes(pairContext.QUOTED_STRING().getText()));

            if (!ALLOWED_SOURCES_KEYS.contains(key)) {
                errorMessages.add("Unsupported key provided in Sources section");
                return;
            }

            if (PRIMARY_SOURCE_KEY.equals(key)) {
                primarySource = pairContext.pairValue().getText().replaceAll("\"", "");
            }

            if (ADDITIONAL_SOURCES_KEY.equals(key)) {
                if (pairContext.pairValue().array() == null) {
                    errorMessages.add("Additional sources must be an array of values.");
                } else {
                    additionalSources = new ArrayList<>();
                    String cleanedSources =
                        pairContext.pairValue().getText()
                            .replace("[", "")
                            .replace("]", "")
                            .replaceAll(" ", "")
                            .replaceAll("\"", "");

                    Collections.addAll(additionalSources, cleanedSources.split(","));
                }
            }
        }
    }

    @Override
    public void enterDqRules(DataQualityDefinitionLanguageUpdatedParser.DqRulesContext dqRulesContext) {
        if (!errorMessages.isEmpty()) {
            return;
        }

        for (DataQualityDefinitionLanguageUpdatedParser.TopLevelRuleContext tlc
            : dqRulesContext.topLevelRule()) {
            if (tlc.AND().size() > 0 || tlc.OR().size() > 0) {
                DQRuleLogicalOperator op = tlc.AND().size() > 0 ? DQRuleLogicalOperator.AND : DQRuleLogicalOperator.OR;
                List<DQRule> nestedRules = new ArrayList<>();

                for (DataQualityDefinitionLanguageUpdatedParser.DqRuleContext rc : tlc.dqRule()) {
                    Either<String, DQRule> dqRuleEither = getDQRule(rc);
                    if (dqRuleEither.isLeft()) {
                        errorMessages.add(dqRuleEither.getLeft());
                        return;
                    } else {
                        nestedRules.add(dqRuleEither.getRight());
                    }
                }

                dqRules.add(new DQRule("Composite", null, null, null, op, nestedRules));
            } else if (tlc.dqRule(0) != null) {
                Either<String, DQRule> dqRuleEither = getDQRule(tlc.dqRule(0));
                if (dqRuleEither.isLeft()) {
                    errorMessages.add(dqRuleEither.getLeft());
                    return;
                } else {
                    dqRules.add(dqRuleEither.getRight());
                }
            } else {
                errorMessages.add("No valid rule found");
                return;
            }
        }
    }

    private Either<String, DQRule> getDQRule(
        DataQualityDefinitionLanguageUpdatedParser.DqRuleContext dqRuleContext) {
        String ruleType = dqRuleContext.ruleType().getText();
        if (!DQRuleType.getRuleTypeMap().containsKey(ruleType)) {
            return Either.fromLeft(String.format("Rule Type: %s is not valid", ruleType));
        }

        DQRuleType dqRuleType = DQRuleType.getRuleTypeMap().get(ruleType);
        List<String> parameters = dqRuleContext.parameter().stream()
            .map(p -> p.getText().replaceAll("\"", ""))
            .collect(Collectors.toList());

        Optional<String> errorMessage = dqRuleType.verifyParameters(dqRuleType.getParameters(), parameters);

        if (errorMessage.isPresent()) {
            return Either.fromLeft(String.format(errorMessage.get() + ": %s", ruleType));
        }

        Map<String, String> parameterMap = dqRuleType.createParameterMap(dqRuleType.getParameters(), parameters);

        Condition condition;

        List<Either<String, Condition>> conditions = Arrays.stream(dqRuleType.getReturnType().split("\\|"))
            .map(rt -> parseCondition(dqRuleType, rt, dqRuleContext))
            .collect(Collectors.toList());

        Optional<Either<String, Condition>> optionalCondition = conditions.stream().filter(Either::isRight).findFirst();
        if (optionalCondition.isPresent()) {
            if (optionalCondition.get().isRight()) {
                condition = optionalCondition.get().getRight();
            } else {
                return Either.fromLeft(optionalCondition.get().getLeft());
            }
        } else {
            Optional<Either<String, Condition>> optionalFailedCondition =
                conditions.stream().filter(Either::isLeft).findFirst();
            if (optionalFailedCondition.isPresent()) {
                return Either.fromLeft(optionalFailedCondition.get().getLeft());
            } else {
                return Either.fromLeft(
                    String.format("Error while parsing condition for rule with rule type: %s", ruleType));
            }
        }

        Condition thresholdCondition = null;
        if (dqRuleContext.withThresholdCondition() != null) {
            if (dqRuleType.getSupportsThreshold()) {
                DataQualityDefinitionLanguageUpdatedParser.NumberBasedConditionContext ctx =
                    dqRuleContext.withThresholdCondition().numberBasedCondition();

                if (ctx == null) {
                    return Either.fromLeft(
                        String.format("Empty threshold condition provided for rule type: %s", ruleType));
                } else {
                    Optional<Condition> possibleCond =
                        parseNumberBasedCondition(dqRuleContext.withThresholdCondition().numberBasedCondition());
                    if (possibleCond.isPresent()) {
                        thresholdCondition = possibleCond.get();
                    } else {
                        return Either.fromLeft(
                            String.format("Unable to parse threshold condition provided for rule type: %s", ruleType));
                    }
                }

            } else {
                return Either.fromLeft(String.format("Threshold condition not supported for rule type: %s", ruleType));
            }
        }

        return Either.fromRight(
            new DQRule(dqRuleType.getRuleTypeName(), parameterMap, condition,
                thresholdCondition, DQRuleLogicalOperator.AND, new ArrayList<>())
        );
    }

    private Either<String, Condition> parseCondition(
        DQRuleType ruleType,
        String returnType,
        DataQualityDefinitionLanguageUpdatedParser.DqRuleContext dqRuleContext) {

        Either<String, Condition> response =
            Either.fromLeft(String.format("Error parsing condition for return type: %s", returnType));

        switch (returnType) {
            case "BOOLEAN":
                if (dqRuleContext.condition() != null) {
                    response = Either.fromLeft(
                        String.format("Unexpected condition for rule of type %s with boolean return type",
                            ruleType.getRuleTypeName()));
                } else {
                    response = Either.fromRight(new Condition(""));
                }
                break;
            case "NUMBER":
            case "NUMBER_ARRAY": {
                if (dqRuleContext.condition() == null || dqRuleContext.condition().numberBasedCondition() == null) {
                    response = Either.fromLeft(
                        String.format("Unexpected condition for rule of type %s with number return type",
                            ruleType.getRuleTypeName()));
                } else {
                    Optional<Condition> possibleCond =
                        parseNumberBasedCondition(dqRuleContext.condition().numberBasedCondition());

                    if (possibleCond.isPresent()) {
                        response = Either.fromRight(possibleCond.get());
                    }
                }
                break;
            }
            case "STRING":
            case "STRING_ARRAY": {
                if (dqRuleContext.condition() == null || dqRuleContext.condition().stringBasedCondition() == null) {
                    response = Either.fromLeft(
                        String.format("Unexpected condition for rule of type %s with string return type",
                            ruleType.getRuleTypeName()));
                } else {
                    Optional<Condition> possibleCond =
                        parseStringBasedCondition(dqRuleContext.condition().stringBasedCondition());

                    if (possibleCond.isPresent()) {
                        response = Either.fromRight(possibleCond.get());
                    }
                }
                break;
            }
            case "DATE":
            case "DATE_ARRAY": {
                if (dqRuleContext.condition() == null || dqRuleContext.condition().dateBasedCondition() == null) {
                    return Either.fromLeft(
                        String.format("Unexpected condition for rule of type %s with date return type",
                            ruleType.getRuleTypeName()));
                } else {
                    Optional<Condition> possibleCond =
                        parseDateBasedCondition(dqRuleContext.condition().dateBasedCondition());

                    if (possibleCond.isPresent()) {
                        response = Either.fromRight(possibleCond.get());
                    }
                }
                break;
            }
            case "DURATION":
            case "DURATION_ARRAY": {
                if (dqRuleContext.condition() == null || dqRuleContext.condition().durationBasedCondition() == null) {
                    return Either.fromLeft(
                        String.format("Unexpected condition for rule of type %s with duration return type",
                            ruleType.getRuleTypeName()));
                } else {
                    Optional<Condition> possibleCond =
                        parseDurationBasedCondition(dqRuleContext.condition().durationBasedCondition());

                    if (possibleCond.isPresent()) {
                        response = Either.fromRight(possibleCond.get());
                    }
                }
                break;
            }
            default:
                break;
        }

        return response;
    }

    private Optional<Condition> parseNumberBasedCondition(
        DataQualityDefinitionLanguageUpdatedParser.NumberBasedConditionContext ctx) {

        String exprStr = ctx.getText();
        Condition condition = null;

        if (ctx.BETWEEN() != null && ctx.number().size() == 2) {
            condition = new NumberBasedCondition(exprStr, NumberBasedConditionOperator.BETWEEN,
                Arrays.asList(ctx.number(0).getText(), ctx.number(1).getText()));
        } else if (ctx.GREATER_THAN_EQUAL_TO() != null && ctx.number().size() == 1) {
            condition = new NumberBasedCondition(exprStr, NumberBasedConditionOperator.GREATER_THAN_EQUAL_TO,
                Collections.singletonList(ctx.number(0).getText()));
        } else if (ctx.GREATER_THAN() != null && ctx.number().size() == 1) {
            condition = new NumberBasedCondition(exprStr, NumberBasedConditionOperator.GREATER_THAN,
                Collections.singletonList(ctx.number(0).getText()));
        } else if (ctx.LESS_THAN() != null && ctx.number().size() == 1) {
            condition = new NumberBasedCondition(exprStr, NumberBasedConditionOperator.LESS_THAN,
                Collections.singletonList(ctx.number(0).getText()));
        } else if (ctx.LESS_THAN_EQUAL_TO() != null && ctx.number().size() == 1) {
            condition = new NumberBasedCondition(exprStr, NumberBasedConditionOperator.LESS_THAN_EQUAL_TO,
                Collections.singletonList(ctx.number(0).getText()));
        } else if (ctx.EQUAL_TO() != null && ctx.number().size() == 1) {
            condition = new NumberBasedCondition(exprStr, NumberBasedConditionOperator.EQUALS,
                Collections.singletonList(ctx.number(0).getText()));
        } else if (ctx.IN() != null && ctx.numberArray() != null && ctx.numberArray().number().size() > 0) {
            List<String> numbers = ctx.numberArray().number().stream()
                .map(RuleContext::getText)
                .collect(Collectors.toList());

            condition = new NumberBasedCondition(exprStr, NumberBasedConditionOperator.IN, numbers);
        }

        return Optional.ofNullable(condition);
    }

    private Optional<Condition> parseStringBasedCondition(
        DataQualityDefinitionLanguageUpdatedParser.StringBasedConditionContext ctx
    ) {
        String exprStr = ctx.getText();
        Condition condition = null;

        if (ctx.EQUAL_TO() != null && ctx.quotedString() != null) {
            condition = new StringBasedCondition(exprStr, StringBasedConditionOperator.EQUALS,
                Collections.singletonList(removeQuotes(ctx.quotedString().QUOTED_STRING().getText())));
        } else if (ctx.IN() != null &&
            ctx.quotedStringArray() != null &&
            ctx.quotedStringArray().quotedString().size() > 0) {
            condition = new StringBasedCondition(exprStr, StringBasedConditionOperator.IN,
                ctx.quotedStringArray().quotedString().stream()
                    .map(s -> removeQuotes(removeEscapes(s.getText())))
                    .collect(Collectors.toList())
            );
        } else if (ctx.matchesRegexCondition() != null) {
            condition = new StringBasedCondition(exprStr, StringBasedConditionOperator.MATCHES,
                Collections.singletonList(removeQuotes(ctx.matchesRegexCondition().quotedString().getText())));
        }

        return Optional.ofNullable(condition);
    }

    private Optional<Condition> parseDateBasedCondition(
        DataQualityDefinitionLanguageUpdatedParser.DateBasedConditionContext ctx) {

        String exprStr = ctx.getText();
        Condition condition = null;

        if (ctx.BETWEEN() != null && ctx.dateExpression().size() == 2) {
            Optional<DateExpression> lower = parseDateExpression(ctx.dateExpression(0));
            Optional<DateExpression> upper = parseDateExpression(ctx.dateExpression(1));
            if (lower.isPresent() && upper.isPresent()) {
                condition = new DateBasedCondition(
                    exprStr, DateBasedConditionOperator.BETWEEN, Arrays.asList(lower.get(), upper.get())
                );
            }
        } else if (ctx.GREATER_THAN_EQUAL_TO() != null && ctx.dateExpression().size() == 1) {
            Optional<DateExpression> operand = parseDateExpression(ctx.dateExpression(0));
            if (operand.isPresent()) {
                condition = new DateBasedCondition(
                    exprStr, DateBasedConditionOperator.GREATER_THAN_EQUAL_TO, Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.GREATER_THAN() != null && ctx.dateExpression().size() == 1) {
            Optional<DateExpression> operand = parseDateExpression(ctx.dateExpression(0));
            if (operand.isPresent()) {
                condition = new DateBasedCondition(
                    exprStr, DateBasedConditionOperator.GREATER_THAN, Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.LESS_THAN() != null && ctx.dateExpression().size() == 1) {
            Optional<DateExpression> operand = parseDateExpression(ctx.dateExpression(0));
            if (operand.isPresent()) {
                condition = new DateBasedCondition(
                    exprStr, DateBasedConditionOperator.LESS_THAN, Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.LESS_THAN_EQUAL_TO() != null && ctx.dateExpression().size() == 1) {
            Optional<DateExpression> operand = parseDateExpression(ctx.dateExpression(0));
            if (operand.isPresent()) {
                condition = new DateBasedCondition(
                    exprStr, DateBasedConditionOperator.LESS_THAN_EQUAL_TO, Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.EQUAL_TO() != null && ctx.dateExpression().size() == 1) {
            Optional<DateExpression> operand = parseDateExpression(ctx.dateExpression(0));
            if (operand.isPresent()) {
                condition = new DateBasedCondition(
                    exprStr, DateBasedConditionOperator.EQUALS, Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.IN() != null &&
            ctx.dateExpressionArray() != null &&
            ctx.dateExpressionArray().dateExpression().size() > 0) {
            List<Optional<DateExpression>> expressions = ctx.dateExpressionArray().dateExpression().stream()
                .map(this::parseDateExpression)
                .collect(Collectors.toList());

            if (expressions.stream().allMatch(Optional::isPresent)) {
                condition = new DateBasedCondition(
                    exprStr, DateBasedConditionOperator.IN,
                    expressions.stream().map(Optional::get).collect(Collectors.toList())
                );
            }
        }

        return Optional.ofNullable(condition);
    }

    private Optional<Condition> parseDurationBasedCondition(
        DataQualityDefinitionLanguageUpdatedParser.DurationBasedConditionContext ctx
    ) {

        String exprStr = ctx.getText();
        Condition condition = null;

        if (ctx.BETWEEN() != null && ctx.durationExpression().size() == 2) {
            Optional<Duration> lower = parseDuration(ctx.durationExpression(0));
            Optional<Duration> upper = parseDuration(ctx.durationExpression(1));
            if (lower.isPresent() && upper.isPresent()) {
                condition = new DurationBasedCondition(
                    exprStr, DurationBasedConditionOperator.BETWEEN, Arrays.asList(lower.get(), upper.get())
                );
            }
        } else if (ctx.GREATER_THAN_EQUAL_TO() != null && ctx.durationExpression().size() == 1) {
            Optional<Duration> operand = parseDuration(ctx.durationExpression(0));
            if (operand.isPresent()) {
                condition = new DurationBasedCondition(
                    exprStr, DurationBasedConditionOperator.GREATER_THAN_EQUAL_TO,
                    Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.GREATER_THAN() != null && ctx.durationExpression().size() == 1) {
            Optional<Duration> operand = parseDuration(ctx.durationExpression(0));
            if (operand.isPresent()) {
                condition = new DurationBasedCondition(
                    exprStr, DurationBasedConditionOperator.GREATER_THAN,
                    Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.LESS_THAN() != null && ctx.durationExpression().size() == 1) {
            Optional<Duration> operand = parseDuration(ctx.durationExpression(0));
            if (operand.isPresent()) {
                condition = new DurationBasedCondition(
                    exprStr, DurationBasedConditionOperator.LESS_THAN,
                    Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.LESS_THAN_EQUAL_TO() != null && ctx.durationExpression().size() == 1) {
            Optional<Duration> operand = parseDuration(ctx.durationExpression(0));
            if (operand.isPresent()) {
                condition = new DurationBasedCondition(
                    exprStr, DurationBasedConditionOperator.LESS_THAN_EQUAL_TO,
                    Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.EQUAL_TO() != null && ctx.durationExpression().size() == 1) {
            Optional<Duration> operand = parseDuration(ctx.durationExpression(0));
            if (operand.isPresent()) {
                condition = new DurationBasedCondition(
                    exprStr, DurationBasedConditionOperator.EQUALS,
                    Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.IN() != null &&
            ctx.durationExpressionArray() != null &&
            ctx.durationExpressionArray().durationExpression().size() > 0) {

            List<Optional<Duration>> durations = ctx.durationExpressionArray().durationExpression().stream()
                .map(this::parseDuration)
                .collect(Collectors.toList());

            if (durations.stream().allMatch(Optional::isPresent)) {
                condition = new DurationBasedCondition(
                    exprStr, DurationBasedConditionOperator.IN,
                    durations.stream().map(Optional::get).collect(Collectors.toList())
                );
            }
        }

        return Optional.ofNullable(condition);
    }

    private Optional<DateExpression> parseDateExpression(
        DataQualityDefinitionLanguageUpdatedParser.DateExpressionContext ctx) {
        if (ctx.durationExpression() != null) {
            Optional<Duration> duration = parseDuration(ctx.durationExpression());
            return duration.map(value -> new DateExpression.CurrentDateExpression(
                ctx.dateExpressionOp().getText().equals("-")
                    ? DateExpression.DateExpressionOperator.MINUS
                    : DateExpression.DateExpressionOperator.PLUS,
                value
            ));
        } else if (ctx.dateNow() != null) {
            return Optional.of(new DateExpression.CurrentDate());
        } else {
            return Optional.of(new DateExpression.StaticDate(removeQuotes(ctx.DATE().getText())));
        }
    }

    private Optional<Duration> parseDuration(
        DataQualityDefinitionLanguageUpdatedParser.DurationExpressionContext ctx) {
        int amount = Integer.parseInt(ctx.INT() != null ? ctx.INT().getText() : ctx.DIGIT().getText());
        if (ctx.durationUnit().exception != null) {
            return Optional.empty();
        } else {
            DurationUnit unit = DurationUnit.valueOf(ctx.durationUnit().getText().toUpperCase());
            return Optional.of(new Duration(amount, unit));
        }
    }

    private String removeQuotes(String quotedString) {
        if (quotedString.startsWith("\"") && quotedString.endsWith("\"")) {
            quotedString = quotedString.substring(1);
            quotedString = quotedString.substring(0, quotedString.length() - 1);
        }
        return quotedString;
    }

    private String removeEscapes(String stringWithEscapes) {
        stringWithEscapes = stringWithEscapes.replaceAll("\\\\(.)", "$1");
        return stringWithEscapes;
    }
}
