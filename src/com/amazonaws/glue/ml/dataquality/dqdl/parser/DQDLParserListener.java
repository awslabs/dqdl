/*
 * DQDLParserListener.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.parser;

import com.amazonaws.glue.ml.dataquality.dqdl.model.DQAnalyzer;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleLogicalOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleParameterValue;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleType;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.Condition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.date.DateBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.date.DateBasedConditionOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.date.DateExpression;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.date.NullDateExpression;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.duration.Duration;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.duration.DurationBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.duration.DurationBasedConditionOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.duration.DurationUnit;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number.AtomicNumberOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number.BinaryExpressionOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number.FunctionCallOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number.NullNumericOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number.NumberBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number.NumberBasedConditionOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number.NumericOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.QuotedStringOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.KeywordStringOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.Keyword;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringBasedConditionOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRule;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleset;
import com.amazonaws.glue.ml.dataquality.dqdl.util.Either;
import com.amazonaws.glue.ml.dataquality.dqdl.DataQualityDefinitionLanguageBaseListener;
import com.amazonaws.glue.ml.dataquality.dqdl.DataQualityDefinitionLanguageParser;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DQDLParserListener extends DataQualityDefinitionLanguageBaseListener {
    private final DQDLErrorListener errorListener;
    private final List<String> errorMessages = new ArrayList<>();
    private final Map<String, String> metadata = new HashMap<>();

    private String primarySource;
    private List<String> additionalSources;
    private final List<DQRule> dqRules = new ArrayList<>();
    private final List<DQAnalyzer> dqAnalyzers = new ArrayList<>();

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
        // Only add this error message if we did not walk the tree due to empty rules or analyzers sections.
        if (errorMessages.isEmpty() && dqRules.isEmpty() && dqAnalyzers.isEmpty()) {
            errorMessages.add("No rules or analyzers provided.");
        }

        if (errorMessages.isEmpty() && errorListener.getErrorMessages().isEmpty()) {
            return Either.fromRight(new DQRuleset(metadata, primarySource, additionalSources, dqRules, dqAnalyzers));
        } else {
            List<String> allErrorMessages = new ArrayList<>();
            allErrorMessages.addAll(errorMessages);
            allErrorMessages.addAll(errorListener.getErrorMessages());

            return Either.fromLeft(allErrorMessages);
        }
    }

    @Override
    public void enterMetadata(DataQualityDefinitionLanguageParser.MetadataContext ctx) {
        // The logic below, just above the loop is a guard against an NPE caused by empty dictionaries.
        // Need to investigate why dictionaryContext.pair() returns 1 element,
        // which is an empty string, for an empty dictionary.
        // We would not have this problem if dictionaryContext.pair() returned 0 entries in the list.
        DataQualityDefinitionLanguageParser.DictionaryContext dictionaryContext = ctx.dictionary();
        List<String> dictionaryErrors = validateDictionary(dictionaryContext);
        if (!dictionaryErrors.isEmpty()) {
            errorMessages.addAll(dictionaryErrors);
            return;
        }

        for (DataQualityDefinitionLanguageParser.PairContext pairContext: dictionaryContext.pair()) {
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
    public void enterDataSources(DataQualityDefinitionLanguageParser.DataSourcesContext ctx) {
        DataQualityDefinitionLanguageParser.DictionaryContext dictionaryContext = ctx.dictionary();
        List<String> dictionaryErrors = validateDictionary(dictionaryContext);
        if (!dictionaryErrors.isEmpty()) {
            errorMessages.addAll(dictionaryErrors);
            return;
        }

        for (DataQualityDefinitionLanguageParser.PairContext pairContext: dictionaryContext.pair()) {
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
    public void enterDqRules(DataQualityDefinitionLanguageParser.DqRulesContext dqRulesContext) {
        if (!errorMessages.isEmpty()) {
            return;
        }

        for (DataQualityDefinitionLanguageParser.TopLevelRuleContext tlc
            : dqRulesContext.topLevelRule()) {
            if (tlc.AND().size() > 0 || tlc.OR().size() > 0) {
                DQRuleLogicalOperator op = tlc.AND().size() > 0 ? DQRuleLogicalOperator.AND : DQRuleLogicalOperator.OR;
                List<DQRule> nestedRules = new ArrayList<>();

                for (DataQualityDefinitionLanguageParser.DqRuleContext rc : tlc.dqRule()) {
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

    @Override
    public void enterDqAnalyzers(DataQualityDefinitionLanguageParser.DqAnalyzersContext dqAnalyzersContext) {
        if (!errorMessages.isEmpty()) {
            return;
        }

        for (DataQualityDefinitionLanguageParser.DqAnalyzerContext dac: dqAnalyzersContext.dqAnalyzer()) {
            Either<String, DQAnalyzer> dqAnalyzerEither = getDQAnalyzer(dac);
            if (dqAnalyzerEither.isLeft()) {
                errorMessages.add(dqAnalyzerEither.getLeft());
                return;
            } else {
                dqAnalyzers.add(dqAnalyzerEither.getRight());
            }
        }
    }

    private Either<String, DQRule> getDQRule(
        DataQualityDefinitionLanguageParser.DqRuleContext dqRuleContext) {
        String ruleType = dqRuleContext.ruleType().getText();

        List<DQRuleParameterValue> parameters = parseParameters(dqRuleContext.parameterWithConnectorWord());

        Optional<DQRuleType> optionalDQRuleType = DQRuleType.getRuleType(ruleType, parameters.size());

        if (!optionalDQRuleType.isPresent()) {
            return Either.fromLeft(String.format("Rule Type: %s is not valid", ruleType));
        }

        DQRuleType dqRuleType = optionalDQRuleType.get();

        if (dqRuleType.isAnalyzerOnly()) {
            return Either.fromLeft(String.format("Analyzer Type: %s is not supported in rules section", ruleType));
        }

        Optional<String> errorMessage = dqRuleType.verifyParameters(dqRuleType.getParameters(), parameters);

        if (errorMessage.isPresent()) {
            return Either.fromLeft(String.format(errorMessage.get() + ": %s", ruleType));
        }

        LinkedHashMap<String, DQRuleParameterValue> parameterMap =
            dqRuleType.createParameterMap(dqRuleType.getParameters(), parameters);

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

        Condition thresholdCondition = null, hashAlgoCondition = null;
        Boolean dataFrameCondition = null;
        List<DataQualityDefinitionLanguageParser.WithConditionContext> conditionContexts =
            dqRuleContext.withCondition() != null
                    ? dqRuleContext.withCondition().stream().filter(Objects::nonNull).collect(Collectors.toList())
                    : Collections.emptyList();

        for (DataQualityDefinitionLanguageParser.WithConditionContext conditionContext : conditionContexts) {
            if (conditionContext.withThresholdCondition() != null) {
                if (dqRuleType.isThresholdSupported()) {
                    DataQualityDefinitionLanguageParser.NumberBasedConditionContext ctx =
                            conditionContext.withThresholdCondition().numberBasedCondition();

                    if (ctx == null) {
                        return Either.fromLeft(
                                String.format("Empty threshold condition provided for rule type: %s", ruleType));
                    } else {
                        Optional<Condition> possibleCond =
                                parseNumberBasedCondition(conditionContext
                                        .withThresholdCondition().numberBasedCondition());
                        if (possibleCond.isPresent()) {
                            thresholdCondition = possibleCond.get();
                        } else {
                            return Either.fromLeft(
                                    String.format("Unable to parse threshold condition " +
                                            "provided for rule type: %s", ruleType));
                        }
                    }

                } else {
                    return Either.fromLeft(String.format("Threshold condition not supported " +
                            "for rule type: %s", ruleType));
                }
            }

            if (conditionContext.withHashAlgorithmCondition() != null) {
                if (dqRuleType.isHashAlgoSupported()) {
                    DataQualityDefinitionLanguageParser.StringBasedConditionContext ctx =
                            conditionContext.withHashAlgorithmCondition().stringBasedCondition();

                    if (ctx == null) {
                        return Either.fromLeft(
                                String.format("Empty algorithm condition provided for rule type: %s", ruleType));
                    } else {
                        Optional<Condition> possibleCond =
                                parseStringBasedCondition(conditionContext
                                        .withHashAlgorithmCondition().stringBasedCondition());
                        if (possibleCond.isPresent()) {
                            hashAlgoCondition = possibleCond.get();
                        } else {
                            return Either.fromLeft(
                                    String.format("Unable to parse algorithm condition provided for rule type: %s",
                                            ruleType));
                        }
                    }
                } else {
                    return Either.fromLeft(String.format("Algorithm condition " +
                            "not supported for rule type: %s", ruleType));
                }
            }

            if (conditionContext.withDataFrameCondition() != null) {
                //all hashAlgo rules also support inferring from dataframes
                if (dqRuleType.isHashAlgoSupported()) {
                    dataFrameCondition = true;
                } else {
                    return Either.fromLeft(String.format("DataFrame condition not supported " +
                            "for rule type: %s", ruleType));
                }
            }
        }

        String whereClause = null;
        if (dqRuleContext.whereClause() != null) {
            if (dqRuleType.isWhereClauseSupported()) {
                DataQualityDefinitionLanguageParser.WhereClauseContext ctx = dqRuleContext.whereClause();
                if (ctx.quotedString().getText().isEmpty() || ctx.quotedString().getText().equals("\"\"")) {
                    return Either.fromLeft(
                            String.format("Empty where condition provided for rule type: %s", ruleType));
                } else {
                    whereClause = removeQuotes(ctx.quotedString().getText());
                }
            } else {
                return Either.fromLeft(String.format("Where clause is not supported for rule type: %s", ruleType));
            }
        }

        return Either.fromRight(
            DQRule.createFromParameterValueMap(
                dqRuleType, parameterMap, condition, thresholdCondition, hashAlgoCondition,
                    whereClause, dataFrameCondition)
        );
    }

    private Either<String, DQAnalyzer> getDQAnalyzer(
        DataQualityDefinitionLanguageParser.DqAnalyzerContext dqAnalyzerContext) {
        String analyzerType = dqAnalyzerContext.analyzerType().getText();

        List<DQRuleParameterValue> parameters = parseParameters(dqAnalyzerContext.parameterWithConnectorWord());

        // We just use the DQ Rule names to validate what analyzer names to allow.
        // This might change closer to re:Invent, but keeping it simple for now.
        Optional<DQRuleType> optionalDQAnalyzerType = DQRuleType.getRuleType(analyzerType, parameters.size());

        if (!optionalDQAnalyzerType.isPresent()) {
            return Either.fromLeft(String.format("Analyzer Type: %s is not valid", analyzerType));
        }

        DQRuleType dqRuleType = optionalDQAnalyzerType.get();

        if (dqRuleType.getReturnType().equals("BOOLEAN")) {
            return Either.fromLeft(String.format("Analyzer Type: %s is not supported", analyzerType));
        }

        Optional<String> errorMessage = dqRuleType.verifyParameters(dqRuleType.getParameters(), parameters);

        if (errorMessage.isPresent()) {
            return Either.fromLeft(String.format(errorMessage.get() + ": %s", analyzerType));
        }

        LinkedHashMap<String, DQRuleParameterValue> parameterMap =
            dqRuleType.createParameterMap(dqRuleType.getParameters(), parameters);

        return Either.fromRight(DQAnalyzer.createFromValueMap(analyzerType, parameterMap));
    }

    private Either<String, Condition> parseCondition(
        DQRuleType ruleType,
        String returnType,
        DataQualityDefinitionLanguageParser.DqRuleContext dqRuleContext) {

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
        DataQualityDefinitionLanguageParser.NumberBasedConditionContext ctx) {

        String exprStr = ctx.getText();
        Condition condition = null;

        if (ctx.BETWEEN() != null && ctx.number().size() == 2) {
            Optional<NumericOperand> operand1 = parseNumericOperand(ctx.number(0), false);
            Optional<NumericOperand> operand2 = parseNumericOperand(ctx.number(1), false);

            if (operand1.isPresent() && operand2.isPresent()) {
                NumberBasedConditionOperator op = (ctx.NOT() != null) ?
                    NumberBasedConditionOperator.NOT_BETWEEN
                    : NumberBasedConditionOperator.BETWEEN;
                condition = new NumberBasedCondition(exprStr, op, Arrays.asList(operand1.get(), operand2.get()));
            }
        } else if (ctx.GREATER_THAN_EQUAL_TO() != null && ctx.number().size() == 1) {
            Optional<NumericOperand> operand = parseNumericOperand(ctx.number(0), false);
            if (operand.isPresent()) {
                condition = new NumberBasedCondition(
                    exprStr, NumberBasedConditionOperator.GREATER_THAN_EQUAL_TO,
                    Collections.singletonList(operand.get()));
            }
        } else if (ctx.GREATER_THAN() != null && ctx.number().size() == 1) {
            Optional<NumericOperand> operand = parseNumericOperand(ctx.number(0), false);
            if (operand.isPresent()) {
                condition = new NumberBasedCondition(
                    exprStr, NumberBasedConditionOperator.GREATER_THAN,
                    Collections.singletonList(operand.get()));
            }
        } else if (ctx.LESS_THAN() != null && ctx.number().size() == 1) {
            Optional<NumericOperand> operand = parseNumericOperand(ctx.number(0), false);
            if (operand.isPresent()) {
                condition = new NumberBasedCondition(
                    exprStr, NumberBasedConditionOperator.LESS_THAN,
                    Collections.singletonList(operand.get()));
            }
        } else if (ctx.LESS_THAN_EQUAL_TO() != null && ctx.number().size() == 1) {
            Optional<NumericOperand> operand = parseNumericOperand(ctx.number(0), false);
            if (operand.isPresent()) {
                condition = new NumberBasedCondition(
                    exprStr, NumberBasedConditionOperator.LESS_THAN_EQUAL_TO,
                    Collections.singletonList(operand.get()));
            }
        } else if (ctx.EQUAL_TO() != null && ctx.number().size() == 1) {
            Optional<NumericOperand> operand = parseNumericOperand(ctx.number(0), false);
            if (operand.isPresent()) {
                NumberBasedConditionOperator op = (ctx.NEGATION() != null) ?
                    NumberBasedConditionOperator.NOT_EQUALS
                    : NumberBasedConditionOperator.EQUALS;
                condition = new NumberBasedCondition(
                    exprStr, op, Collections.singletonList(operand.get()));
            }
        } else if (ctx.IN() != null && ctx.numberArray() != null && ctx.numberArray().number().size() > 0) {
            List<Optional<NumericOperand>> numbers = ctx.numberArray().number()
                .stream()
                .map(op -> parseNumericOperand(op, false))
                .collect(Collectors.toList());

            if (numbers.stream().allMatch(Optional::isPresent)) {
                NumberBasedConditionOperator op = (ctx.NOT() != null) ?
                    NumberBasedConditionOperator.NOT_IN
                    : NumberBasedConditionOperator.IN;
                condition = new NumberBasedCondition(exprStr, op,
                    numbers.stream().map(Optional::get).collect(Collectors.toList()));
            }
        }

        return Optional.ofNullable(condition);
    }

    private Optional<NumericOperand> parseNumericOperand(
        DataQualityDefinitionLanguageParser.NumberContext numberContext, boolean isParenthesized
    ) {
        if (numberContext.numberOp() != null) {
            Optional<NumericOperand> operand1 = parseNumericOperand(numberContext.number(0), false);
            Optional<NumericOperand> operand2 = parseNumericOperand(numberContext.number(1), false);
            if (operand1.isPresent() && operand2.isPresent()) {
                return Optional.of(
                    new BinaryExpressionOperand(
                        numberContext.getText(),
                        numberContext.numberOp().getText(),
                        operand1.get(), operand2.get(),
                        isParenthesized
                    )
                );
            } else {
                return Optional.empty();
            }
        } else if (numberContext.functionCall() != null) {
            DataQualityDefinitionLanguageParser.FunctionCallContext fcc = numberContext.functionCall();
            String functionName = fcc.IDENTIFIER().getText();
            List<NumericOperand> functionParameters = new ArrayList<>();

            if (fcc.functionParameters() != null) {
                List<Optional<NumericOperand>> parameters = fcc.functionParameters().number()
                    .stream()
                    .map(op -> parseNumericOperand(op, false))
                    .collect(Collectors.toList());

                if (parameters.stream().allMatch(Optional::isPresent)) {
                    functionParameters = parameters.stream().map(Optional::get).collect(Collectors.toList());
                    return Optional.of(
                        new FunctionCallOperand(fcc.getText(), functionName, functionParameters)
                    );
                }
            } else {
                // No parameter function
                return Optional.of(
                    new FunctionCallOperand(fcc.getText(), functionName, functionParameters)
                );
            }
        } else if (numberContext.LPAREN() != null) {
            return parseNumericOperand(numberContext.number(0), true);
        } else if (numberContext.atomicNumber() != null) {
            return Optional.of(new AtomicNumberOperand(numberContext.getText()));
        } else if (numberContext.NULL() != null) {
            return Optional.of(new NullNumericOperand(numberContext.getText()));
        }

        return Optional.empty();
    }

    private Optional<Condition> parseStringBasedCondition(
        DataQualityDefinitionLanguageParser.StringBasedConditionContext ctx
    ) {
        String exprStr = ctx.getText();
        Condition condition = null;

        if (ctx.EQUAL_TO() != null && ctx.stringValues() != null) {
            StringBasedConditionOperator op = (ctx.NEGATION() != null) ?
                StringBasedConditionOperator.NOT_EQUALS
                : StringBasedConditionOperator.EQUALS;
            Optional<StringOperand> operand = parseStringOperand(ctx, Optional.of(ctx.stringValues()), op);
            if (operand.isPresent()) {
                condition = new StringBasedCondition(exprStr, op, Collections.singletonList(operand.get()));
            }
        } else if (ctx.IN() != null &&
            ctx.stringValuesArray() != null &&
            ctx.stringValuesArray().stringValues().size() > 0) {
            StringBasedConditionOperator op = (ctx.NOT() != null) ?
                StringBasedConditionOperator.NOT_IN
                : StringBasedConditionOperator.IN;
            List<Optional<StringOperand>> operands = ctx.stringValuesArray().stringValues()
                .stream()
                .map(s -> parseStringOperand(ctx, Optional.of(s), op))
                .collect(Collectors.toList());

            condition = new StringBasedCondition(exprStr, op,
                operands.stream().map(Optional::get).collect(Collectors.toList())
            );
        } else if (ctx.matchesRegexCondition() != null) {
            StringBasedConditionOperator op = (ctx.NOT() != null) ?
                StringBasedConditionOperator.NOT_MATCHES
                : StringBasedConditionOperator.MATCHES;
            Optional<StringOperand> operand = parseStringOperand(ctx, Optional.ofNullable(ctx.stringValues()), op);
            if (operand.isPresent()) {
                condition = new StringBasedCondition(exprStr, op, Collections.singletonList(operand.get()));
            }
        }

        return Optional.ofNullable(condition);
    }

    private Optional<StringOperand> parseStringOperand(
        DataQualityDefinitionLanguageParser.StringBasedConditionContext ctx,
        Optional<DataQualityDefinitionLanguageParser.StringValuesContext>
        stringValuesContext, StringBasedConditionOperator op) {

        switch (op) {
            case NOT_EQUALS:
            case EQUALS:
                Keyword keyword = parseKeyword(stringValuesContext.get());
                if (keyword == null) {
                    return Optional.of(new QuotedStringOperand(
                        removeQuotes(stringValuesContext.get().quotedString().getText())));
                } else {
                    return Optional.of(new KeywordStringOperand(keyword));
                }
            case NOT_IN:
            case IN:
                keyword = parseKeyword(stringValuesContext.get());
                if (keyword == null) {
                    return Optional.of(new QuotedStringOperand(
                        removeQuotes(removeEscapes(stringValuesContext.get().quotedString().getText()))));
                } else {
                    return Optional.of(new KeywordStringOperand(keyword));
                }
            case MATCHES:
            case NOT_MATCHES:
                return Optional.of(new QuotedStringOperand(
                        removeQuotes(ctx.matchesRegexCondition().quotedString().getText())));
            default:
                return Optional.empty();
        }
    }

    private Optional<Condition> parseDateBasedCondition(
        DataQualityDefinitionLanguageParser.DateBasedConditionContext ctx) {

        String exprStr = ctx.getText();
        Condition condition = null;

        if (ctx.BETWEEN() != null && ctx.dateExpression().size() == 2) {
            Optional<DateExpression> lower = parseDateExpression(ctx.dateExpression(0));
            Optional<DateExpression> upper = parseDateExpression(ctx.dateExpression(1));
            if (lower.isPresent() && upper.isPresent()) {
                DateBasedConditionOperator op = (ctx.NOT() != null) ?
                    DateBasedConditionOperator.NOT_BETWEEN
                    : DateBasedConditionOperator.BETWEEN;
                condition = new DateBasedCondition(
                    exprStr, op, Arrays.asList(lower.get(), upper.get())
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
                DateBasedConditionOperator op = (ctx.NEGATION() != null) ?
                    DateBasedConditionOperator.NOT_EQUALS
                    : DateBasedConditionOperator.EQUALS;
                condition = new DateBasedCondition(
                    exprStr, op, Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.IN() != null &&
            ctx.dateExpressionArray() != null &&
            ctx.dateExpressionArray().dateExpression().size() > 0) {
            List<Optional<DateExpression>> expressions = ctx.dateExpressionArray().dateExpression().stream()
                .map(this::parseDateExpression)
                .collect(Collectors.toList());

            if (expressions.stream().allMatch(Optional::isPresent)) {
                DateBasedConditionOperator op = (ctx.NOT() != null) ?
                    DateBasedConditionOperator.NOT_IN
                    : DateBasedConditionOperator.IN;
                condition = new DateBasedCondition(
                    exprStr, op,
                    expressions.stream().map(Optional::get).collect(Collectors.toList())
                );
            }
        }

        return Optional.ofNullable(condition);
    }

    private Optional<Condition> parseDurationBasedCondition(
        DataQualityDefinitionLanguageParser.DurationBasedConditionContext ctx
    ) {

        String exprStr = ctx.getText();
        Condition condition = null;

        if (ctx.BETWEEN() != null && ctx.durationExpression().size() == 2) {
            Optional<Duration> lower = parseDuration(ctx.durationExpression(0));
            Optional<Duration> upper = parseDuration(ctx.durationExpression(1));
            if (lower.isPresent() && upper.isPresent()) {
                DurationBasedConditionOperator op = (ctx.NOT() != null) ?
                    DurationBasedConditionOperator.NOT_BETWEEN
                    : DurationBasedConditionOperator.BETWEEN;
                condition = new DurationBasedCondition(
                    exprStr, op, Arrays.asList(lower.get(), upper.get())
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
                DurationBasedConditionOperator op = (ctx.NEGATION() != null) ?
                    DurationBasedConditionOperator.NOT_EQUALS
                    : DurationBasedConditionOperator.EQUALS;
                condition = new DurationBasedCondition(
                    exprStr, op,
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
                DurationBasedConditionOperator op = (ctx.NOT() != null) ?
                    DurationBasedConditionOperator.NOT_IN
                    : DurationBasedConditionOperator.IN;
                condition = new DurationBasedCondition(
                    exprStr, op,
                    durations.stream().map(Optional::get).collect(Collectors.toList())
                );
            }
        }

        return Optional.ofNullable(condition);
    }

    private Optional<DateExpression> parseDateExpression(
        DataQualityDefinitionLanguageParser.DateExpressionContext ctx) {
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
        } else if (ctx.NULL() != null) {
            return Optional.of(new NullDateExpression());
        } else {
            return Optional.of(new DateExpression.StaticDate(removeQuotes(ctx.DATE().getText())));
        }
    }

    private Optional<Duration> parseDuration(
        DataQualityDefinitionLanguageParser.DurationExpressionContext ctx) {
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

    private List<DQRuleParameterValue> parseParameters(
        List<DataQualityDefinitionLanguageParser.ParameterWithConnectorWordContext> parameters) {
        if (parameters == null) return new ArrayList<>();
        return parameters.stream().map(this::parseParameter).collect(Collectors.toList());
    }

    private DQRuleParameterValue parseParameter(
        DataQualityDefinitionLanguageParser.ParameterWithConnectorWordContext pc) {
        String connectorWord = pc.connectorWord() == null ? "" : pc.connectorWord().getText();

        if (pc.parameter().QUOTED_STRING() != null) {
            return new DQRuleParameterValue(
                removeQuotes(pc.parameter().QUOTED_STRING().getText()), true, connectorWord);
        } else if (pc.parameter().IDENTIFIER() != null) {
            return new DQRuleParameterValue(
                pc.parameter().IDENTIFIER().getText(), false, connectorWord);
        } else {
            return new DQRuleParameterValue(pc.parameter().getText(), true, connectorWord);
        }
    }

    private List<String> validateDictionary(DataQualityDefinitionLanguageParser.DictionaryContext dc) {
        List<String> dictionaryErrors = new ArrayList<>();
        if (dc.pair() == null || (dc.pair().size() == 1 && dc.pair().get(0).getText().isEmpty())) {
            dictionaryErrors.add("Empty dictionary provided");
        }
        return dictionaryErrors;
    }

    private Keyword parseKeyword(
        DataQualityDefinitionLanguageParser.StringValuesContext stringValuesContext) {
        Keyword keyword = null;
        try {
            String operand = stringValuesContext.getText().toUpperCase();
            if (isValidEnumValue(operand)) {
                Method method = stringValuesContext.getClass().getMethod(operand);
                Object result = method.invoke(stringValuesContext);
                if (result != null) {
                    keyword = Keyword.valueOf(operand);
                }
            }
        } catch (IllegalArgumentException | IllegalAccessException | NoSuchMethodException |
                 InvocationTargetException e) {
            errorMessages.add(e.getMessage());
        }
        return keyword;
    }

    private boolean isValidEnumValue(String value) {
        try {
            Enum.valueOf(Keyword.class, value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

}
