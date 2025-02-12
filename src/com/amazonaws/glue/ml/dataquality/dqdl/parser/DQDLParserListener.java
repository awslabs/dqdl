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

import com.amazonaws.glue.ml.dataquality.dqdl.DataQualityDefinitionLanguageBaseListener;
import com.amazonaws.glue.ml.dataquality.dqdl.DataQualityDefinitionLanguageParser;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQAnalyzer;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRule;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleLogicalOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleType;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleset;
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
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.size.Size;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.size.SizeBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.size.SizeBasedConditionOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.size.SizeUnit;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.Keyword;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.KeywordStringOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.QuotedStringOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringBasedConditionOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.Tag;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.variable.VariableReferenceOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.parameter.DQRuleParameterConstantValue;
import com.amazonaws.glue.ml.dataquality.dqdl.model.parameter.DQRuleParameterValue;
import com.amazonaws.glue.ml.dataquality.dqdl.model.parameter.DQRuleParameterVariableValue;
import com.amazonaws.glue.ml.dataquality.dqdl.model.variable.DQVariable;
import com.amazonaws.glue.ml.dataquality.dqdl.model.variable.VariableResolutionResult;
import com.amazonaws.glue.ml.dataquality.dqdl.util.Either;
import org.antlr.v4.runtime.ParserRuleContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.zone.ZoneRulesException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.Tag.convertToStringMap;
import static com.amazonaws.glue.ml.dataquality.dqdl.util.StringUtils.removeEscapes;

public class DQDLParserListener extends DataQualityDefinitionLanguageBaseListener {
    private final DQDLErrorListener errorListener;
    private final List<String> errorMessages = new ArrayList<>();
    private final Map<String, String> metadata = new HashMap<>();

    private String primarySource;
    private List<String> additionalSources;
    private final List<DQRule> dqRules = new ArrayList<>();
    private final List<DQAnalyzer> dqAnalyzers = new ArrayList<>();
    private final Map<String, DQVariable> dqVariables = new HashMap<>();

    private static final String METADATA_VERSION_KEY = "Version";
    private static final Set<String> ALLOWED_METADATA_KEYS;

    private static final String PRIMARY_SOURCE_KEY = "Primary";
    private static final String ADDITIONAL_SOURCES_KEY = "AdditionalDataSources";
    private static final Set<String> ALLOWED_SOURCES_KEYS;
    private static final String THRESHOLD_KEY = "threshold";

    private static final String MILITARY_TIME_FORMAT = "HH:mm";
    private static final String AMPM_TIME_FORMAT = "h:mm a";

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

        for (DataQualityDefinitionLanguageParser.TopLevelRuleContext tlc: dqRulesContext.topLevelRule()) {
            Either<String, DQRule> dqRuleEither = parseTopLevelRule(tlc);
            if (dqRuleEither.isLeft()) {
                errorMessages.add(dqRuleEither.getLeft());
                return;
            } else {
                dqRules.add(dqRuleEither.getRight());
            }
        }
    }

    private Either<String, DQRule> parseTopLevelRule(DataQualityDefinitionLanguageParser.TopLevelRuleContext tlc) {
        if (tlc.LPAREN() != null && tlc.RPAREN() != null) {
            return parseTopLevelRule(tlc.topLevelRule(0));
        } else if (tlc.AND() != null || tlc.OR() != null) {
            DQRuleLogicalOperator op = tlc.AND() != null ? DQRuleLogicalOperator.AND : DQRuleLogicalOperator.OR;
            List<Either<String, DQRule>> nestedRuleEitherList =
                tlc.topLevelRule().stream().map(this::parseTopLevelRule).collect(Collectors.toList());

            List<String> allErrorMessages = new ArrayList<>();
            List<DQRule> allRules = new ArrayList<>();

            nestedRuleEitherList.forEach(arg -> {
                if (arg.isLeft()) {
                    allErrorMessages.add(arg.getLeft());
                } else {
                    allRules.add(arg.getRight());
                }
            });

            if (allErrorMessages.isEmpty()) {
                return Either.fromRight(
                    new DQRule("Composite", null, null, null, op, allRules)
                );
            } else {
                return Either.fromLeft(allErrorMessages.get(0));
            }
        } else if (tlc.dqRule() != null) {
            return getDQRule(tlc.dqRule());
        } else {
            return Either.fromLeft("No valid rule found");
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

    @Override
    public void enterVariableDeclaration(DataQualityDefinitionLanguageParser.VariableDeclarationContext ctx) {
        if (!errorMessages.isEmpty()) {
            return;
        }

        String variableName = ctx.IDENTIFIER().getText();

        if (variableName.startsWith(".") || variableName.startsWith("_")) {
            errorMessages.add(String.format("Variable name '%s' cannot start with '.' or '_'", variableName));
            return;
        }

        if (dqVariables.containsKey(variableName)) {
            errorMessages.add("Variable '" + variableName + "' is already defined");
            return;
        }

        DQVariable variable = null;
        DataQualityDefinitionLanguageParser.ExpressionContext expr = ctx.expression();
        if (expr == null) {
            errorMessages.add(String.format("Missing value for variable '%s'", variableName));
            return;
        }

        if (expr.stringValuesArray() != null) {
            List<String> values = expr.stringValuesArray().stringValues().stream()
                    .map(this::processStringValues)
                    .collect(Collectors.toList());
            variable = new DQVariable(variableName, DQVariable.VariableType.STRING_ARRAY, values);
        } else if (expr.stringValues() != null) {
            String value = processStringValues(expr.stringValues());
            variable = new DQVariable(variableName, DQVariable.VariableType.STRING, value);
        }

        if (variable != null) {
            // Check for nested variables
            if (containsVariableReference(variable.getValue().toString())) {
                errorMessages.add(String.format("Nested variables are not supported in variable '%s'", variableName));
                return;
            }
            dqVariables.put(variableName, variable);
        } else {
            errorMessages.add(String.format("Failed to parse variable '%s'", variableName));
        }
    }

    private boolean containsVariableReference(String value) {
        return Pattern.compile("\\$[a-zA-Z_][a-zA-Z0-9_.]*").matcher(value).find();
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
            dqRuleType.createParameterMap(dqRuleType.getParameters(), parameters, ruleType);

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

        Condition thresholdCondition = null;
        Map<String, Tag> tags = new HashMap<>();
        List<DataQualityDefinitionLanguageParser.TagWithConditionContext> tagContexts =
                (dqRuleContext.tagWithCondition() == null) ? new ArrayList<>() : dqRuleContext.tagWithCondition();
        for (DataQualityDefinitionLanguageParser.TagWithConditionContext tagContext : tagContexts) {
            if (tagContext.stringBasedCondition() != null) {
                //process plain string tag
                final Either<String, Tag> outcome = processStringTag(tagContext);
                if (outcome.isLeft()) {
                    return Either.fromLeft(outcome.getLeft());
                } else {
                    final Tag tag = outcome.getRight();
                    tags.put(tag.getKey(), tag);
                }
            } else if (tagContext.numberBasedCondition() != null) {
                final String tagName = tagContext.tagValues().getText();
                if (tagName.equalsIgnoreCase(THRESHOLD_KEY)) {
                    //process threshold tag
                    final Either<String, Condition> outcome =
                            processThresholdTag(dqRuleType, thresholdCondition, tagContext, ruleType);
                    if (outcome.isLeft()) {
                        return Either.fromLeft(outcome.getLeft());
                    } else {
                        thresholdCondition = outcome.getRight();
                    }
                } else {
                    //convert number tag into string tag
                    final Either<String, Tag> outcome = processNumberTag(tagContext, tagName);
                    if (outcome.isLeft()) {
                        return Either.fromLeft(outcome.getLeft());
                    } else {
                        final Tag tag = outcome.getRight();
                        tags.put(tag.getKey(), tag);
                    }
                }
            } else {
                return Either.fromLeft(String.format("Invalid tag provided for rule type: %s", ruleType));
            }
        }

        Condition condition;

        List<Either<String, Condition>> conditions = Arrays.stream(dqRuleType.getReturnType().split("\\|"))
                .map(rt -> parseCondition(dqRuleType, rt, dqRuleContext, convertToStringMap(tags)))
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

        Either<String, VariableResolutionResult> resolutionResultEither =
                resolveVariables(parameterMap, condition, thresholdCondition);

        if (resolutionResultEither.isLeft()) {
            return Either.fromLeft(resolutionResultEither.getLeft());
        }

        VariableResolutionResult resolutionResult = resolutionResultEither.getRight();

        return Either.fromRight(
                DQRule.createFromParameterValueMap(
                        dqRuleType,
                        resolutionResult.getResolvedParameters(),
                        resolutionResult.getResolvedCondition(),
                        resolutionResult.getResolvedThresholdCondition(),
                        whereClause,
                        tags
                )
        );
    }

    private Either<String, Condition> processThresholdTag(DQRuleType dqRuleType,
                                                          Condition thresholdCondition,
                                                          DataQualityDefinitionLanguageParser
                                                                  .TagWithConditionContext tagContext,
                                                          String ruleType) {
        if (dqRuleType.isThresholdSupported()) {
            if (thresholdCondition != null) {
                return Either.fromLeft("Only one threshold condition at a time is supported.");
            }
            return processThresholdTag(tagContext, ruleType);
        } else {
            return Either.fromLeft(String.format("Threshold condition not supported for rule type: %s", ruleType));
        }
    }

    private Either<String, Tag> processNumberTag(DataQualityDefinitionLanguageParser
                                                         .TagWithConditionContext tagContext,
                                                 String tagName) {
        if (!isTagValid(tagContext.numberBasedCondition())) {
            return Either.fromLeft("Number tags only support the equality operator.");
        }
        final List<DataQualityDefinitionLanguageParser.NumberContext> numberContexts =
                tagContext.numberBasedCondition().number();
        if (numberContexts != null && !numberContexts.isEmpty()) {
            final String tagValue = numberContexts.get(0).getText();
            return Either.fromRight(new Tag(tagName, tagValue));
        } else {
            return Either.fromLeft(String.format("Error Parsing Tag %s", tagName));
        }
    }

    private Either<String, Tag> processStringTag(
            DataQualityDefinitionLanguageParser.TagWithConditionContext tagContext) {
        if (!isTagValid(tagContext.stringBasedCondition())) {
            return Either.fromLeft("String tags only support the equality operator.");
        }
        String tagKey = tagContext.tagValues().getText();
        Optional<Condition> valueCondition = parseStringBasedCondition(tagContext.stringBasedCondition());
        if (valueCondition.isPresent()) {
            StringBasedCondition stringCondition = (StringBasedCondition) valueCondition.get();
            String tagValue = stringCondition.getOperands().get(0).formatOperand();
            return Either.fromRight(new Tag(tagKey, tagValue));
        } else {
            return Either.fromLeft(String.format("Error while parsing tag: %s", tagKey));
        }
    }

    private Either<String, Condition> processThresholdTag(
            DataQualityDefinitionLanguageParser.TagWithConditionContext tagContext, String ruleType) {
        DataQualityDefinitionLanguageParser.NumberBasedConditionContext ctx =
                tagContext.numberBasedCondition();
        Optional<Condition> possibleCond = parseNumberBasedCondition(ctx);
        if (possibleCond.isPresent()) {
            return Either.fromRight(possibleCond.get());
        } else {
            return Either.fromLeft(String.format(
                    "Unable to parse threshold condition provided for rule type: %s", ruleType));
        }
    }

    private boolean isTagValid(ParserRuleContext ctx) {
        if (ctx instanceof DataQualityDefinitionLanguageParser.StringBasedConditionContext) {
            final DataQualityDefinitionLanguageParser.StringBasedConditionContext stringCtx =
                    (DataQualityDefinitionLanguageParser.StringBasedConditionContext) ctx;
            return stringCtx.EQUAL_TO() != null && stringCtx.NEGATION() == null;
        } else if (ctx instanceof DataQualityDefinitionLanguageParser.NumberBasedConditionContext) {
            final DataQualityDefinitionLanguageParser.NumberBasedConditionContext numberCtx =
                    (DataQualityDefinitionLanguageParser.NumberBasedConditionContext) ctx;
            return numberCtx.EQUAL_TO() != null && numberCtx.NEGATION() == null;
        } else {
            return false;
        }
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
            dqRuleType.createParameterMap(dqRuleType.getParameters(), parameters, analyzerType);

        return Either.fromRight(DQAnalyzer.createFromValueMap(analyzerType, parameterMap));
    }

    private Either<String, Condition> parseCondition(
        DQRuleType ruleType,
        String returnType,
        DataQualityDefinitionLanguageParser.DqRuleContext dqRuleContext,
        Map<String, String> tags) {

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
                        parseDateBasedCondition(dqRuleContext.condition().dateBasedCondition(), tags);

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
            case "SIZE":
            case "SIZE_ARRAY": {
                DataQualityDefinitionLanguageParser.ConditionContext cx = dqRuleContext.condition();
                if (cx == null || (cx.sizeBasedCondition() == null && cx.numberBasedCondition() == null)) {
                    return Either.fromLeft(
                            String.format("Unexpected condition for rule of type %s with size return type",
                                    ruleType.getRuleTypeName()));
                } else if (cx.sizeBasedCondition() != null) {
                    Optional<Condition> possibleCond =
                            parseSizeBasedCondition(dqRuleContext.condition().sizeBasedCondition());

                    if (possibleCond.isPresent()) {
                        response = Either.fromRight(possibleCond.get());
                    }
                } else if (cx.numberBasedCondition() != null) {
                    Optional<SizeBasedCondition> possibleCond =
                            convertNumberToSizeCondition(
                                    parseNumberBasedCondition(dqRuleContext.condition().numberBasedCondition()));

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

    private Optional<SizeBasedCondition> convertNumberToSizeCondition(Optional<Condition> in) {
        if (!in.isPresent() || !(in.get() instanceof NumberBasedCondition)) {
            return Optional.empty();
        }
        NumberBasedCondition input = (NumberBasedCondition) in.get();
        final String conditionAsString = input.getConditionAsString();
        final SizeBasedConditionOperator operator = SizeBasedConditionOperator.valueOf(input.getOperator().name());
        final List<Size> operands = input.getOperands().stream()
                .filter(x -> x instanceof AtomicNumberOperand)
                .filter(x -> Double.parseDouble(x.getOperand()) % 1 == 0) // filter only integer
                .map(x -> new Size(Integer.parseInt(x.getOperand()), SizeUnit.B))
                .collect(Collectors.toList());
        if (operands.size() != input.getOperands().size()) {
            return Optional.empty();
        }
        return Optional.of(new SizeBasedCondition(conditionAsString, operator, operands));
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

        if (ctx.EQUAL_TO() != null) {
            StringBasedConditionOperator op = (ctx.NEGATION() != null) ?
                    StringBasedConditionOperator.NOT_EQUALS
                    : StringBasedConditionOperator.EQUALS;

            StringOperand operand;
            if (ctx.variableDereference() != null) {
                operand = new VariableReferenceOperand(ctx.variableDereference().IDENTIFIER().getText());
            } else if (ctx.stringValues() != null) {
                Optional<StringOperand> parsedOperand = parseStringOperand(ctx, Optional.of(ctx.stringValues()), op);
                if (!parsedOperand.isPresent()) {
                    return Optional.empty();
                }
                operand = parsedOperand.get();
            } else {
                return Optional.empty();
            }

            condition = new StringBasedCondition(exprStr, op, Collections.singletonList(operand));
        } else if (ctx.IN() != null) {
            StringBasedConditionOperator op = (ctx.NOT() != null) ?
                    StringBasedConditionOperator.NOT_IN
                    : StringBasedConditionOperator.IN;

            List<StringOperand> operands;
            if (ctx.variableDereference() != null) {
                operands = Collections.singletonList(
                        new VariableReferenceOperand(ctx.variableDereference().IDENTIFIER().getText()));
            } else if (ctx.stringValuesArray() != null && ctx.stringValuesArray().stringValues().size() > 0) {
                operands = ctx.stringValuesArray().stringValues()
                        .stream()
                        .map(s -> parseStringOperand(ctx, Optional.of(s), op))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());
            } else {
                return Optional.empty();
            }

            if (!operands.isEmpty()) {
                condition = new StringBasedCondition(exprStr, op, operands);
            }
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
        DataQualityDefinitionLanguageParser.DateBasedConditionContext ctx, Map<String, String> tags) {

        String exprStr = ctx.getText();
        Condition condition = null;

        if (ctx.BETWEEN() != null && ctx.dateExpression().size() == 2) {
            Optional<DateExpression> lower = parseDateExpression(ctx.dateExpression(0), tags);
            Optional<DateExpression> upper = parseDateExpression(ctx.dateExpression(1), tags);
            if (lower.isPresent() && upper.isPresent()) {
                DateBasedConditionOperator op = (ctx.NOT() != null) ?
                    DateBasedConditionOperator.NOT_BETWEEN
                    : DateBasedConditionOperator.BETWEEN;
                condition = new DateBasedCondition(
                    exprStr, op, Arrays.asList(lower.get(), upper.get())
                );
            }
        } else if (ctx.GREATER_THAN_EQUAL_TO() != null && ctx.dateExpression().size() == 1) {
            Optional<DateExpression> operand = parseDateExpression(ctx.dateExpression(0), tags);
            if (operand.isPresent()) {
                condition = new DateBasedCondition(
                    exprStr, DateBasedConditionOperator.GREATER_THAN_EQUAL_TO, Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.GREATER_THAN() != null && ctx.dateExpression().size() == 1) {
            Optional<DateExpression> operand = parseDateExpression(ctx.dateExpression(0), tags);
            if (operand.isPresent()) {
                condition = new DateBasedCondition(
                    exprStr, DateBasedConditionOperator.GREATER_THAN, Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.LESS_THAN() != null && ctx.dateExpression().size() == 1) {
            Optional<DateExpression> operand = parseDateExpression(ctx.dateExpression(0), tags);
            if (operand.isPresent()) {
                condition = new DateBasedCondition(
                    exprStr, DateBasedConditionOperator.LESS_THAN, Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.LESS_THAN_EQUAL_TO() != null && ctx.dateExpression().size() == 1) {
            Optional<DateExpression> operand = parseDateExpression(ctx.dateExpression(0), tags);
            if (operand.isPresent()) {
                condition = new DateBasedCondition(
                    exprStr, DateBasedConditionOperator.LESS_THAN_EQUAL_TO, Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.EQUAL_TO() != null && ctx.dateExpression().size() == 1) {
            Optional<DateExpression> operand = parseDateExpression(ctx.dateExpression(0), tags);
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
                .map(x -> parseDateExpression(x, tags))
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

    private Optional<Condition> parseSizeBasedCondition(
            DataQualityDefinitionLanguageParser.SizeBasedConditionContext ctx
    ) {

        String exprStr = ctx.getText();
        Condition condition = null;

        if (ctx.BETWEEN() != null && ctx.sizeExpression().size() == 2) {
            Optional<Size> lower = parseSize(ctx.sizeExpression(0));
            Optional<Size> upper = parseSize(ctx.sizeExpression(1));
            if (lower.isPresent() && upper.isPresent()) {
                SizeBasedConditionOperator op = (ctx.NOT() != null) ?
                        SizeBasedConditionOperator.NOT_BETWEEN
                        : SizeBasedConditionOperator.BETWEEN;
                condition = new SizeBasedCondition(
                        exprStr, op, Arrays.asList(lower.get(), upper.get())
                );
            }
        } else if (ctx.GREATER_THAN_EQUAL_TO() != null && ctx.sizeExpression().size() == 1) {
            Optional<Size> operand = parseSize(ctx.sizeExpression(0));
            if (operand.isPresent()) {
                condition = new SizeBasedCondition(
                        exprStr, SizeBasedConditionOperator.GREATER_THAN_EQUAL_TO,
                        Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.GREATER_THAN() != null && ctx.sizeExpression().size() == 1) {
            Optional<Size> operand = parseSize(ctx.sizeExpression(0));
            if (operand.isPresent()) {
                condition = new SizeBasedCondition(
                        exprStr, SizeBasedConditionOperator.GREATER_THAN,
                        Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.LESS_THAN() != null && ctx.sizeExpression().size() == 1) {
            Optional<Size> operand = parseSize(ctx.sizeExpression(0));
            if (operand.isPresent()) {
                condition = new SizeBasedCondition(
                        exprStr, SizeBasedConditionOperator.LESS_THAN,
                        Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.LESS_THAN_EQUAL_TO() != null && ctx.sizeExpression().size() == 1) {
            Optional<Size> operand = parseSize(ctx.sizeExpression(0));
            if (operand.isPresent()) {
                condition = new SizeBasedCondition(
                        exprStr, SizeBasedConditionOperator.LESS_THAN_EQUAL_TO,
                        Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.EQUAL_TO() != null && ctx.sizeExpression().size() == 1) {
            Optional<Size> operand = parseSize(ctx.sizeExpression(0));
            if (operand.isPresent()) {
                SizeBasedConditionOperator op = (ctx.NEGATION() != null) ?
                        SizeBasedConditionOperator.NOT_EQUALS
                        : SizeBasedConditionOperator.EQUALS;
                condition = new SizeBasedCondition(
                        exprStr, op,
                        Collections.singletonList(operand.get())
                );
            }
        } else if (ctx.IN() != null &&
                ctx.sizeExpressionArray() != null &&
                ctx.sizeExpressionArray().sizeExpression().size() > 0) {

            List<Optional<Size>> sizes = ctx.sizeExpressionArray().sizeExpression().stream()
                    .map(this::parseSize)
                    .collect(Collectors.toList());

            if (sizes.stream().allMatch(Optional::isPresent)) {
                SizeBasedConditionOperator op = (ctx.NOT() != null) ?
                        SizeBasedConditionOperator.NOT_IN
                        : SizeBasedConditionOperator.IN;
                condition = new SizeBasedCondition(
                        exprStr, op,
                        sizes.stream().map(Optional::get).collect(Collectors.toList())
                );
            }
        }

        return Optional.ofNullable(condition);
    }

    private Optional<DateExpression> parseDateExpression(
        DataQualityDefinitionLanguageParser.DateExpressionContext ctx, Map<String, String> tags) {
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
        } else if (ctx.timeExpression() != null) {
            final String time = removeQuotes(ctx.timeExpression().MIL_TIME() != null
                    ? ctx.timeExpression().MIL_TIME().getText()
                    : ctx.timeExpression().TIME().getText());
            final String pattern = ctx.timeExpression().MIL_TIME() != null
                    ? MILITARY_TIME_FORMAT
                    : AMPM_TIME_FORMAT;
            final String timeZone = tags.getOrDefault("timeZone", "UTC");
            return parseTime(time, pattern, timeZone);
        } else {
            return Optional.of(new DateExpression.StaticDate(removeQuotes(ctx.DATE().getText())));
        }
    }

    private Optional<DateExpression> parseTime(final String in, final String pattern, final String timeZone) {
        try {
            final ZoneId zoneId = ZoneId.of(timeZone); // https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html
            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
            final LocalTime time = LocalTime.parse(in, formatter);
            final LocalDate today = LocalDate.now(zoneId);
            final ZonedDateTime localDateTime = ZonedDateTime.of(today, time, zoneId);
            final ZonedDateTime utcTime = localDateTime.withZoneSameInstant(ZoneOffset.UTC);
            return Optional.of(new DateExpression.StaticDateTime(utcTime.toLocalDateTime(), in));
        } catch (final DateTimeParseException e) {
            errorMessages.add(String.format("Error Parsing Date: %s. %s.", in, e.getMessage()));
            return Optional.empty();
        } catch (final ZoneRulesException e) {
            errorMessages.add(String.format("Error Parsing Time Zone: %s. %s.", timeZone, e.getMessage()));
            return Optional.empty();
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

    private Optional<Size> parseSize(
            DataQualityDefinitionLanguageParser.SizeExpressionContext ctx) {
        int amount = Integer.parseInt(ctx.INT() != null ? ctx.INT().getText() : ctx.DIGIT().getText());
        if (ctx.sizeUnit().exception != null) {
            return Optional.empty();
        } else {
            SizeUnit unit = SizeUnit.valueOf(ctx.sizeUnit().getText().toUpperCase());
            return Optional.of(new Size(amount, unit));
        }
    }

    private String removeQuotes(String quotedString) {
        if (quotedString.startsWith("\"") && quotedString.endsWith("\"")) {
            quotedString = quotedString.substring(1);
            quotedString = quotedString.substring(0, quotedString.length() - 1);
        }
        return quotedString;
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
            return new DQRuleParameterConstantValue(
                removeQuotes(pc.parameter().QUOTED_STRING().getText()), true, connectorWord);
        } else if (pc.parameter().IDENTIFIER() != null) {
            return new DQRuleParameterConstantValue(
                pc.parameter().IDENTIFIER().getText(), false, connectorWord);
        } else if (pc.parameter().variableDereference() != null &&
                pc.parameter().variableDereference().IDENTIFIER() != null) {
            String varName = pc.parameter().variableDereference().IDENTIFIER().getText();
            return new DQRuleParameterVariableValue(varName, connectorWord);
        } else {
            return new DQRuleParameterConstantValue(pc.parameter().getText(), true, connectorWord);
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

    private String processStringValues(DataQualityDefinitionLanguageParser.StringValuesContext sv) {
        if (sv.quotedString() != null) {
            return removeQuotes(sv.quotedString().getText());
        }
        return sv.getText();
    }

    private Either<String, VariableResolutionResult> resolveVariables(
            LinkedHashMap<String, DQRuleParameterValue> parameters,
            Condition condition,
            Condition thresholdCondition) {
        return DQDLVariableResolver.resolveVariables(parameters, condition, thresholdCondition, dqVariables);
    }

}
