/*
 * DQDLParserListener.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.parser;

import com.amazonaws.glue.ml.dataquality.dqdl.DataQualityDefinitionLanguageBaseListener;
import com.amazonaws.glue.ml.dataquality.dqdl.DataQualityDefinitionLanguageParser;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRule;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleLogicalOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleType;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleset;
import com.amazonaws.glue.ml.dataquality.dqdl.util.Either;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DQDLParserListener extends DataQualityDefinitionLanguageBaseListener {
    private final DQDLErrorListener errorListener;
    private final List<String> errorMessages = new ArrayList<>();
    private final Map<String, String> metadata = new HashMap<>();

    private String primarySource;
    private List<String> additionalSources;
    private final List<DQRule> dqRules = new ArrayList<>();

    private static final String METADATA_VERSION_KEY = "Version";
    private static final Set<String> ALLOWED_METADATA_KEYS;

    private static final String PRIMARY_SOURCE_KEY = "Primary";
    private static final String ADDITIONAL_SOURCES_KEY = "AdditionalSources";
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

    @Override
    public void enterMetadata(DataQualityDefinitionLanguageParser.MetadataContext ctx) {
        for (DataQualityDefinitionLanguageParser.PairContext pairContext : ctx.dictionary().pair()) {
            String key = pairContext.QUOTED_STRING().getText().replaceAll("\"", "");
            if (!ALLOWED_METADATA_KEYS.contains(key)) {
                errorMessages.add("Unsupported key provided in Metadata section");
                return;
            }

            String value = pairContext.pairValue().getText().replaceAll("\"", "");
            metadata.put(key, value);
        }
    }

    @Override
    public void enterSources(DataQualityDefinitionLanguageParser.SourcesContext ctx) {
        for (DataQualityDefinitionLanguageParser.PairContext pairContext : ctx.dictionary().pair()) {
            String key = pairContext.QUOTED_STRING().getText().replaceAll("\"", "");

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
    public void enterRules(DataQualityDefinitionLanguageParser.RulesContext ctx) {
        if (ctx.dqRules() == null) {
            errorMessages.add("No rules provided.");
        }
    }

    @Override
    public void enterDqRules(DataQualityDefinitionLanguageParser.DqRulesContext dqRulesContext) {
        if (!errorMessages.isEmpty()) {
            return;
        }

        for (DataQualityDefinitionLanguageParser.TopLevelRuleContext tlc : dqRulesContext.topLevelRule()) {
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

                dqRules.add(new DQRule("Composite", null, "", "", op, nestedRules));
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

    private Either<String, DQRule> getDQRule(DataQualityDefinitionLanguageParser.DqRuleContext dqRuleContext) {
        String ruleType = dqRuleContext.ruleType().getText();
        if (!DQRuleType.getRuleTypeMap().containsKey(ruleType)) {
            return Either.fromLeft(String.format("Rule Type: %s is not valid", ruleType));
        }

        DQRuleType dqRuleType = DQRuleType.getRuleTypeMap().get(ruleType);
        List<String> parameters = dqRuleContext.parameter().stream()
            .map(p -> p.getText().replaceAll("\"", ""))
            .collect(Collectors.toList());

        if (dqRuleType.getParameters().size() != parameters.size()) {
            return Either.fromLeft(String.format("Unexpected number of parameters for Rule Type: %s", ruleType));
        }

        Map<String, String> parameterMap = new HashMap<>();
        for (int i = 0; i < parameters.size(); i++) {
            parameterMap.put(dqRuleType.getParameters().get(i).getName(), parameters.get(i));
        }

        String condition = "";
        if (dqRuleType.getReturnType().equals("BOOLEAN")) {
          if (dqRuleContext.condition() != null) {
              return Either.fromLeft(
                  String.format("Unexpected condition for rule with boolean rule type: %s", ruleType));
          }
        } else {
            if (dqRuleContext.condition() == null) {
                return Either.fromLeft(
                    String.format("No condition provided for rule with non boolean rule type: %s", ruleType));
            }

            condition = dqRuleContext.condition().getText();
        }

        String hasThreshold = "";
        if (dqRuleContext.withThresholdCondition() != null &&
            dqRuleContext.withThresholdCondition().numericThresholdExpression() != null) {
            if (dqRuleType.getRuleTypeName().equals("ColumnValues")) {
                if (dqRuleContext.condition().matchesRegexExpression() != null ||
                    dqRuleContext.condition().jobStatusExpression() != null) {
                    hasThreshold = dqRuleContext.withThresholdCondition().numericThresholdExpression().getText();
                } else {
                    return Either.fromLeft("Threshold unsupported for the provided condition");
                }
            } else {
                return Either.fromLeft("Threshold is only applicable for ColumnValues rule");
            }
        }

        return Either.fromRight(
            new DQRule(dqRuleType.getRuleTypeName(), parameterMap, condition,
                hasThreshold, DQRuleLogicalOperator.AND, new ArrayList<>())
        );
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
}
