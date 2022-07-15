/*
 * DQDLParser.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.parser;

import com.amazonaws.glue.ml.dataquality.dqdl.DataQualityDefinitionLanguageLexer;
import com.amazonaws.glue.ml.dataquality.dqdl.DataQualityDefinitionLanguageParser;
import com.amazonaws.glue.ml.dataquality.dqdl.exception.InvalidDataQualityRulesetException;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleLogicalOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRule;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleType;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleset;
import lombok.NoArgsConstructor;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
public class DQDLParser {
    private static final String PARSING_ERROR_MESSAGE_PREFIX = "Parsing Error";

    public DQRuleset parse(String dqdl) throws InvalidDataQualityRulesetException {
        CharStream input = CharStreams.fromString(dqdl);
        DQDLErrorListener errorListener = new DQDLErrorListener();

        DataQualityDefinitionLanguageLexer lexer = new DataQualityDefinitionLanguageLexer(input);
        lexer.addErrorListener(errorListener);
        TokenStream tokens = new CommonTokenStream(lexer);

        DataQualityDefinitionLanguageParser parser = new DataQualityDefinitionLanguageParser(tokens);
        parser.addErrorListener(errorListener);

        List<DQRule> dqRules = new ArrayList<>();
        DataQualityDefinitionLanguageParser.DqRulesContext rulesContext = parser.rules().dqRules();
        if (rulesContext == null) {
            throw new InvalidDataQualityRulesetException(generateExceptionMessage(errorListener));
        }

        for (DataQualityDefinitionLanguageParser.TopLevelRuleContext tlc : rulesContext.topLevelRule()) {
            if (tlc.AND().size() > 0) {
                List<DQRule> nestedRules = new ArrayList<>();
                for (DataQualityDefinitionLanguageParser.DqRuleContext rc : tlc.dqRule()) {
                    DQRule dqRule = getDQRule(rc);
                    nestedRules.add(dqRule);
                }
                dqRules.add(new DQRule("Composite", null, "", DQRuleLogicalOperator.AND, nestedRules));
            } else if (tlc.OR().size() > 0) {
                List<DQRule> nestedRules = new ArrayList<>();
                for (DataQualityDefinitionLanguageParser.DqRuleContext rc : tlc.dqRule()) {
                    DQRule dqRule = getDQRule(rc);
                    nestedRules.add(dqRule);
                }
                dqRules.add(new DQRule("Composite", null, "", DQRuleLogicalOperator.OR, nestedRules));
            } else if (tlc.dqRule(0) != null) {
                dqRules.add(getDQRule(tlc.dqRule(0)));
            } else {
                throw new InvalidDataQualityRulesetException(generateExceptionMessage(errorListener));
            }
        }

        if (!errorListener.getErrorMessages().isEmpty()) {
            throw new InvalidDataQualityRulesetException(generateExceptionMessage(errorListener));
        }

        return new DQRuleset(dqRules);
    }

    private DQRule getDQRule(DataQualityDefinitionLanguageParser.DqRuleContext dqRuleContext)
        throws InvalidDataQualityRulesetException {
        String ruleType = dqRuleContext.ruleType().getText();
        if (!DQRuleType.getRuleTypeMap().containsKey(ruleType)) {
            throw new InvalidDataQualityRulesetException(
                String.format("Rule Type: %s is not valid", ruleType));
        }

        DQRuleType dqRuleType = DQRuleType.getRuleTypeMap().get(ruleType);
        List<String> parameters =
            dqRuleContext.parameter().stream().map(ParseTree::getText).collect(Collectors.toList());

        if (dqRuleType.getParameters().size() != parameters.size()) {
            throw new InvalidDataQualityRulesetException(
                String.format("Unexpected number of parameters for Rule Type: %s", ruleType));
        }

        Map<String, String> parameterMap = new HashMap<>();
        for (int i = 0; i < parameters.size(); i++) {
            parameterMap.put(dqRuleType.getParameters().get(i).getName(), parameters.get(i));
        }

        String condition = "";
        if (!dqRuleType.getReturnType().equals("BOOLEAN")) {
            if (dqRuleContext.condition() == null) {
                throw new InvalidDataQualityRulesetException(
                    String.format("No condition provided for rule with non boolean rule type: %s", ruleType));
            }

            condition = dqRuleContext.condition().getText();
        }

        return new DQRule(
            dqRuleType.getRuleTypeName(),
            parameterMap,
            condition,
            DQRuleLogicalOperator.AND,
            Collections.emptyList()
        );
    }

    private String generateExceptionMessage(DQDLErrorListener errorListener) {
        String message = PARSING_ERROR_MESSAGE_PREFIX;
        if (!errorListener.getErrorMessages().isEmpty()) {
            String delimiter = ", ";
            message += ": " + String.join(delimiter, errorListener.getErrorMessages());
        }

        return message;
    }
}
