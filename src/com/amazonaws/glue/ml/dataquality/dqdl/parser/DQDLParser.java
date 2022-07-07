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
import com.amazonaws.glue.ml.dataquality.dqdl.exception.DataQualityRulesetNotValidException;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@NoArgsConstructor
public class DQDLParser {
    public DQRuleset parse(String dqdl) throws DataQualityRulesetNotValidException {
        CharStream input = CharStreams.fromString(dqdl);
        TokenStream tokens = new CommonTokenStream(new DataQualityDefinitionLanguageLexer(input));
        DataQualityDefinitionLanguageParser parser = new DataQualityDefinitionLanguageParser(tokens);
        List<DQRule> dqRules = parser.rules().dqRules().topLevelRule().stream().map(topLevelRuleContext -> {
            if (topLevelRuleContext.AND().size() > 0) {
                List<DQRule> nestedRules =
                    topLevelRuleContext.dqRule().stream().map(this::getDQRule).collect(Collectors.toList());
                return new DQRule(
                    "Composite", null, "", DQRuleLogicalOperator.AND, nestedRules
                );
            } else if (topLevelRuleContext.OR().size() > 0) {
                List<DQRule> nestedRules =
                    topLevelRuleContext.dqRule().stream().map(this::getDQRule).collect(Collectors.toList());
                return new DQRule(
                    "Composite", null, "", DQRuleLogicalOperator.OR, nestedRules
                );
            } else if (topLevelRuleContext.dqRule(0) != null) {
                return getDQRule(topLevelRuleContext.dqRule(0));
            }

            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());

        return new DQRuleset(dqRules);
    }

    private DQRule getDQRule(DataQualityDefinitionLanguageParser.DqRuleContext dqRuleContext) {
        String ruleType = dqRuleContext.ruleType().getText();
        if (!DQRuleType.getRuleTypeMap().containsKey(ruleType)) {
            System.out.printf("Rule Type: %s is not valid%n", ruleType);
            return null;
        }

        DQRuleType dqRuleType = DQRuleType.getRuleTypeMap().get(ruleType);
        List<String> parameters =
            dqRuleContext.parameter().stream().map(ParseTree::getText).collect(Collectors.toList());

        if (dqRuleType.getParameters().size() != parameters.size()) {
            System.out.printf("Unexpected number of parameters for Rule Type: %s", ruleType);
            return null;
        }

        Map<String, String> parameterMap = new HashMap<>();
        for (int i = 0; i < parameters.size(); i++) {
            parameterMap.put(dqRuleType.getParameters().get(i).getName(), parameters.get(0));
        }

        String condition = "";
        if (!dqRuleType.getReturnType().equals("BOOLEAN")) {
            if (dqRuleContext.condition() == null) {
                System.out.printf("No condition provided for rule with non boolean rule type: %s", ruleType);
                return null;
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
}
