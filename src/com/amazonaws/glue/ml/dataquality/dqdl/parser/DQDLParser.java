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
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRule;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleset;
import lombok.NoArgsConstructor;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@NoArgsConstructor
public class DQDLParser {
    public DQRuleset parse(String dqdl) {
        CharStream input = CharStreams.fromString(dqdl);
        TokenStream tokens = new CommonTokenStream(new DataQualityDefinitionLanguageLexer(input));
        DataQualityDefinitionLanguageParser parser = new DataQualityDefinitionLanguageParser(tokens);
        List<DQRule> dqRules = parser.rules().dqRules().dqRule().stream().map(dqRuleContext -> {
            if (dqRuleContext.COLUMN_NAME() != null) {
                return new DQRule(
                    dqRuleContext.columnRuleType().getText(),
                    Optional.of(dqRuleContext.COLUMN_NAME().getText()),
                    Optional.empty()
                );
            } else if (dqRuleContext.INT() != null) {
                return new DQRule(
                    dqRuleContext.datasetRuleType().getText(),
                    Optional.empty(),
                    Optional.of(Integer.parseInt(dqRuleContext.INT().getText()))
                );
            }
            return new DQRule(dqRuleContext.getText(), Optional.empty(), Optional.empty());
        }).collect(Collectors.toList());
        return new DQRuleset(dqRules);
    }
}
