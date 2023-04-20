/*
 * DQDLParser.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.parser.updated;

import com.amazonaws.glue.ml.dataquality.dqdl.exception.InvalidDataQualityRulesetException;
import com.amazonaws.glue.ml.dataquality.dqdl.model.updated.DQRuleset;
import com.amazonaws.glue.ml.dataquality.dqdl.parser.DQDLErrorListener;
import com.amazonaws.glue.ml.dataquality.dqdl.updated.DataQualityDefinitionLanguageUpdatedLexer;
import com.amazonaws.glue.ml.dataquality.dqdl.updated.DataQualityDefinitionLanguageUpdatedParser;
import com.amazonaws.glue.ml.dataquality.dqdl.util.Either;


import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.List;

public class DQDLParser {
    private static final String PARSING_ERROR_MESSAGE_PREFIX = "Parsing Error";

    public DQRuleset parse(String dqdl) throws InvalidDataQualityRulesetException {
        CharStream input = CharStreams.fromString(dqdl);
        DQDLErrorListener errorListener = new DQDLErrorListener();

        DataQualityDefinitionLanguageUpdatedLexer lexer = new DataQualityDefinitionLanguageUpdatedLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        TokenStream tokens = new CommonTokenStream(lexer);

        DataQualityDefinitionLanguageUpdatedParser parser = new DataQualityDefinitionLanguageUpdatedParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        DQDLParserListener listener = new DQDLParserListener(errorListener);
        ParseTreeWalker.DEFAULT.walk(listener, parser.document());

        Either<List<String>, DQRuleset> dqRulesetEither = listener.getParsedRuleset();

        if (dqRulesetEither.isLeft()) {
            throw new InvalidDataQualityRulesetException(generateExceptionMessage(dqRulesetEither.getLeft()));
        } else {
            return dqRulesetEither.getRight();
        }
    }

    private String generateExceptionMessage(List<String> errorMessages) {
        String message = PARSING_ERROR_MESSAGE_PREFIX;
        if (!errorMessages.isEmpty()) {
            String delimiter = ", ";
            message += ": " + String.join(delimiter, errorMessages);
        }

        return message;
    }
}
