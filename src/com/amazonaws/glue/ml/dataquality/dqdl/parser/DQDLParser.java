/*
 * DQDLParser.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.parser;

import com.amazonaws.glue.ml.dataquality.dqdl.exception.InvalidDataQualityRulesetException;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleset;
import com.amazonaws.glue.ml.dataquality.dqdl.DataQualityDefinitionLanguageLexer;
import com.amazonaws.glue.ml.dataquality.dqdl.DataQualityDefinitionLanguageParser;
import com.amazonaws.glue.ml.dataquality.dqdl.util.Either;

import lombok.extern.slf4j.Slf4j;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.List;

@Slf4j
public class DQDLParser {
    private static final String PARSING_ERROR_MESSAGE_PREFIX = "Parsing Error";

    public DQRuleset parse(String dqdl) throws InvalidDataQualityRulesetException {

        CharStream input = CharStreams.fromString(dqdl);
        DQDLErrorListener errorListener = new DQDLErrorListener();

        DataQualityDefinitionLanguageLexer lexer = new DataQualityDefinitionLanguageLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        TokenStream tokens = new CommonTokenStream(lexer);

        DataQualityDefinitionLanguageParser parser = new DataQualityDefinitionLanguageParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        DQDLParserListener listener = new DQDLParserListener(errorListener);
        try {
            ParseTreeWalker.DEFAULT.walk(listener, parser.document());
        } catch (StringIndexOutOfBoundsException e) {
            log.error(e.getMessage(), e);
            throw new InvalidDataQualityRulesetException("Invalid DQDL.");
        }
        Either<List<String>, DQRuleset> dqRulesetEither = listener.getParsedRuleset();
        if (dqRulesetEither.isLeft()) {
            throw new InvalidDataQualityRulesetException(generateExceptionMessage(dqRulesetEither.getLeft()));
        }
        return dqRulesetEither.getRight();

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
