/*
 * DQAnalyzerTest.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import com.amazonaws.glue.ml.dataquality.dqdl.exception.InvalidDataQualityRulesetException;
import com.amazonaws.glue.ml.dataquality.dqdl.parser.DQDLParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class DQAnalyzerTest {
    DQDLParser parser = new DQDLParser();

    @Test
    void test_singleAnalyzer() {
        String column = "colA";
        String ruleset = String.format("Rules = [ IsComplete \"%s\" ] Analyzers = [ Completeness \"%s\" ]", column, column);

        try {
            DQRuleset dqRuleset = parser.parse(ruleset);
            DQAnalyzer dqAnalyzer = dqRuleset.getAnalyzers().get(0);
            assertEquals("Completeness", dqAnalyzer.getRuleType());
            assertEquals(1, dqAnalyzer.getParameters().size());
            assertTrue(dqAnalyzer.getParameters().containsValue(column));
        } catch (InvalidDataQualityRulesetException e) {
            fail(e.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("provideRawAnalyzers")
    void test_analyzerParsingAndGeneratingWithParser(String analyzer) {
        try {
            DQRuleset dqRuleset = parser.parse(String.format("Rules = [ IsComplete \"colA\" ] Analyzers = [ %s ]", analyzer));
            assertEquals(1, dqRuleset.getRules().size());
            assertEquals(1, dqRuleset.getAnalyzers().size());

            DQAnalyzer dqAnalyzer = dqRuleset.getAnalyzers().get(0);
            String dqAnalyzerAsString = dqAnalyzer.toString();
            assertEquals(analyzer, dqAnalyzerAsString);
        } catch (InvalidDataQualityRulesetException e) {
            fail(e.getMessage());
        }
    }

    private static Stream<Arguments> provideRawAnalyzers() {
        return Stream.of(
            Arguments.of("RowCount"),
            Arguments.of("RowCountMatch \"reference\""),
            Arguments.of("Completeness \"col_1\""),
            Arguments.of("ColumnCount"),
            Arguments.of("ColumnCorrelation \"col_1\" \"col_2\""),
            Arguments.of("Uniqueness \"col_1\""),
            Arguments.of("Uniqueness \"col_1\" \"col_2\""),
            Arguments.of("Sum \"col_A-B.C\""),
            Arguments.of("Mean \"col_A-B.CD\""),
            Arguments.of("StandardDeviation \"col_A-B.CD\""),
            Arguments.of("Entropy \"col_A-B.CD\""),
            Arguments.of("DistinctValuesCount \"col_A-B.CD\""),
            Arguments.of("UniqueValueRatio \"col_A-B.CD\""),
            Arguments.of("ReferentialIntegrity \"col-A\" \"reference.col-A1\""),
            Arguments.of("ReferentialIntegrity \"col-A,col-B\" \"reference.{col-A1,col-A2}\""),
            Arguments.of("DatasetMatch \"reference\" \"ID1,ID2\""),
            Arguments.of("DatasetMatch \"reference\" \"ID1,ID2\" \"colA,colB,colC\""),
            Arguments.of("DatasetMatch \"reference\" \"ID1->ID11,ID2->ID22\" \"colA->colAA\""),
            Arguments.of("SchemaMatch \"ref-1\""),
            Arguments.of("AggregateMatch \"sum(col-A)\" \"sum(colB)\""),
            Arguments.of("AggregateMatch \"sum(col-A)\" \"sum(reference.colA)\""),
            Arguments.of("AggregateMatch \"avg(col-A)\" \"avg(reference.colA)\""),
            Arguments.of("AggregateMatch \"SUM(col-A)\" \"SUM(reference.colA)\""),
            Arguments.of("CustomSql \"select count(*) from primary\""),
            Arguments.of("AllStatistics \"id\"")
        );
    }
}
