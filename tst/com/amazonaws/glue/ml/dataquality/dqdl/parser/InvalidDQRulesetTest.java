/*
 * InvalidDQRulesetTest.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.parser;

import com.amazonaws.glue.ml.dataquality.dqdl.exception.InvalidDataQualityRulesetException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;

public class InvalidDQRulesetTest {
    DQDLParser parser = new DQDLParser();

    private static Stream<Arguments> provideInvalidRulesets() {
        return Stream.of(
                Arguments.of(""),
                Arguments.of("Metadata = {}"),
                Arguments.of("DataSources = {}"),
                Arguments.of("Metadata = { \"Version\": \"1.0\" }"),
                Arguments.of("Metadata = { \"Version\": \"1.0\" } DataSources = {}"),
                Arguments.of("Metadata = { \"Version\": \"1.0\" } DataSources = { \"Primary\": \"Foo\" }"),
                Arguments.of("Rules = {"),
                Arguments.of("Rules = }"),
                Arguments.of("Rules = { }"),
                Arguments.of("Rules = [ ]"),
                Arguments.of("Rules = ["),
                Arguments.of("Rules = ]"),
                Arguments.of("Rules = Abcdefg123"),
                Arguments.of("Rules11 = [ ColumnValues \"load_dt\" > (now() - 1) ]"),
                Arguments.of("Rules = [ 11ColumnValues \"load_dt\" > (now() - 1) ]"),
                Arguments.of("Rules = [ ColumnValues \"load_dt\" \"load_dt_2\" > (now() - 1) ]"),
                Arguments.of("Rules = [ Completeness \"col-A\" ]"),
                Arguments.of("Rules = { Completeness \"col-A\" }"),
                Arguments.of("Rules = [ ColumnNamesMatchPattern aws_* ]"),
                Arguments.of("Rules = [ ColumnNamesMatchPattern \"aws_*\" where \"aws_id > 100\"]"),
                Arguments.of("Rules = [ IsComplete \"col-A\" > 0.05 ]"),
                Arguments.of("Rules = [ IsUnique \"col-A\" <= 1.5 ]"),
                Arguments.of("Rules = [ IsPrimaryKey \"col-A\" between 1 and 2 ]"),
                Arguments.of("Rules = [ ColumnDataType \"col-A\" ]"),
                Arguments.of("Rules = [ ColumnDataType \"col-A\" with threshold > 0.7 ]"),
                Arguments.of("Rules = [ ColumnDataType \"col-A\" \"col-B\" ]"),
                Arguments.of("Rules = [ ColumnDataType \"col_1\" in [\"Date\",\"String\"] with threshold > 0.9 with threshold > 0.7 ]"),
                Arguments.of("Rules = [ ColumnValues \"col-A\" matches ]"),
                Arguments.of("Rules = [ ColumnValues \"col-A\" now() ]"),
                Arguments.of("Rules = [ ColumnValues \"col-A\" > now() + 1 hours ]"),
                Arguments.of("Rules = [ ColumnValues \"col-A\" = (now() - 3 weeks) ]"),
                Arguments.of("Rules = [ Completeness \"col-A\" > 0.4 with threshold > 0.4]"),
                Arguments.of("Rules = [ ColumnValues \"col-A\" > 0.4 with]"),
                Arguments.of("Rules = [ ColumnValues \"col-A\" > 0.4 threshold]"),
                Arguments.of("Rules = [ ColumnValues \"col-A\" > 0.4 with threshold]"),
                Arguments.of("Rules = [ ColumnValues \"col-A\" in [1,\"2\"] ]"),
                Arguments.of("Rules = [ DataFreshness \"col-A\" <= 3 ]"),
                Arguments.of("Rules = [ DataFreshness \"col-A\" > 30 ]"),
                Arguments.of("Rules = [ DataFreshness \"col-A\" between 2 and 4 days ]"),
                Arguments.of("Rules = [ ReferentialIntegrity \"col-A\" \"reference\" \"col-A1\" ]"),
                Arguments.of("Rules = [ ReferentialIntegrity \"col-A\" = 0.99 ]"),
                Arguments.of("Rules = [ ReferentialIntegrity \"col-A\" \"reference.col-A\" = 0.99 where \"col-A > 100\"]"),
                Arguments.of("Rules = [ DatasetMatch \"reference\" = 0.99 ]"),
                Arguments.of("Rules = [ DatasetMatch \"reference\" \"ID\" ]"),
                Arguments.of("Rules = [ DatasetMatch \"reference\" \"ID\" \"colA\" ]"),
                Arguments.of("Rules = [ DatasetMatch \"reference\" \"ID\" \"colA\" > 0.9 with threshold > 0.9]"),
                Arguments.of("Rules = [ DatasetMatch \"reference\" \"ID\" \"colA\" > 0.9 where \"ID > 100\"]"),
                Arguments.of("Rules = [ SchemaMatch with threshold between 0.2 and 0.4 ]"),
                Arguments.of("Rules = [ SchemaMatch \"ref-1\" between 0.2 and 0.4 with threshold > 0.5 ]"),
                Arguments.of("Rules = [ SchemaMatch \"ref-1\" \"ref-2\" ]"),
                Arguments.of("Rules = [ RowCountMatch > 0.1 ]"),
                Arguments.of("Rules = [ RowCountMatch \"reference-1\" \"col-1\" > 0.1 ]"),
                Arguments.of("Rules = [ RowCountMatch \"reference-1\" > 0.1 with threshold > 0.1 ]"),
                Arguments.of("Rules = [ RowCountMatch \"reference-1\" > 0.1 where \"id > 100\"]"),
                Arguments.of("Rules = [ AggregateMatch > 0.1 ]"),
                Arguments.of("Rules = [ AggregateMatch \"sum(col-A)\" > 0.1 ]"),
                Arguments.of("Rules = [ AggregateMatch \"sum(col-A)\" \"sum(reference.colA)\"]"),
                Arguments.of("Rules = [ AggregateMatch \"sum(col-A)\" \"sum(reference.colA)\" > 0.8 where \"col-A > 100\"]"),
                Arguments.of("Rules = [ DetectAnomalies ]"),
                Arguments.of("Rules = [ DetectAnomalies \"col-A\"  where \"col-A > 100\"]"),
                Arguments.of("Rules = [ AllStatistics \"id\" > 0 ]"),
                Arguments.of("Rules = [ FileMatch ]"),
                Arguments.of("Rules = [ FileMatch in [] ]"),
                Arguments.of("Rules = [ FileMatch SHA SHA SHA ]"),
                Arguments.of("Rules = [ FileMatch SHA SHA SHA in [] ]"),
                Arguments.of("Rules = [ FileMatch s3Path ]"),
                Arguments.of("Rules = [ FileMatch s3Path with noHashAlgorithm ]"),
                Arguments.of("FileMatch \"S3://PATH\" \"S3://PATH\" in [\"hashList\",\"hashList\"] with hashAlgorithm = \"MD5\""),
                Arguments.of("Rules = [ FileMatch S3://PATH1 ]"),
                Arguments.of("Rules = [ FileUniqueness S3://PATH1 S3://PATH1 ]"),
                Arguments.of("Rules = [ FileFreshness between \"2024-07-15\" ]"),
                Arguments.of("Rules = [ FileFreshness \"S3://PATH\" between and \"2024-07-15\" ]"),
                Arguments.of("Rules = [ FileFreshness \"S3://PATH\" \"S3://PATH\" ]"),
                Arguments.of("Rules = [ FileFreshness > (now() 3 days) ]"),
                Arguments.of("Rules = [ FileUniqueness \"PATH\" ]"),
                Arguments.of("Rules = [ FileUniqueness ]"),
                Arguments.of("Rules = [ FileSize ]"),
                Arguments.of("Rules = [ FileSize > 1 SAM]"),
                Arguments.of("Rules = [ FileSize > 1 KB with exampleTag in [\"SAM\"] ]"),
                Arguments.of("Rules = [ FileSize > 1 KB with exampleTag != \"SAM\"]"),
                Arguments.of("Rules = [ FileSize 1 GB]"),
                Arguments.of("Rules = [ FileSize <= 1 ZB ]"),
                Arguments.of("(RowCount > 0) OR (IsComplete \"colA\") AND (IsUnique \"colA\"))"),
                Arguments.of("((RowCount > 0) AND IsComplete")
        );
    }

    private static Stream<Arguments> provideInvalidRulesetsWithAnalyzers() {
        return Stream.of(
                Arguments.of("Rules = [ ] Analyzers = [ ]"),
                Arguments.of("Rules = [ IsPrimaryKey \"col-A\"] Analyzers = [ IsComplete \"colA\" ]"),
                Arguments.of("Rules = [ IsPrimaryKey \"col-A\"] Analyzers = [ Completeness \"colA\", ]"),
                Arguments.of("Rules = [ IsPrimaryKey \"col-A\"] Analyzers = [ Completeness \"colA\", Foo ]"),
                Arguments.of("Rules = [ IsPrimaryKey \"col-A\"] Analyzers = [ Completeness \"colA\" > 1.0 ]"),
                Arguments.of("Rules = [ IsPrimaryKey \"col-A\"] Analyzers = [ Completeness \"colA\", Uniqueness \"colB\" = 1.0 ]")
        );
    }

    @ParameterizedTest
    @MethodSource("provideInvalidRulesets")
    void test_invalidRulesetParsing(String ruleset) {
        try {
            parser.parse(ruleset);
            fail("Ruleset validation exception was expected");
        } catch (InvalidDataQualityRulesetException e) {
            System.out.println(e.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("provideInvalidRulesetsWithAnalyzers")
    void test_invalidRulesetWithAnalyzersParsing(String ruleset) {
        try {
            parser.parse(ruleset);
            fail("Ruleset validation exception was expected");
        } catch (InvalidDataQualityRulesetException e) {
            System.out.println(e.getMessage());
        }
    }
}
