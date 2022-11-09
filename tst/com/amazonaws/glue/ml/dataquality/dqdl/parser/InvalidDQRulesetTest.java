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
            Arguments.of("Rules = [ IsComplete \"col-A\" > 0.05 ]"),
            Arguments.of("Rules = [ IsUnique \"col-A\" <= 1.5 ]"),
            Arguments.of("Rules = [ IsPrimaryKey \"col-A\" between 1 and 2 ]"),
            Arguments.of("Rules = [ ColumnValues \"col-A\" matches ]"),
            Arguments.of("Rules = [ ColumnValues \"col-A\" now() ]"),
            Arguments.of("Rules = [ ColumnValues \"col-A\" > now() + 1 hours ]"),
            Arguments.of("Rules = [ ColumnValues \"col-A\" = (now() - 3 weeks) ]"),
            Arguments.of("Rules = [ Completeness \"col-A\" > 0.4 with threshold > 0.4]"),
            Arguments.of("Rules = [ ColumnValues \"col-A\" > 0.4 with]"),
            Arguments.of("Rules = [ ColumnValues \"col-A\" > 0.4 threshold]"),
            Arguments.of("Rules = [ ColumnValues \"col-A\" > 0.4 with threshold]"),
            Arguments.of("Rules = [ ColumnValues \"col-A\" <= 0.4 with threshold between 0.4 and 0.8 ]"),
            Arguments.of("Rules = [ ColumnValues \"col-A\" > 0.4 with threshold > 0.4 ]"),
            Arguments.of("Rules = [ ColumnValues \"col-A\" in [\"2022-01-01\"] with threshold > 0.98 ]"),
            Arguments.of("Rules = [ ColumnValues \"col-A\" = 1 with threshold > 0.98 ]"),
            Arguments.of("Rules = [ ColumnValues \"col-A\" in [1,\"2\"] ]"),
            Arguments.of("ColumnValues \"col-A\" = \"2022-01-01\" with threshold > 0.98")
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
}
