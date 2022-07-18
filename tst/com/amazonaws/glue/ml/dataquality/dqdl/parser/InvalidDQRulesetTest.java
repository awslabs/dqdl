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
            Arguments.of("Rules = [ ColumnNamesMatchPattern aws_* ]")
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
