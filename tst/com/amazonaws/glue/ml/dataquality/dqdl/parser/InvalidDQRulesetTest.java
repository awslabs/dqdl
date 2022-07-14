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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

public class InvalidDQRulesetTest {
    DQDLParser dqdlParser = new DQDLParser();

    @Test
    void test_randomCharactersRulesetThrowsException() {
        String dqdl = "Rules = Abcdefg123";
        try {
            dqdlParser.parse(dqdl);;
            fail("Ruleset validation exception was expected");
        } catch (InvalidDataQualityRulesetException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    void test_missingRulesThrowsException() {
        String dqdl = "Rules = [ ]";
        try {
            dqdlParser.parse(dqdl);;
            fail("Ruleset validation exception was expected");
        } catch (InvalidDataQualityRulesetException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    void test_invalidRulesIdentifierThrowsException() {
        String dqdl = "Rules11 = [ ColumnValues \"load_dt\" > (now() - 1) ]";
        try {
            dqdlParser.parse(dqdl);;
            fail("Ruleset validation exception was expected");
        } catch (InvalidDataQualityRulesetException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    void test_invalidRuleIdentifierThrowsException() {
        String dqdl = "Rules = [ 11ColumnValues \"load_dt\" > (now() - 1) ]";
        try {
            dqdlParser.parse(dqdl);;
            fail("Ruleset validation exception was expected");
        } catch (InvalidDataQualityRulesetException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    void test_incorrectNumberOfParametersThrowsException() {
        String dqdl = "Rules = [ ColumnValues \"load_dt\" \"load_dt_2\" > (now() - 1) ]";
        try {
            dqdlParser.parse(dqdl);;
            fail("Ruleset validation exception was expected");
        } catch (InvalidDataQualityRulesetException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    void test_missingConditionForNonBooleanRuleTypeThrowsException() {
        String dqdl = "Rules = [ Completeness \"col-A\" ]";
        try {
            dqdlParser.parse(dqdl);;
            fail("Ruleset validation exception was expected");
        } catch (InvalidDataQualityRulesetException e) {
            System.out.println(e.getMessage());
        }
    }
}
