/*
 * DQRuleParameterValueTest.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DQRuleParameterValueTest {

    @Test
    public void test_constructorWithValueArg() {
        String value = "col-A";
        DQRuleParameterValue param = new DQRuleParameterValue(value);
        assertEquals(value, param.getValue());
        assertFalse(param.isQuoted());
        assertTrue(param.getConnectorWord().isEmpty());
    }

    @Test
    public void test_constructorWithValueAndIsQuotedArgs() {
        String value = "col-A";
        boolean isQuoted = true;
        DQRuleParameterValue param = new DQRuleParameterValue(value, isQuoted);
        assertEquals(value, param.getValue());
        assertEquals(isQuoted, param.isQuoted());
        assertTrue(param.getConnectorWord().isEmpty());
    }

    @Test
    public void test_parameterValueToStringWithNoConnectorWordAndNoQuotes() {
        String value = "col-A";
        String connectorWord = "";
        boolean isQuoted = false;
        DQRuleParameterValue param = new DQRuleParameterValue(value, isQuoted, connectorWord);
        assertEquals(value, param.toString());
    }

    @Test
    public void test_parameterValueToStringWithConnectorWordAndNoQuotes() {
        String value = "col-A";
        String connectorWord = "of";
        boolean isQuoted = false;
        DQRuleParameterValue param = new DQRuleParameterValue(value, isQuoted, connectorWord);
        assertEquals(String.format("%s %s", connectorWord, value), param.toString());
    }

    @Test
    public void test_parameterValueToStringWithConnectorWordAndWithQuotes() {
        String value = "col-A";
        String connectorWord = "of";
        boolean isQuoted = true;
        DQRuleParameterValue param = new DQRuleParameterValue(value, isQuoted, connectorWord);
        assertEquals(String.format("%s \"%s\"", connectorWord, value), param.toString());
    }

    @Test
    public void test_equalsAndHashCode() {
        String value = "col-A";
        String connectorWord = "of";
        boolean isQuoted = true;

        DQRuleParameterValue param1 = new DQRuleParameterValue(value, isQuoted, connectorWord);
        DQRuleParameterValue param2 = new DQRuleParameterValue(value, isQuoted, connectorWord);

        assertNotSame(param1, param2);
        assertEquals(param1, param2);
    }
}
