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

import com.amazonaws.glue.ml.dataquality.dqdl.model.parameter.DQRuleParameterConstantValue;
import com.amazonaws.glue.ml.dataquality.dqdl.model.parameter.DQRuleParameterVariableValue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DQRuleParameterValueTest {

    @Test
    public void test_constructorWithValueArg() {
        String value = "col-A";
        DQRuleParameterConstantValue param = new DQRuleParameterConstantValue(value, false);
        assertEquals(value, param.getValue());
        assertFalse(param.isQuoted());
        assertTrue(param.getConnectorWord().isEmpty());
    }

    @Test
    public void test_constructorWithValueAndIsQuotedArgs() {
        String value = "col-A";
        boolean isQuoted = true;
        DQRuleParameterConstantValue param = new DQRuleParameterConstantValue(value, isQuoted);
        assertEquals(value, param.getValue());
        assertEquals(isQuoted, param.isQuoted());
        assertTrue(param.getConnectorWord().isEmpty());
    }

    @Test
    public void test_parameterConstantValueToStringWithNoConnectorWordAndNoQuotes() {
        String value = "col-A";
        String connectorWord = "";
        boolean isQuoted = false;
        DQRuleParameterConstantValue param = new DQRuleParameterConstantValue(value, isQuoted, connectorWord);
        assertEquals(value, param.toString());
    }

    @Test
    public void test_parameterConstantValueToStringWithConnectorWordAndNoQuotes() {
        String value = "col-A";
        String connectorWord = "of";
        boolean isQuoted = false;
        DQRuleParameterConstantValue param = new DQRuleParameterConstantValue(value, isQuoted, connectorWord);
        assertEquals(String.format("%s %s", connectorWord, value), param.toString());
    }

    @Test
    public void test_parameterConstantValueToStringWithConnectorWordAndWithQuotes() {
        String value = "col-A";
        String connectorWord = "of";
        boolean isQuoted = true;
        DQRuleParameterConstantValue param = new DQRuleParameterConstantValue(value, isQuoted, connectorWord);
        assertEquals(String.format("%s \"%s\"", connectorWord, value), param.toString());
    }

    @Test
    public void test_parameterVariableValueToString() {
        String value = "variableName";
        String connectorWord = "of";
        DQRuleParameterVariableValue param = new DQRuleParameterVariableValue(value, connectorWord);
        assertEquals(String.format("%s $%s", connectorWord, value), param.toString());
    }

    @Test
    public void test_equalsAndHashCodeForConstantValue() {
        String value = "col-A";
        String connectorWord = "of";
        boolean isQuoted = true;

        DQRuleParameterConstantValue param1 = new DQRuleParameterConstantValue(value, isQuoted, connectorWord);
        DQRuleParameterConstantValue param2 = new DQRuleParameterConstantValue(value, isQuoted, connectorWord);

        assertNotSame(param1, param2);
        assertEquals(param1, param2);
        assertEquals(param1.hashCode(), param2.hashCode());
    }

    @Test
    public void test_equalsAndHashCodeForVariableValue() {
        String value = "variableName";
        String connectorWord = "of";

        DQRuleParameterVariableValue param1 = new DQRuleParameterVariableValue(value, connectorWord);
        DQRuleParameterVariableValue param2 = new DQRuleParameterVariableValue(value, connectorWord);

        assertNotSame(param1, param2);
        assertEquals(param1, param2);
        assertEquals(param1.hashCode(), param2.hashCode());
    }
}
