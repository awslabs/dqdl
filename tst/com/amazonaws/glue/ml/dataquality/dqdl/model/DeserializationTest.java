/*
 * DeserializationTest.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeserializationTest {
    @Test
    public void test_parseRuleParameter() throws JsonProcessingException {
        String paramType = "String";
        String name = "TargetColumn";
        String description = "This is a parameter";
        boolean isVarArg = true;

        String json = String.format("{\"type\":\"%s\",\"name\":\"%s\",\"description\":\"%s\",\"is_var_arg\":%s}",
            paramType, name, description, isVarArg);

        DQRuleParameter ruleParameter = new ObjectMapper().readValue(json, DQRuleParameter.class);
        assertEquals(paramType, ruleParameter.getType());
        assertEquals(name, ruleParameter.getName());
        assertEquals(description, ruleParameter.getDescription());
        assertEquals(isVarArg, ruleParameter.isVarArg());
    }

    @Test
    public void test_parseRuleParameterWithIsVarArgSetToFalse() throws JsonProcessingException {
        String paramType = "String";
        String name = "TargetColumn";
        String description = "This is a parameter";
        boolean isVarArg = false;

        String json = String.format("{\"type\":\"%s\",\"name\":\"%s\",\"description\":\"%s\",\"is_var_arg\":%s}",
            paramType, name, description, isVarArg);

        DQRuleParameter ruleParameter = new ObjectMapper().readValue(json, DQRuleParameter.class);
        assertEquals(paramType, ruleParameter.getType());
        assertEquals(name, ruleParameter.getName());
        assertEquals(description, ruleParameter.getDescription());
        assertEquals(isVarArg, ruleParameter.isVarArg());
    }

    @Test
    public void test_parseRuleParameterWithoutIsVarArg() throws JsonProcessingException {
        String paramType = "String";
        String name = "TargetColumn";
        String description = "This is a parameter";

        String json = String.format("{\"type\":\"%s\",\"name\":\"%s\",\"description\":\"%s\"}", paramType, name, description);

        DQRuleParameter ruleParameter = new ObjectMapper().readValue(json, DQRuleParameter.class);
        assertEquals(paramType, ruleParameter.getType());
        assertEquals(name, ruleParameter.getName());
        assertEquals(description, ruleParameter.getDescription());
        assertFalse(ruleParameter.isVarArg());
    }

    @Test
    public void test_parseDQRuleType() throws JsonProcessingException {
        String ruleTypeName = "DatasetMatch";
        String ruleTypeDesc = "This rule matches two datasets";
        String returnType = "STRING";
        boolean isThresholdSupported = true;
        boolean isCompositeRuleEvaluationRowLevelSupported = false;

        // Parameter 1
        String param1Type = "String";
        String param1Name = "PrimaryDatasetAlias";
        String param1Desc = "This is the primary dataset alias";
        String param1Json = String.format(
            "{\"type\":\"%s\",\"name\":\"%s\",\"description\":\"%s\"}", param1Type, param1Name, param1Desc);

        // Parameter2
        String param2Type = "String";
        String param2Name = "ReferenceDatasetAlias";
        String param2Desc = "This is the reference dataset alias";

        String param2Json = String.format(
            "{\"type\":\"%s\",\"name\":\"%s\",\"description\":\"%s\"}", param2Type, param2Name, param2Desc);

        String json = String.format(
            "{" +
                "\"rule_type_name\":\"%s\"," +
                "\"description\":\"%s\"," +
                "\"parameters\": [ %s, %s ]," +
                "\"return_type\": \"%s\"," +
                "\"is_threshold_supported\": \"%s\"," +
                "\"is_composite_rule_evaluation_row_level_supported\": %s" +
            "}",
            ruleTypeName, ruleTypeDesc, param1Json, param2Json, returnType, isThresholdSupported,
                isCompositeRuleEvaluationRowLevelSupported);

        DQRuleType ruleType = new ObjectMapper().readValue(json, DQRuleType.class);
        assertEquals(ruleTypeName, ruleType.getRuleTypeName());
        assertEquals(ruleTypeDesc, ruleType.getDescription());
        assertEquals(returnType, ruleType.getReturnType());
        assertEquals(isThresholdSupported, ruleType.isThresholdSupported());
        assertEquals(isCompositeRuleEvaluationRowLevelSupported, ruleType.isCompositeRuleEvaluationRowLevelSupported());

        DQRuleParameter param1 = ruleType.getParameters().get(0);
        assertEquals(param1Type, param1.getType());
        assertEquals(param1Name, param1.getName());
        assertEquals(param1Desc, param1.getDescription());

        DQRuleParameter param2 = ruleType.getParameters().get(1);
        assertEquals(param2Type, param2.getType());
        assertEquals(param2Name, param2.getName());
        assertEquals(param2Desc, param2.getDescription());
    }

    @Test
    public void test_parseDQRuleTypeWithNoParameters() throws JsonProcessingException {
        String ruleTypeName = "ColumnCount";
        String ruleTypeDesc = "This rule checks the column count";
        String returnType = "NUMBER";

        String json = String.format(
            "{" +
                "\"rule_type_name\":\"%s\"," +
                "\"description\":\"%s\"," +
                "\"parameters\": [ ]," +
                "\"return_type\": \"%s\"" +
            "}",
            ruleTypeName, ruleTypeDesc, returnType);

        DQRuleType ruleType = new ObjectMapper().readValue(json, DQRuleType.class);
        assertEquals(ruleTypeName, ruleType.getRuleTypeName());
        assertEquals(ruleTypeDesc, ruleType.getDescription());
        assertEquals(returnType, ruleType.getReturnType());
        assertTrue(ruleType.getParameters().isEmpty());
    }

    @Test
    public void test_parseDQRuleTypeWithMultipleParametersAndIncorrectVarArgParameter() {
        String ruleTypeName = "ColumnCorrelation";
        String ruleTypeDesc = "This rule checks column correlation";
        String returnType = "STRING";

        // Parameter 1
        String param1Type = "String";
        String param1Name = "TargetColumn1";
        String param1Desc = "The first column";
        boolean param1IsVarArg = true;
        String param1Json = String.format(
            "{\"type\":\"%s\",\"name\":\"%s\",\"description\":\"%s\", \"is_var_arg\": %s}",
            param1Type, param1Name, param1Desc, param1IsVarArg);

        // Parameter2
        String param2Type = "String";
        String param2Name = "TargetColumn2";
        String param2Desc = "The second column";

        String param2Json = String.format("{\"type\":\"%s\",\"name\":\"%s\",\"description\":\"%s\"}", param2Type, param2Name, param2Desc);

        String json = String.format(
            "{" +
                "\"rule_type_name\":\"%s\"," +
                "\"description\":\"%s\"," +
                "\"parameters\": [ %s, %s ]," +
                "\"return_type\": \"%s\"" +
            "}", ruleTypeName, ruleTypeDesc, param1Json, param2Json, returnType);

        ValueInstantiationException thrown = assertThrows(
            ValueInstantiationException.class,
            () -> new ObjectMapper().readValue(json, DQRuleType.class)
        );

        assertEquals(IllegalArgumentException.class, thrown.getCause().getClass());
        assertTrue(thrown.getMessage().contains("Property isVarArg can only be set to true on last element in parameters list"));
    }

    @Test
    public void test_parseDQRuleTypeScope() throws JsonProcessingException {
        String ruleTypeName = "ColumnCount";
        String ruleTypeDesc = "This rule checks the column count";
        String returnType = "NUMBER";
        String scope = "table";

        String json = String.format(
            "{" +
                "\"rule_type_name\":\"%s\"," +
                "\"description\":\"%s\"," +
                "\"parameters\": [ ]," +
                "\"return_type\": \"%s\"," +
                "\"scope\": \"%s\"" +
            "}",
            ruleTypeName, ruleTypeDesc, returnType, scope);

        DQRuleType ruleType = new ObjectMapper().readValue(json, DQRuleType.class);

        assertEquals(ruleTypeName, ruleType.getRuleTypeName());
        assertEquals(ruleTypeDesc, ruleType.getDescription());
        assertEquals(returnType, ruleType.getReturnType());
        assertEquals(scope, ruleType.getScope());
        assertTrue(ruleType.getParameters().isEmpty());
    }
}
