/*
 * DQRuleType.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Getter
public class DQRuleType {
    private final String ruleTypeName;
    private final String description;
    private final List<DQRuleParameter> parameters;
    private final String returnType;
    private final boolean isThresholdSupported;
    private final boolean isAnalyzerOnly;

    private final String scope;

    public DQRuleType(@JsonProperty(value = "rule_type_name") String ruleTypeName,
                      @JsonProperty(value = "description") String description,
                      @JsonProperty(value = "parameters") List<DQRuleParameter> parameters,
                      @JsonProperty(value = "return_type") String returnType,
                      // boolean defaults to false if not present
                      @JsonProperty(value = "is_threshold_supported") boolean isThresholdSupported,
                      @JsonProperty(value = "is_analyzer_only") boolean isAnalyzerOnly,
                        @JsonProperty(value = "scope") String scope) {
        this.ruleTypeName = ruleTypeName;
        this.description = description;
        this.parameters = parameters;
        this.returnType = returnType;
        this.isThresholdSupported = isThresholdSupported;
        this.isAnalyzerOnly = isAnalyzerOnly;
        this.scope = scope;

        if (parameters.isEmpty()) {
            return;
        }

        // There should only be one parameter that contains isVarArgs set to true and at the end of the list
        // Check all except for last param
        List<DQRuleParameter> expectedParametersToCheck = parameters.subList(0, parameters.size() - 1);

        if (expectedParametersToCheck.stream().anyMatch(DQRuleParameter::isVarArg)) {
            throw new IllegalArgumentException(
                "Property isVarArg can only be set to true on last element in parameters list");
        }
    }

    public Optional<String> verifyParameters(List<DQRuleParameter> expectedParameters,
                                             List<String> actualParameters) {
        if (!expectedParameters.isEmpty()) {

            boolean isVarArg = expectedParameters.get(
                    expectedParameters.size() - 1).isVarArg();

            if (isVarArg) {
                if (expectedParameters.size() > actualParameters.size()) {
                    return Optional.of("VarArgs needs at least one parameter");
                }

                return Optional.empty();
            }
        }

        if (expectedParameters.size() != actualParameters.size()) {
            return Optional.of("Unexpected number of parameters");
        }

        return Optional.empty();
    }

    public Map<String, String> createParameterMap(List<DQRuleParameter> dqRuleTypeParameters,
                                                  List<String> actualParameters) {
        Map<String, String> parameterMap = new LinkedHashMap<>();

        for (int i = 0; i < dqRuleTypeParameters.size(); i++) {
            String dqRuleTypeParameterName = dqRuleTypeParameters.get(i).getName();
            // If rule type needs variable arguments, add as many columns as needed.
            if (dqRuleTypeParameters.get(i).isVarArg()) {
                int counter = 0;
                // Keeps the position of VarArgs parameters.
                if (dqRuleTypeParameters.size() > 1) {
                    counter = dqRuleTypeParameters.size() - 1;
                }

                for (int j = counter; j < actualParameters.size(); j++) {
                    String newDqRuleTypeParameterName = dqRuleTypeParameterName + (j + 1);
                    String actualParameterName = actualParameters.get(j);

                    parameterMap.put(newDqRuleTypeParameterName, actualParameterName);
                }
            } else {
                parameterMap.put(dqRuleTypeParameterName, actualParameters.get(i));
            }
        }

        return parameterMap;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    private static class DQRuleTypes {
        @JsonProperty(value = "rule_types")
        private List<DQRuleType> ruleTypes;
    }

    private static final List<DQRuleType> ALL_RULES = generateRuleTypes("/rules/rules-config.json");

    static List<DQRuleType> generateRuleTypes(final String rulesConfigPath) {
        try (
            InputStream inputStream = DQRuleType.class.getResourceAsStream(rulesConfigPath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
        ) {
            String rulesConfig = reader.lines().collect(Collectors.joining("\n"));
            DQRuleTypes ruleTypes = new ObjectMapper().readValue(rulesConfig, DQRuleTypes.class);
            return ruleTypes.ruleTypes;
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Failed to load rule types", e);
        }
    }

    public static Optional<DQRuleType> getRuleType(String ruleTypeName, int parameterCount) {
        return ALL_RULES.stream()
            .filter(ruleType -> {
                int ruleTypeParameterCount = ruleType.getParameters().size();
                boolean containsVarArg =
                    ruleTypeParameterCount > 0 &&
                    ruleType.getParameters().get(ruleTypeParameterCount - 1).isVarArg();

                boolean ruleTypeNameMatches = ruleType.getRuleTypeName().equals(ruleTypeName);
                boolean parameterCountMatches = containsVarArg
                    ? parameterCount >= ruleTypeParameterCount
                    : parameterCount == ruleTypeParameterCount;

                return ruleTypeNameMatches && parameterCountMatches;
            })
            .findFirst();
    }
}
