package com.amazonaws.glue.ml.dataquality.dqdl.parser;

import com.amazonaws.glue.ml.dataquality.dqdl.model.parameter.DQRuleParameterValue;
import com.amazonaws.glue.ml.dataquality.dqdl.model.parameter.DQRuleParameterConstantValue;
import com.amazonaws.glue.ml.dataquality.dqdl.model.parameter.DQRuleParameterVariableValue;
import com.amazonaws.glue.ml.dataquality.dqdl.model.variable.DQVariable;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.Condition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.QuotedStringOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.variable.VariableReferenceOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.variable.VariableResolutionResult;
import com.amazonaws.glue.ml.dataquality.dqdl.util.Either;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

public final class DQDLVariableResolver {

    // Private constructor to prevent instantiation
    private DQDLVariableResolver() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    public static Either<String, Condition> resolveVariablesInCondition(Condition condition,
                                                                       Map<String, DQVariable> variables) {
        if (!(condition instanceof StringBasedCondition)) {
            return Either.fromRight(condition);
        }

        StringBasedCondition stringCondition = (StringBasedCondition) condition;
        List<StringOperand> resolvedOperands = new ArrayList<>();

        for (StringOperand operand : stringCondition.getOperands()) {
            if (operand instanceof VariableReferenceOperand) {
                String varName = operand.getOperand();
                Either<String, List<StringOperand>> resolvedOperand = resolveVariableOperand(varName, variables);
                if (resolvedOperand.isLeft()) {
                    return Either.fromLeft(resolvedOperand.getLeft());
                }
                resolvedOperands.addAll(resolvedOperand.getRight());
            } else {
                resolvedOperands.add(operand);
            }
        }

        return Either.fromRight(new StringBasedCondition(
                stringCondition.getConditionAsString(),
                stringCondition.getOperator(),
                resolvedOperands,
                stringCondition.getOperands()
        ));
    }

    public static Either<String, LinkedHashMap<String, DQRuleParameterValue>> resolveVariablesInParameters(
            Map<String, DQRuleParameterValue> parameters,
            Map<String, DQVariable> variables) {

        LinkedHashMap<String, DQRuleParameterValue> resolvedParameters = new LinkedHashMap<>();

        for (Map.Entry<String, DQRuleParameterValue> entry : parameters.entrySet()) {
            String key = entry.getKey();
            DQRuleParameterValue paramValue = entry.getValue();

            if (paramValue instanceof DQRuleParameterVariableValue) {
                DQRuleParameterVariableValue variableValue = (DQRuleParameterVariableValue) paramValue;
                String varName = variableValue.getUnresolvedValue();
                Either<String, String> resolvedValue = resolveStringVariable(varName, variables);

                if (resolvedValue.isLeft()) {
                    return Either.fromLeft(resolvedValue.getLeft());
                }

                resolvedParameters.put(key, new DQRuleParameterConstantValue(
                        resolvedValue.getRight(),
                        true,  // Assuming resolved variables should be quoted
                        variableValue.getConnectorWord(),
                        "$" + varName  // Keep the original unresolved value
                ));
            } else {
                resolvedParameters.put(key, paramValue);
            }
        }

        return Either.fromRight(resolvedParameters);
    }

    private static Either<String, List<StringOperand>> resolveVariableOperand(String varName,
                                                                              Map<String, DQVariable> variables) {
        DQVariable variable = variables.get(varName);
        if (variable == null) {
            return Either.fromLeft("Variable not found: " + varName);
        }

        List<StringOperand> resolvedOperands = new ArrayList<>();
        switch (variable.getType()) {
            case STRING:
                resolvedOperands.add(new QuotedStringOperand(variable.getValue().toString()));
                break;
            case STRING_ARRAY:
                List<String> values = (List<String>) variable.getValue();
                for (String value : values) {
                    resolvedOperands.add(new QuotedStringOperand(value));
                }
                break;
            default:
                return Either.fromLeft(
                        String.format("Invalid variable type for '%s': expected STRING or STRING_ARRAY, but got %s",
                                varName, variable.getType()));
        }
        return Either.fromRight(resolvedOperands);
    }

    private static Either<String, String> resolveStringVariable(String varName, Map<String, DQVariable> variables) {
        DQVariable variable = variables.get(varName);
        if (variable == null) {
            return Either.fromLeft("Variable not found: " + varName);
        }

        if (variable.getType() != DQVariable.VariableType.STRING) {
            return Either.fromLeft(
                    String.format("Invalid variable type for '%s': expected STRING, but got %s",
                            varName, variable.getType()));
        }

        return Either.fromRight(variable.getValue().toString());
    }

    public static Either<String, VariableResolutionResult> resolveVariables(
            LinkedHashMap<String, DQRuleParameterValue> parameters,
            Condition condition,
            Condition thresholdCondition,
            Map<String, DQVariable> dqVariables) {

        // Resolve variables in parameters
        Either<String, LinkedHashMap<String, DQRuleParameterValue>> resolvedParamsEither =
                resolveVariablesInParameters(parameters, dqVariables);

        if (resolvedParamsEither.isLeft()) {
            return Either.fromLeft("Error resolving parameters: " + resolvedParamsEither.getLeft());
        }

        LinkedHashMap<String, DQRuleParameterValue> resolvedParameters = resolvedParamsEither.getRight();

        // Resolve variables in condition
        Either<String, Condition> resolvedConditionEither =
                (condition != null) ? resolveVariablesInCondition(condition, dqVariables)
                        : Either.fromRight(null);

        if (resolvedConditionEither.isLeft()) {
            return Either.fromLeft("Error resolving condition: " + resolvedConditionEither.getLeft());
        }

        Condition resolvedCondition = resolvedConditionEither.getRight();

        return Either.fromRight(new VariableResolutionResult(
                resolvedParameters, resolvedCondition, thresholdCondition));
    }
}
