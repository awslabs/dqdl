package com.amazonaws.glue.ml.dataquality.dqdl.parser;

import com.amazonaws.glue.ml.dataquality.dqdl.model.DQVariable;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.Condition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.QuotedStringOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.variable.VariableReferenceOperand;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class DQDLVariableResolver {

    // Private constructor to prevent instantiation
    private DQDLVariableResolver() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    public static Condition resolveVariablesInCondition(Condition condition, Map<String, DQVariable> variables,
                                                        Map<String, DQVariable> usedVars) {
        if (!(condition instanceof StringBasedCondition)) {
            return condition;
        }

        StringBasedCondition stringCondition = (StringBasedCondition) condition;
        List<StringOperand> resolvedOperands = new ArrayList<>();

        for (StringOperand operand : stringCondition.getOperands()) {
            if (operand instanceof VariableReferenceOperand) {
                String varName = operand.getOperand();
                DQVariable variable = variables.get(varName);
                if (variable != null) {
                    usedVars.put(varName, variable);
                    Object value = variable.getValue();
                    if (value instanceof List) {
                        for (Object listItem : (List<?>) value) {
                            resolvedOperands.add(new QuotedStringOperand(listItem.toString()));
                        }
                    } else {
                        resolvedOperands.add(new QuotedStringOperand(value.toString()));
                    }
                } else {
                    resolvedOperands.add(operand);
                }
            } else {
                resolvedOperands.add(operand);
            }
        }

        return new StringBasedCondition(
                stringCondition.getConditionAsString(),
                stringCondition.getOperator(),
                resolvedOperands,
                stringCondition.getOperands()
        );
    }
}
