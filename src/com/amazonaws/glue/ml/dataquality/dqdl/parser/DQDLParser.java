/*
 * DQDLParser.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.parser;

import com.amazonaws.glue.ml.dataquality.dqdl.DataQualityDefinitionLanguageLexer;
import com.amazonaws.glue.ml.dataquality.dqdl.DataQualityDefinitionLanguageParser;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQConstraint;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQConstraintOperator;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRule;
import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleset;
import lombok.NoArgsConstructor;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@NoArgsConstructor
public class DQDLParser {
    public DQRuleset parse(String dqdl) {
        CharStream input = CharStreams.fromString(dqdl);
        TokenStream tokens = new CommonTokenStream(new DataQualityDefinitionLanguageLexer(input));
        DataQualityDefinitionLanguageParser parser = new DataQualityDefinitionLanguageParser(tokens);
        List<DQRule> dqRules = parser.rules().dqRules().dqRule().stream().map(dqRuleContext -> {
            if (dqRuleContext.AND().size() > 0) {
                return new DQRule(
                    dqRuleContext.constraint().stream().map(this::getDQConstraint).collect(Collectors.toList()),
                    DQConstraintOperator.AND
                );
            } else if (dqRuleContext.OR().size() > 0) {
                return new DQRule(
                    dqRuleContext.constraint().stream().map(this::getDQConstraint).collect(Collectors.toList()),
                    DQConstraintOperator.OR
                );
            } else if (dqRuleContext.constraint(0) != null) {
                return new DQRule(
                    Collections.singletonList(getDQConstraint(dqRuleContext.constraint(0))),
                    DQConstraintOperator.AND
                );
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());

        return new DQRuleset(dqRules);
    }

    private DQConstraint getDQConstraint(DataQualityDefinitionLanguageParser.ConstraintContext constraintContext) {
        Map<String, String> parameters = new HashMap<>();

        if (constraintContext.jobStatusConstraint() != null) {
            String thresholdExpression =
                constraintContext.jobStatusConstraint().jobStatusExpression().getText();
            return new DQConstraint("JobStatus", parameters, Optional.of(thresholdExpression));
        } else if (constraintContext.jobDurationConstraint() != null) {
            String thresholdExpression =
                constraintContext.jobDurationConstraint().numericThresholdExpression().getText();
            return new DQConstraint("JobDuration", parameters, Optional.of(thresholdExpression));
        } else if (constraintContext.rowCountConstraint() != null) {
            String thresholdExpression =
                constraintContext.rowCountConstraint().numericThresholdExpression().getText();
            return new DQConstraint("RowCount", parameters, Optional.of(thresholdExpression));
        } else if (constraintContext.fileCountConstraint() != null) {
            String thresholdExpression =
                constraintContext.fileCountConstraint().numericThresholdExpression().getText();
            return new DQConstraint("FileCount", parameters, Optional.of(thresholdExpression));
        } else if (constraintContext.isCompleteConstraint() != null) {
            parameters.put("target-column", constraintContext.isCompleteConstraint().columnName().getText());
            return new DQConstraint("IsComplete", parameters, Optional.empty());
        } else if (constraintContext.columnHasDataTypeConstraint() != null) {
            parameters.put("target-column", constraintContext.columnHasDataTypeConstraint().columnName().getText());
            parameters.put("expected-type", constraintContext.columnHasDataTypeConstraint().columnType().getText());
            return new DQConstraint("ColumnHasDataType", parameters, Optional.empty());
        } else if (constraintContext.columnNamesMatchPatternConstraint() != null) {
            parameters.put("pattern", constraintContext.columnNamesMatchPatternConstraint().REGEX().getText());
            return new DQConstraint("ColumnNamesMatchPattern", parameters, Optional.empty());
        } else if (constraintContext.columnExistsConstraint() != null) {
            parameters.put("target-column", constraintContext.columnExistsConstraint().columnName().getText());
            return new DQConstraint("ColumnExists", parameters, Optional.empty());
        } else if (constraintContext.datasetColumnCountConstraint() != null) {
            String thresholdExpression =
                constraintContext.datasetColumnCountConstraint().numericThresholdExpression().getText();
            return new DQConstraint("DatasetColumnCount", parameters, Optional.of(thresholdExpression));
        } else if (constraintContext.columnCorrelationConstraint() != null) {
            String thresholdExpression =
                constraintContext.columnCorrelationConstraint().percentageThresholdExpression().getText();
            parameters.put("first-column", constraintContext.columnCorrelationConstraint().columnName(0).getText());
            parameters.put("second-column", constraintContext.columnCorrelationConstraint().columnName(1).getText());
            return new DQConstraint("ColumnCorrelation", parameters, Optional.of(thresholdExpression));
        } else if (constraintContext.isUniqueConstraint() != null) {
            parameters.put("target-column", constraintContext.isUniqueConstraint().columnName().getText());
            return new DQConstraint("IsUnique", parameters, Optional.empty());
        } else if (constraintContext.isPrimaryKeyConstraint() != null) {
            parameters.put("target-column", constraintContext.isPrimaryKeyConstraint().columnName().getText());
            return new DQConstraint("IsPrimaryKey", parameters, Optional.empty());
        } else if (constraintContext.columnValuesConstraint() != null) {
            parameters.put("target-column", constraintContext.columnValuesConstraint().columnName().getText());
            String thresholdExpression = constraintContext.columnValuesConstraint().dateThresholdExpression() != null
                ? constraintContext.columnValuesConstraint().dateThresholdExpression().getText()
                : constraintContext.columnValuesConstraint().numericThresholdExpression().getText();
            return new DQConstraint("ColumnValues", parameters, Optional.of(thresholdExpression));
        }

        return null;
    }
}
