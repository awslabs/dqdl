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

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@AllArgsConstructor
public class DQRuleType {
    private final String ruleTypeName;
    private final String description;
    private final List<DQRuleParameter> parameters;
    private final String returnType;

    private static final DQRuleType JOB_STATUS_RULE_TYPE = new DQRuleType(
        "JobStatus",
        "Check the status of the job which populated the table",
        Collections.emptyList(),
        "STRING"
    );

    private static final DQRuleType JOB_DURATION_RULE_TYPE = new DQRuleType(
        "JobDuration",
        "Check the duration of the job which populated the table",
        Collections.emptyList(),
        "NUMBER"
    );

    private static final DQRuleType ROW_COUNT_RULE_TYPE = new DQRuleType(
        "RowCount",
        "Check the row count of the dataset",
        Collections.emptyList(),
        "NUMBER"
    );

    private static final DQRuleType FILE_COUNT_RULE_TYPE = new DQRuleType(
        "FileCount",
        "Check the number of files in s3 the dataset comprises of",
        Collections.emptyList(),
        "NUMBER"
    );

    private static final DQRuleType COMPLETENESS_RULE_TYPE = new DQRuleType(
        "Completeness",
        "Check the percentage of complete (non-null) values in a given column",
        Collections.singletonList(
            new DQRuleParameter(
                "String",
                "TargetColumn",
                "Name of column to check completeness of."
            )
        ),
        "PERCENTAGE"
    );

    private static final DQRuleType COLUMN_DATA_TYPE_RULE_TYPE = new DQRuleType(
        "ColumnDataType",
        "Check the data type of the given column",
        Collections.singletonList(
            new DQRuleParameter(
                "String",
                "TargetColumn",
                "Name of column to check data type of."
            )
        ),
        "STRING"
    );

    private static final DQRuleType COLUMN_NAMES_MATCH_PATTERN_RULE_TYPE = new DQRuleType(
        "ColumnNamesMatchPattern",
        "Checks if the names of the columns in the dataset match a given pattern",
        Collections.singletonList(
            new DQRuleParameter(
                "String",
                "PatternToMatch",
                "Pattern to match against the names of the columns."
            )
        ),
        "BOOLEAN"
    );

    private static final DQRuleType COLUMN_EXISTS_RULE_TYPE = new DQRuleType(
        "ColumnExists",
        "Check the existence of a given column",
        Collections.singletonList(
            new DQRuleParameter(
                "String",
                "TargetColumn",
                "Name of column to check existence of."
            )
        ),
        "BOOLEAN"
    );

    private static final DQRuleType DATASET_COLUMN_COUNT_RULE_TYPE = new DQRuleType(
        "DatasetColumnsCount",
        "Checks the number of columns in the dataset",
        Collections.emptyList(),
        "NUMBER"
    );

    private static final DQRuleType COLUMN_CORRELATION_RULE_TYPE = new DQRuleType(
        "ColumnCorrelation",
        "Check the correlation between two given columns",
        Arrays.asList(
            new DQRuleParameter(
                "String",
                "TargetColumn1",
                "Name of first column."
            ),
            new DQRuleParameter(
                "String",
                "TargetColumn2",
                "Name of second column."
            )
        ),
        "NUMBER"
    );

    private static final DQRuleType UNIQUENESS_RULE_TYPE = new DQRuleType(
        "Uniqueness",
        "Check the percentage of unique values in a given column",
        Collections.singletonList(
            new DQRuleParameter(
                "String",
                "TargetColumn",
                "Name of column to check uniqueness of."
            )
        ),
        "NUMBER"
    );

    private static final DQRuleType IS_PRIMARY_KEY_RULE_TYPE = new DQRuleType(
        "IsPrimaryKey",
        "Check if a given column contains a primary key, by checking for uniqueness and completeness",
        Collections.singletonList(
            new DQRuleParameter(
                "String",
                "TargetColumn",
                "Name of column to check for primary key attributes."
            )
        ),
        "BOOLEAN"
    );

    private static final DQRuleType COLUMN_VALUES_RULE_TYPE = new DQRuleType(
        "ColumnValues",
        "Returns the column values of a given column",
        Collections.singletonList(
            new DQRuleParameter(
                "String",
                "TargetColumn",
                "Name of column return the values of."
            )
        ),
        "STRING_ARRAY|NUMBER_ARRAY|DATE_ARRAY"
    );

    private static final DQRuleType CUSTOM_SQL_RULE_TYPE = new DQRuleType(
        "CustomSql",
        "Runs a custom SQL against the dataset and returns a single value",
        Collections.singletonList(
            new DQRuleParameter(
                "String",
                "CustomSqlStatement",
                "Custom SQL statement to run against the dataset."
            )
        ),
        "NUMBER"
    );

    private static final Map<String, DQRuleType> RULE_TYPE_MAP = new HashMap<>();

    static {
        RULE_TYPE_MAP.put(JOB_STATUS_RULE_TYPE.getRuleTypeName(), JOB_STATUS_RULE_TYPE);
        RULE_TYPE_MAP.put(JOB_DURATION_RULE_TYPE.getRuleTypeName(), JOB_DURATION_RULE_TYPE);
        RULE_TYPE_MAP.put(ROW_COUNT_RULE_TYPE.getRuleTypeName(), ROW_COUNT_RULE_TYPE);
        RULE_TYPE_MAP.put(FILE_COUNT_RULE_TYPE.getRuleTypeName(), FILE_COUNT_RULE_TYPE);
        RULE_TYPE_MAP.put(COMPLETENESS_RULE_TYPE.getRuleTypeName(), COMPLETENESS_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_DATA_TYPE_RULE_TYPE.getRuleTypeName(), COLUMN_DATA_TYPE_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_NAMES_MATCH_PATTERN_RULE_TYPE.getRuleTypeName(), COLUMN_NAMES_MATCH_PATTERN_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_EXISTS_RULE_TYPE.getRuleTypeName(), COLUMN_EXISTS_RULE_TYPE);
        RULE_TYPE_MAP.put(DATASET_COLUMN_COUNT_RULE_TYPE.getRuleTypeName(), DATASET_COLUMN_COUNT_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_CORRELATION_RULE_TYPE.getRuleTypeName(), COLUMN_CORRELATION_RULE_TYPE);
        RULE_TYPE_MAP.put(UNIQUENESS_RULE_TYPE.getRuleTypeName(), UNIQUENESS_RULE_TYPE);
        RULE_TYPE_MAP.put(IS_PRIMARY_KEY_RULE_TYPE.getRuleTypeName(), IS_PRIMARY_KEY_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_VALUES_RULE_TYPE.getRuleTypeName(), COLUMN_VALUES_RULE_TYPE);
        RULE_TYPE_MAP.put(CUSTOM_SQL_RULE_TYPE.getRuleTypeName(), CUSTOM_SQL_RULE_TYPE);
    }

    public static Map<String, DQRuleType> getRuleTypeMap() {
        return RULE_TYPE_MAP;
    }
}
