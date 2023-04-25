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

import lombok.Getter;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Getter
public class DQRuleType {
    private final String ruleTypeName;
    private final String description;
    private final List<DQRuleParameter> parameters;
    private final String returnType;
    private final Boolean supportsThreshold;

    public DQRuleType(String ruleTypeName,
                      String description,
                      List<DQRuleParameter> parameters,
                      String returnType,
                      Boolean supportsThreshold) {
        this.ruleTypeName = ruleTypeName;
        this.description = description;
        this.parameters = parameters;
        this.returnType = returnType;
        this.supportsThreshold = supportsThreshold;

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

    public DQRuleType(String ruleTypeName, String description, List<DQRuleParameter> parameters, String returnType) {
        this(ruleTypeName, description, parameters, returnType, false);
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
            "Check the number of rows in the dataset",
            Collections.emptyList(),
            "NUMBER"
    );

    private static final DQRuleType COLUMN_COUNT_RULE_TYPE = new DQRuleType(
            "ColumnCount",
            "Checks the number of columns in the dataset",
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
            "NUMBER"
    );

    private static final DQRuleType IS_COMPLETE_RULE_TYPE = new DQRuleType(
            "IsComplete",
            "Check if all values in a given column are complete (non-null)",
            Collections.singletonList(
                    new DQRuleParameter(
                            "String",
                            "TargetColumn",
                            "Name of column to check completeness of."
                    )
            ),
            "BOOLEAN"
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

    private static final DQRuleType IS_UNIQUE_RULE_TYPE = new DQRuleType(
            "IsUnique",
            "Check if all values in a given column are unique",
            Collections.singletonList(
                    new DQRuleParameter(
                            "String",
                            "TargetColumn",
                            "Name of column to check uniqueness of."
                    )
            ),
            "BOOLEAN"
    );

    private static final DQRuleType COLUMN_MEAN_RULE_TYPE = new DQRuleType(
            "Mean",
            "Check the mean (average) of all values in a given column",
            Collections.singletonList(
                    new DQRuleParameter(
                            "String",
                            "TargetColumn",
                            "Name of column to check mean of."
                    )
            ),
            "NUMBER"
    );

    private static final DQRuleType COLUMN_SUM_RULE_TYPE = new DQRuleType(
            "Sum",
            "Check the sum of all values in a given column",
            Collections.singletonList(
                    new DQRuleParameter(
                            "String",
                            "TargetColumn",
                            "Name of column to check sum of."
                    )
            ),
            "NUMBER"
    );

    private static final DQRuleType COLUMN_STD_DEV_RULE_TYPE = new DQRuleType(
            "StandardDeviation",
            "Check the standard deviation of all values in a given column",
            Collections.singletonList(
                    new DQRuleParameter(
                            "String",
                            "TargetColumn",
                            "Name of column to check standard deviation of."
                    )
            ),
            "NUMBER"
    );

    private static final DQRuleType COLUMN_ENTROPY_RULE_TYPE = new DQRuleType(
            "Entropy",
            "Check the entropy of all values in a given column",
            Collections.singletonList(
                    new DQRuleParameter(
                            "String",
                            "TargetColumn",
                            "Name of column to check entropy of."
                    )
            ),
            "NUMBER"
    );

    private static final DQRuleType DISTINCT_VALUES_COUNT_RULE_TYPE = new DQRuleType(
            "DistinctValuesCount",
            "Check the number of distinct values in a given column",
            Collections.singletonList(
                    new DQRuleParameter(
                            "String",
                            "TargetColumn",
                            "Name of column to check distinct values count of."
                    )
            ),
            "NUMBER"
    );

    private static final DQRuleType UNIQUE_VALUE_RATIO_RULE_TYPE = new DQRuleType(
            "UniqueValueRatio",
            "Check the ratio of unique values in a given column",
            Collections.singletonList(
                    new DQRuleParameter(
                            "String",
                            "TargetColumn",
                            "Name of column to check unique value ratio of."
                    )
            ),
            "NUMBER"
    );

    private static final DQRuleType COLUMN_LENGTH_RULE_TYPE = new DQRuleType(
            "ColumnLength",
            "Check the length of values of a given column",
            Collections.singletonList(
                    new DQRuleParameter(
                            "String",
                            "TargetColumn",
                            "Name of column to check the length of the values of."
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
                            "Name of first column.",
                            true
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
                            "Name of column to return the values of."
                    )
            ),
            "STRING_ARRAY|NUMBER_ARRAY|DATE_ARRAY",
            true
    );

    private static final DQRuleType DATA_FRESHNESS_RULE_TYPE = new DQRuleType(
            "DataFreshness",
            "Check the freshness of a date column",
            Collections.singletonList(
                    new DQRuleParameter(
                            "String",
                            "TargetColumn",
                            "Name of column to check the freshness of."
                    )
            ),
            "DURATION_ARRAY"
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

    private static final DQRuleType REFERENTIAL_INTEGRITY_RULE_TYPE = new DQRuleType(
        "ReferentialIntegrity",
        "Runs a referential integrity check against a reference dataset using the given column",
        Arrays.asList(
            new DQRuleParameter(
                "String",
                "PrimaryDatasetColumns",
                "Names of columns from primary dataset separated by commas."
            ),
            new DQRuleParameter(
                "String",
                "ReferenceDatasetColumns",
                "Alias of reference dataset and comma separated names of columns from reference dataset. " +
                "The alias and the names should be separated by a period. " +
                "The names should be enclosed in curly brackets."
            )
        ),
        "NUMBER"
    );

    private static final DQRuleType DATA_SYNCHRONIZATION_RULE_TYPE = new DQRuleType(
        "DataSynchronization",
        "Runs a data synchronization check against a reference dataset using the given columns",
        Arrays.asList(
            new DQRuleParameter(
                "String",
                "ReferenceDatasetAlias",
                "Alias of reference dataset."
            ),
            new DQRuleParameter(
                "String",
                "KeyColumnMappings",
                "Mappings of key columns used for joining the two datasets."
            ),
            new DQRuleParameter(
                "String",
                "MatchColumnMappings",
                "Mappings of columns used for matching."
            )
        ),
        "NUMBER"
    );

    private static final DQRuleType SCHEMA_MATCHES_RULE_TYPE = new DQRuleType(
        "SchemaMatches",
        "Checks the schema of the primary dataset against the reference dataset",
        Collections.singletonList(
            new DQRuleParameter(
                "String",
                "ReferenceDatasetAlias",
                "Alias of reference dataset."
            )
        ),
        "BOOLEAN",
        true
    );

    private static final Map<String, DQRuleType> RULE_TYPE_MAP = new HashMap<>();

    static {
        // Not supported for re:Invent
        // RULE_TYPE_MAP.put(JOB_STATUS_RULE_TYPE.getRuleTypeName(), JOB_STATUS_RULE_TYPE);
        // RULE_TYPE_MAP.put(JOB_DURATION_RULE_TYPE.getRuleTypeName(), JOB_DURATION_RULE_TYPE);
        // RULE_TYPE_MAP.put(FILE_COUNT_RULE_TYPE.getRuleTypeName(), FILE_COUNT_RULE_TYPE);

        RULE_TYPE_MAP.put(ROW_COUNT_RULE_TYPE.getRuleTypeName(), ROW_COUNT_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_COUNT_RULE_TYPE.getRuleTypeName(), COLUMN_COUNT_RULE_TYPE);
        RULE_TYPE_MAP.put(COMPLETENESS_RULE_TYPE.getRuleTypeName(), COMPLETENESS_RULE_TYPE);
        RULE_TYPE_MAP.put(IS_COMPLETE_RULE_TYPE.getRuleTypeName(), IS_COMPLETE_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_DATA_TYPE_RULE_TYPE.getRuleTypeName(), COLUMN_DATA_TYPE_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_NAMES_MATCH_PATTERN_RULE_TYPE.getRuleTypeName(), COLUMN_NAMES_MATCH_PATTERN_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_EXISTS_RULE_TYPE.getRuleTypeName(), COLUMN_EXISTS_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_CORRELATION_RULE_TYPE.getRuleTypeName(), COLUMN_CORRELATION_RULE_TYPE);
        RULE_TYPE_MAP.put(UNIQUENESS_RULE_TYPE.getRuleTypeName(), UNIQUENESS_RULE_TYPE);
        RULE_TYPE_MAP.put(IS_UNIQUE_RULE_TYPE.getRuleTypeName(), IS_UNIQUE_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_MEAN_RULE_TYPE.getRuleTypeName(), COLUMN_MEAN_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_SUM_RULE_TYPE.getRuleTypeName(), COLUMN_SUM_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_STD_DEV_RULE_TYPE.getRuleTypeName(), COLUMN_STD_DEV_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_ENTROPY_RULE_TYPE.getRuleTypeName(), COLUMN_ENTROPY_RULE_TYPE);
        RULE_TYPE_MAP.put(DISTINCT_VALUES_COUNT_RULE_TYPE.getRuleTypeName(), DISTINCT_VALUES_COUNT_RULE_TYPE);
        RULE_TYPE_MAP.put(UNIQUE_VALUE_RATIO_RULE_TYPE.getRuleTypeName(), UNIQUE_VALUE_RATIO_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_LENGTH_RULE_TYPE.getRuleTypeName(), COLUMN_LENGTH_RULE_TYPE);
        RULE_TYPE_MAP.put(IS_PRIMARY_KEY_RULE_TYPE.getRuleTypeName(), IS_PRIMARY_KEY_RULE_TYPE);
        RULE_TYPE_MAP.put(COLUMN_VALUES_RULE_TYPE.getRuleTypeName(), COLUMN_VALUES_RULE_TYPE);
        RULE_TYPE_MAP.put(DATA_FRESHNESS_RULE_TYPE.getRuleTypeName(), DATA_FRESHNESS_RULE_TYPE);
        RULE_TYPE_MAP.put(CUSTOM_SQL_RULE_TYPE.getRuleTypeName(), CUSTOM_SQL_RULE_TYPE);
        RULE_TYPE_MAP.put(REFERENTIAL_INTEGRITY_RULE_TYPE.getRuleTypeName(), REFERENTIAL_INTEGRITY_RULE_TYPE);
        RULE_TYPE_MAP.put(DATA_SYNCHRONIZATION_RULE_TYPE.getRuleTypeName(), DATA_SYNCHRONIZATION_RULE_TYPE);
        RULE_TYPE_MAP.put(SCHEMA_MATCHES_RULE_TYPE.getRuleTypeName(), SCHEMA_MATCHES_RULE_TYPE);
    }

    public static Map<String, DQRuleType> getRuleTypeMap() {
        return RULE_TYPE_MAP;
    }
}
