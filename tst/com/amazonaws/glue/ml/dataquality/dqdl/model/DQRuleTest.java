/*
 * DQRuleTest.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import com.amazonaws.glue.ml.dataquality.dqdl.exception.InvalidDataQualityRulesetException;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.Condition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.date.DateBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.date.DateExpression;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number.NumberBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringOperand;
import com.amazonaws.glue.ml.dataquality.dqdl.parser.DQDLParser;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number.NumericOperandTest.testEvaluator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/*
 * The purpose of this test is to ensure that parsing a rule and
 * converting it back to a string yields the original raw rule.
 */
class DQRuleTest {
    DQDLParser parser = new DQDLParser();

    @ParameterizedTest
    @MethodSource("provideRawRules")
    void test_ruleParsingAndGeneratingWithParser(String rule) {
        try {
            DQRuleset dqRuleset = parser.parse(String.format("Rules = [ %s ]", rule));
            assertEquals(1, dqRuleset.getRules().size());
            DQRule dqRule = dqRuleset.getRules().get(0);
            String dqRuleAsString = dqRule.toString();
            assertEquals(rule, dqRuleAsString);
        } catch (InvalidDataQualityRulesetException e) {
            fail(e.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("provideRawRules")
    void test_rulesEqualWhenRepresentationsEqual(String ruleStringRepr) {
        try {
            DQRule rule1 = parser.parse("Rules = [ " + ruleStringRepr + " ]").getRules().get(0);
            DQRule rule2 = parser.parse("Rules = [ " + ruleStringRepr + " ]").getRules().get(0);

            assertEquals(rule1, rule2);
            assertTrue(rule1.equals(rule2));
            assertEquals(rule1.hashCode(), rule2.hashCode());
            assertNotSame(rule1, rule2);
        } catch (InvalidDataQualityRulesetException e) {
            fail(e.getMessage());
        }
    }

    private static Stream<Arguments> provideRawRules() {
        return Stream.of(
            Arguments.of("IsPrimaryKey \"colA\""),
            Arguments.of("IsPrimaryKey \"colA\" \"colB\""),
            Arguments.of("IsPrimaryKey colA \"col B\""),
            Arguments.of("IsPrimaryKey \"colA\" \"colB\" \"colC\""),
            Arguments.of("IsPrimaryKey \"colA\" where \"colA > 100\""),
            Arguments.of("RowCount = 100"),
            Arguments.of("RowCount != 100"),
            Arguments.of("RowCount = -100"),
            Arguments.of("RowCount = 100 where \"colA > 100\""),
            Arguments.of("RowCount between (0.9 * average(last(10))) and 1.1 * average(last(10))"),
            Arguments.of("RowCount not between (0.9 * average(last(10))) and 1.1 * average(last(10))"),
            Arguments.of("RowCountMatch \"reference\" = 1.0"),
            Arguments.of("RowCountMatch \"reference\" >= 0.95"),
            Arguments.of("RowCountMatch \"reference\" between 0.8 and 0.98"),
            Arguments.of("Completeness \"col_1\" between 0.5 and 0.8"),
            Arguments.of("Completeness of col_1 between 0.5 and 0.8"),
            Arguments.of("Completeness of col_1 not between 0.5 and 0.8"),
            Arguments.of("Completeness \"col_1\" between 0.5 and 0.8 where \"col-A > 100\""),
            Arguments.of("IsComplete \"col_1\""),
            Arguments.of("IsComplete \"col_1\" where \"col-A > 100\""),
            Arguments.of("Completeness \"col_1\" between -0.5 and -0.4"),
            Arguments.of("Completeness \"col_1\" between (0.9 * avg(last(10))) and (1.1 * avg(last(10)))"),
            Arguments.of("ColumnDataType \"col_1\" = \"String\""),
            Arguments.of("ColumnDataType \"col_1\" != \"String\""),
            Arguments.of("ColumnDataType \"col_2\" = \"Integer\""),
            Arguments.of("ColumnDataType \"col_1\" = \"String\" with threshold between 0.4 and 0.8"),
            Arguments.of("ColumnDataType \"col_1\" in [\"Date\",\"String\"]"),
            Arguments.of("ColumnDataType \"col_1\" in [\"Date\",\"String\"] with threshold > 0.9"),
            Arguments.of("ColumnDataType \"col_1\" = \"String\" where \"col-A > 100\""),
            Arguments.of("ColumnNamesMatchPattern \"aws_.*_[a-zA-Z0-9]+\""),
            Arguments.of("ColumnExists \"load_dt\""),
            Arguments.of("ColumnCount >= 100"),
            Arguments.of("ColumnCount = avg(std(last(10)))"),
            Arguments.of("ColumnCount != avg(std(last(10)))"),
            Arguments.of("ColumnCount = avg(std(last(percentile(1,2,3))))"),
            Arguments.of("ColumnCount > -100.123456"),
            Arguments.of("ColumnCorrelation \"col_1\" \"col_2\" between 0.4 and 0.8"),
            Arguments.of("ColumnCorrelation of col_1 col_2 between 0.4 and 0.8"),
            Arguments.of("ColumnCorrelation of col_1 and \"col abc\" between 0.4 and 0.8"),
            Arguments.of("ColumnCorrelation \"col_1\" \"col_2\" between -0.44444 and 0.888888"),
            Arguments.of("ColumnCorrelation \"col_1\" \"col_2\" between 0.4 and 0.8 where \"col-A > 100\""),
            Arguments.of("Uniqueness \"col_1\" between 0.1 and 0.2"),
            Arguments.of("Uniqueness \"col_1\" between 0.1 and 0.2 where \"col-A > 100\""),
            Arguments.of("IsUnique \"col_1\""),
            Arguments.of("IsUnique \"col_1\" where \"col-A > 100\""),
            Arguments.of("Uniqueness \"col_1\" between -0.00000001 and 0.00000000000002"),
            Arguments.of("ColumnValues \"col_1\" between \"2022-06-01\" and \"2022-06-30\""),
            Arguments.of("ColumnValues \"col_1\" between \"2022-06-01\" and \"2022-06-30\" where \"col-A > 100\""),
            Arguments.of("ColumnValues \"load_dt\" > (now() - 1 days)"),
            Arguments.of("ColumnValues \"order-id\" in [1,2,3,4]"),
            Arguments.of("ColumnValues \"order-id\" in [1,2,3,4,NULL]"),
            Arguments.of("ColumnValues \"order-id\" not in [1,2,3,4]"),
            Arguments.of("ColumnValues \"order-id\" in [\"1\",\"2\",\"3\",\"4\"]"),
            Arguments.of("ColumnValues \"order-id\" not in [\"1\",\"2\",\"3\",\"4\"]"),
            Arguments.of("Sum \"col_A-B.C\" > 100.0"),
            Arguments.of("Sum \"col_A-B.C\" > -100.0"),
            Arguments.of("Sum \"col_A-B.C\" > -100.0 where \"col-A > 100\""),
            Arguments.of("Mean \"col_A-B.CD\" between 10 and 20"),
            Arguments.of("Mean \"col_A-B.CD\" between -20 and -10"),
            Arguments.of("Mean \"col_A-B.CD\" between -20 and -10 where \"col-A > 100\""),
            Arguments.of("StandardDeviation \"col_A-B.CD\" <= 10.0"),
            Arguments.of("StandardDeviation \"col_A-B.CD\" <= -10000.0"),
            Arguments.of("StandardDeviation \"col_A-B.CD\" <= -10000.0 where \"col-A > 100\""),
            Arguments.of("Entropy \"col_A-B.CD\" <= 10.0"),
            Arguments.of("Entropy \"col_A-B.CD\" between 10 and 30"),
            Arguments.of("Entropy \"col_A-B.CD\" between 10 and 30 where \"col-A > 100\""),
            Arguments.of("DistinctValuesCount \"col_A-B.CD\" > 1000"),
            Arguments.of("DistinctValuesCount \"col_A-B.CD\" between 10 and 30"),
            Arguments.of("DistinctValuesCount \"col_A-B.CD\" between 10 and 30 where \"col-A > 100\""),
            Arguments.of("UniqueValueRatio \"col_A-B.CD\" < 0.5"),
            Arguments.of("UniqueValueRatio \"col_A-B.CD\" between 0.1 and 0.5"),
            Arguments.of("UniqueValueRatio \"col_A-B.CD\" between 0.1 and 0.5 where \"col-A > 100\""),
            Arguments.of("ColumnLength \"col_A-B.CD\" < 10"),
            Arguments.of("ColumnLength \"col_A-B.CD\" >= 100"),
            Arguments.of("ColumnLength \"col_A-B.CD\" >= 100 where \"col-A > 100\""),
            Arguments.of("ColumnValues \"col-A\" matches \"[a-zA-Z0-9]*\""),
            Arguments.of("ColumnValues \"col-A\" not matches \"[a-zA-Z0-9]*\""),
            Arguments.of("ColumnValues \"col-A\" >= now()"),
            Arguments.of("ColumnValues \"col-A\" between (now() - 3 hours) and now()"),
            Arguments.of("ColumnValues \"col-A\" not between (now() - 3 hours) and now()"),
            Arguments.of("ColumnValues \"col-A\" between now() and (now() + 3 hours)"),
            Arguments.of("ColumnValues \"col-A\" < (now() + 4 days)"),
            Arguments.of("ColumnValues \"col-A\" = (now() - 3 hours)"),
            Arguments.of("ColumnValues \"col-A\" != (now() - 3 hours)"),
            Arguments.of("ColumnValues \"col-A\" in [now(),(now() - 3 hours),now(),(now() + 4 days)]"),
            Arguments.of("ColumnValues \"col-A\" not in [now(),(now() - 3 hours),now(),(now() + 4 days)]"),
            Arguments.of("ColumnValues \"col-A\" between (now() - 3 hours) and (now() + 14 days)"),
            Arguments.of("ColumnValues \"col-A\" not between (now() - 3 hours) and (now() + 14 days)"),
            Arguments.of("ColumnValues \"col-A\" matches \"[a-z]*\" with threshold <= 0.4"),
            Arguments.of("ColumnValues \"col-A\" not matches \"[a-z]*\" with threshold <= 0.4"),
            Arguments.of("ColumnValues \"col-A\" in [\"A\",\"B\"] with threshold <= 0.4"),
            Arguments.of("ColumnValues \"col-A\" in [1,2,3] with threshold > 0.98"),
            Arguments.of("ColumnValues \"col-A\" = \"A\" with threshold > 0.98"),
            Arguments.of("ColumnValues \"col-A\" = NULL"),
            Arguments.of("ColumnValues \"col-A\" = EMPTY"),
            Arguments.of("ColumnValues \"col-A\" = WHITESPACES_ONLY"),
            Arguments.of("ColumnValues \"col-A\" != NULL"),
            Arguments.of("ColumnValues \"col-A\" != EMPTY"),
            Arguments.of("ColumnValues \"col-A\" != WHITESPACES_ONLY"),
            Arguments.of("ColumnValues \"col-A\" in [\"a\",NULL]"),
            Arguments.of("ColumnValues \"col-A\" in [\"a\",NULL]"),
            Arguments.of("ColumnValues \"col-A\" not in [\"a\",NULL]"),
            Arguments.of("ColumnValues \"col-A\" in [\"a\",NULL,EMPTY,WHITESPACES_ONLY]"),
            Arguments.of("ColumnValues \"col-A\" in [NULL,EMPTY,WHITESPACES_ONLY]"),
            Arguments.of("(ColumnValues \"col-A\" not in [NULL,EMPTY,WHITESPACES_ONLY]) OR (ColumnValues \"col-B\" != WHITESPACES_ONLY)"),
            Arguments.of("(ColumnValues \"col-A\" in [NULL,EMPTY,WHITESPACES_ONLY]) AND (ColumnValues \"col-B\" != WHITESPACES_ONLY)"),
            Arguments.of("ColumnValues \"col-A\" <= 0.4 with threshold between 0.4 and 0.8"),
            Arguments.of("ColumnValues \"col-A\" <= 0.4 with threshold not between 0.4 and 0.8"),
            Arguments.of("ColumnValues \"col-A\" > 0.4 with threshold > 0.4"),
            Arguments.of("ColumnValues \"col-A\" in [\"2022-01-01\"] with threshold > 0.98"),
            Arguments.of("ColumnValues \"col-A\" = NULL"),
            Arguments.of("ColumnValues \"col-A\" != NULL"),
            Arguments.of("ColumnValues \"col-A\" in [NULL]"),
            Arguments.of("ColumnValues \"col-A\" in [\"2022-01-01\",NULL] with threshold > 0.98"),
            Arguments.of("ColumnValues \"col-A\" not in [\"2022-01-01\",NULL] with threshold > 0.98"),
            Arguments.of("ColumnValues \"col-A\" = 1 with threshold > 0.98"),
            Arguments.of("ColumnValues \"col-A\" = \"2022-01-01\" with threshold > 0.98"),
            Arguments.of("DataFreshness \"col-A\" <= 3 days"),
            Arguments.of("DataFreshness \"col-A\" > 30 hours"),
            Arguments.of("DataFreshness \"col-A\" between 2 days and 4 days"),
            Arguments.of("DataFreshness \"col-A\" <= 3 days where \"col-A > 100\""),
            Arguments.of("ReferentialIntegrity \"col-A\" \"reference.col-A1\" between 0.4 and 0.6"),
            Arguments.of("ReferentialIntegrity \"col-A\" \"reference.col-A1\" > 0.98"),
            Arguments.of("ReferentialIntegrity \"col-A\" \"reference.col-A1\" = 0.99"),
            Arguments.of("ReferentialIntegrity \"col-A,col-B\" \"reference.{col-A1,col-A2}\" = 0.99"),
            Arguments.of("DatasetMatch \"reference\" \"ID1,ID2\" = 0.99"),
            Arguments.of("DatasetMatch \"reference\" \"ID1,ID2\" \"colA,colB,colC\" = 0.99"),
            Arguments.of("DatasetMatch \"reference\" \"ID1->ID11,ID2->ID22\" \"colA->colAA\" between 0.4 and 0.8"),
            Arguments.of("SchemaMatch \"ref-1\" between 0.4 and 0.9"),
            Arguments.of("SchemaMatch \"ref-1\" >= 0.6"),
            Arguments.of("SchemaMatch \"ref-1\" >= 1.0"),
            Arguments.of("AggregateMatch \"sum(col-A)\" \"sum(colB)\" > 0.9"),
            Arguments.of("AggregateMatch \"sum(col-A)\" \"sum(reference.colA)\" > 0.1"),
            Arguments.of("AggregateMatch \"avg(col-A)\" \"avg(reference.colA)\" between 0.8 and 0.9"),
            Arguments.of("AggregateMatch \"SUM(col-A)\" \"SUM(reference.colA)\" >= 0.95"),
            Arguments.of("CustomSql \"select count(*) from primary\" > 0"),
            Arguments.of("CustomSql \"select col-A from primary\""),
            Arguments.of("CustomSql \"select col-A from primary\" with threshold > 0.5"),
            Arguments.of("DetectAnomalies \"RowCount\""),
            Arguments.of("DetectAnomalies of RowCount"),
            Arguments.of("DetectAnomalies of Completeness of \"colA\""),
            Arguments.of("DetectAnomalies of ColumnCorrelation of \"colA\" and \"colB\""),
            Arguments.of("FileMatch \"S3://PATH\" in [\"hashList\"]"),
            Arguments.of("FileMatch \"S3://PATH\" in [\"hashList\",\"hashList\"]"),
            Arguments.of("FileMatch in [\"hashList\",\"hashList\"]"),
            Arguments.of("FileMatch \"S3://PATH\" in [\"hashList\",\"hashList\"] with \"hashAlgorithm\" = \"MD5\""),
            Arguments.of("FileMatch \"S3://PATH1\" \"S3://PATH2\" with \"randomTagThing\" = \"@sampom\""),
            Arguments.of("FileMatch \"S3://PATH1\" in [\"a\"] with \"tag1\" = \"sampom\" with \"tag2\" = \"pomsam\""),
            Arguments.of("FileMatch \"S3://PATH1\" \"S3://PATH2\""),
            Arguments.of("FileUniqueness \"S3://PATH1\" >= 0.9"),
            Arguments.of("FileFreshness \"S3://PATH\" between \"2023-02-07\" and \"2024-07-15\""),
            Arguments.of("FileFreshness \"S3://PATH\" > (now() - 3 days)")
        );
    }

    @Test
    void test_fileFileFreshnessParsing() throws Exception {
        String fileRules = "Rules = [ " +
                "FileFreshness \"S3://path\" between \"2023-02-07\" and \"2024-07-15\", " +
                "FileFreshness \"S3://path\" > (now() - 3 days), " +
                "FileFreshness \"S3://path\" < (now() - 4 days), " +
                "FileFreshness between \"2023-02-07\" and \"2024-07-15\" " +
                "]";
        DQRuleset dqRuleset = parser.parse(fileRules);
        List<DQRule> ruleList = dqRuleset.getRules();
        DQRule rule0 = ruleList.get(0);

        DateBasedCondition c0 = (DateBasedCondition) rule0.getCondition();
        assertEquals("FileFreshness", rule0.getRuleType());
        assertEquals("S3://path", rule0.getParameters().get("DataPath"));
        assertEquals("2023-02-07", removeQuotes(c0.getOperands().get(0).getFormattedExpression()));
        assertEquals("2024-07-15", removeQuotes(c0.getOperands().get(1).getFormattedExpression()));

        DQRule rule1 = ruleList.get(1);
        DateBasedCondition c1 = (DateBasedCondition) rule1.getCondition();
        assertEquals("FileFreshness", rule1.getRuleType());
        assertEquals("S3://path", rule1.getParameters().get("DataPath"));
        assertEquals("GREATER_THAN", c1.getOperator().toString());
        assertEquals("(now() - 3 days)", c1.getOperands().get(0).getFormattedExpression());

        DQRule rule2 = ruleList.get(2);
        DateBasedCondition c2 = (DateBasedCondition) rule2.getCondition();
        assertEquals("FileFreshness", rule2.getRuleType());
        assertEquals("S3://path", rule2.getParameters().get("DataPath"));
        assertEquals("LESS_THAN", c2.getOperator().toString());
        assertEquals("(now() - 4 days)", c2.getOperands().get(0).getFormattedExpression());

        DQRule rule3 = ruleList.get(3);
        DateBasedCondition c3 = (DateBasedCondition) rule3.getCondition();
        assertEquals("FileFreshness", rule3.getRuleType());
        assertFalse(rule3.getParameters().containsKey("DataPath"));
        assertEquals("2023-02-07", removeQuotes(c3.getOperands().get(0).getFormattedExpression()));
        assertEquals("2024-07-15", removeQuotes(c3.getOperands().get(1).getFormattedExpression()));
    }

    @Test
    void test_checksumRuleParsing() throws Exception {
        String fileRules = "Rules = [ " +
                "FileMatch in [\"exampleHash\"] with \"hashAlgorithm\" = \"MD5\" with \"dataFrame\" = \"true\" ," +
                "FileMatch \"s3://sampom-bucket2/\" in [\"exampleHash2\"] with \"hashAlgorithm\" = \"SHA-256\" ," +
                "FileMatch \"s3://sampom-bucket3/\" in [\"exampleHash3\"] ," +
                "FileMatch in [\"exampleHash4\"] with \"dataFrame\" = \"true\"" +
                "]";
        DQRuleset dqRuleset = parser.parse(fileRules);
        List<DQRule> ruleList = dqRuleset.getRules();

        DQRule rule0 = ruleList.get(0);
        assertEquals("FileMatch", rule0.getRuleType());
        assertEquals("exampleHash", ((StringBasedCondition) rule0.getCondition()).getOperands().get(0).getOperand());
        assertEquals("MD5", rule0.getTags().get("hashAlgorithm"));
        assertEquals("true", rule0.getTags().get("dataFrame"));

        DQRule rule1 = ruleList.get(1);
        assertEquals("FileMatch", rule1.getRuleType());
        assertEquals("s3://sampom-bucket2/", rule1.getParameters().get("DataPath"));
        assertEquals("exampleHash2", ((StringBasedCondition) rule1.getCondition()).getOperands().get(0).getOperand());
        assertEquals("SHA-256", rule1.getTags().get("hashAlgorithm"));

        DQRule rule2 = ruleList.get(2);
        assertEquals("FileMatch", rule2.getRuleType());
        assertEquals("s3://sampom-bucket3/", rule2.getParameters().get("DataPath"));
        assertEquals("exampleHash3", ((StringBasedCondition) rule2.getCondition()).getOperands().get(0).getOperand());

        DQRule rule3 = ruleList.get(3);
        assertEquals("FileMatch", rule3.getRuleType());
        assertEquals("exampleHash4", ((StringBasedCondition) rule3.getCondition()).getOperands().get(0).getOperand());
    }

    @Test
    void test_fileMatchRuleParsing() throws Exception {
        String fileRules = "Rules = [ " +
                "FileMatch \"s3://sampom-bucket1/\" \"s3://sampom-bucket2/\"," +
                "FileMatch \"s3://sampom-bucket1/file1.json\" \"s3://sampom-bucket2/file2.json\"" +
                "]";
        DQRuleset dqRuleset = parser.parse(fileRules);
        List<DQRule> ruleList = dqRuleset.getRules();

        DQRule rule0 = ruleList.get(0);
        assertEquals("FileMatch", rule0.getRuleType());
        assertEquals("s3://sampom-bucket1/", rule0.getParameters().get("DataPath"));
        assertEquals("s3://sampom-bucket2/", rule0.getParameters().get("CompareDataPath"));

        DQRule rule1 = ruleList.get(1);
        assertEquals("FileMatch", rule0.getRuleType());
        assertEquals("s3://sampom-bucket1/file1.json", rule1.getParameters().get("DataPath"));
        assertEquals("s3://sampom-bucket2/file2.json", rule1.getParameters().get("CompareDataPath"));
    }

    @Test
    void test_toStringIgnoresSpacesOnlyThreshold() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("TargetColumn", "colA");
        Condition whitespaceCondition = new Condition("     ");
        DQRule dqRule = new DQRule("IsPrimaryKey", parameters, whitespaceCondition);
        String dqRuleAsString = dqRule.toString();
        assertEquals("IsPrimaryKey \"colA\"", dqRuleAsString);
    }

    @Test
    void test_toStringIgnoresTabsOnlyThreshold() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("TargetColumn", "colA");
        Condition whitespaceCondition = new Condition("\t\t");
        DQRule dqRule = new DQRule("IsPrimaryKey", parameters, whitespaceCondition);
        String dqRuleAsString = dqRule.toString();
        assertEquals("IsPrimaryKey \"colA\"", dqRuleAsString);
    }

    @Test
    void test_toStringIgnoresMixedWhitespaceThreshold() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("TargetColumn", "colA");
        Condition whitespaceCondition = new Condition("\t\t  \t \t \n   \t");
        DQRule dqRule = new DQRule("IsPrimaryKey", parameters, whitespaceCondition);
        String dqRuleAsString = dqRule.toString();
        assertEquals("IsPrimaryKey \"colA\"", dqRuleAsString);
    }

    @Test
    void test_setExpressionContainsRuleContainingRule() throws InvalidDataQualityRulesetException {
        DQRuleset dqRuleset = parser.parse(
            "Rules = [ ColumnValues \"col-A\" in [ \"ColumnValues in [ \\\"col-A\\\" ]\" ]]"
        );
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(StringBasedCondition.class, dqRule.getCondition().getClass());
        List<String> stringList = constructOperandsAsStringList(dqRule);
        assertEquals(
            Collections.singletonList("ColumnValues in [ \"col-A\" ]"), stringList);
    }

    @Test
    void test_setExpressionContainsDates() throws InvalidDataQualityRulesetException {
        DQRuleset dqRuleset = parser.parse(
            "Rules = [ ColumnValues \"col-A\" in [ now(), (now() - 4 days), \"2023-01-01\", \"2023-02-01\"]]"
        );
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(DateBasedCondition.class, dqRule.getCondition().getClass());
        assertEquals(
            Arrays.asList("now()", "(now() - 4 days)", "\"2023-01-01\"", "\"2023-02-01\""),
            ((DateBasedCondition) dqRule.getCondition()).getOperands().stream()
                .map(DateExpression::getFormattedExpression)
                .collect(Collectors.toList())
        );
    }

    @Test
    void test_setExpressionContainsItemContainingEscapedQuotes() throws InvalidDataQualityRulesetException {
        DQRuleset dqRuleset = parser.parse("Rules = [ ColumnValues \"col-A\" in [\"a\\\"b\", \"c\", \"d\\\"e\"]]");
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(StringBasedCondition.class, dqRule.getCondition().getClass());
        List<String> stringList = constructOperandsAsStringList(dqRule);
        assertEquals(Arrays.asList("a\"b", "c", "d\"e"), stringList);
    }

    @Test
    void test_setExpressionContainsItemContainingCommas() throws InvalidDataQualityRulesetException {
        DQRuleset dqRuleset = parser.parse("Rules = [ ColumnValues \"col-A\" in [\"a,,b\", \"c\", \"d,,,e\"]]");
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(StringBasedCondition.class, dqRule.getCondition().getClass());
        List<String> stringList = constructOperandsAsStringList(dqRule);
        assertEquals(Arrays.asList("a,,b", "c", "d,,,e"), stringList);
    }

    @Test
    void test_serializationDeserializationWithExpressionFieldSet()
        throws InvalidDataQualityRulesetException, IOException, ClassNotFoundException {
        DQRuleset dqRuleset = parser.parse("Rules = [ ColumnValues \"col-A\" in [\"A\", \"B\"]]");
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(StringBasedCondition.class, dqRule.getCondition().getClass());
        List<String> stringList = constructOperandsAsStringList(dqRule);
        assertEquals(Arrays.asList("A", "B"), stringList);
        byte[] serialized = serialize(dqRule);
        DQRule deserialized = deserialize(serialized, DQRule.class);
        assertEquals(dqRule.toString(), deserialized.toString());
        assertEquals(StringBasedCondition.class, deserialized.getCondition().getClass());
    }

    private static List<String> constructOperandsAsStringList(DQRule dqRule) {
        List<StringOperand> stringOperandsList = ((StringBasedCondition) dqRule.getCondition()).getOperands();
        List<String> stringList = stringOperandsList.stream()
            .map(StringOperand::getOperand)
            .collect(Collectors.toList());
        return stringList;
    }

    @Test
    void test_serializationDeserializationWithNumericExpression()
        throws InvalidDataQualityRulesetException, IOException, ClassNotFoundException {
        DQRuleset dqRuleset = parser.parse("Rules = [ Completeness \"colA\" between 0.2 and 0.8]]");
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(NumberBasedCondition.class, dqRule.getCondition().getClass());
        assertTrue(((NumberBasedCondition) dqRule.getCondition()).evaluate(0.4, dqRule, testEvaluator));
        byte[] serialized = serialize(dqRule);
        DQRule deserialized = deserialize(serialized, DQRule.class);
        assertEquals(dqRule.toString(), deserialized.toString());
        assertEquals(NumberBasedCondition.class, deserialized.getCondition().getClass());
        assertFalse(((NumberBasedCondition) deserialized.getCondition()).evaluate(0.9, dqRule, testEvaluator));
    }

    @Test
    void test_compositeRulesAreReparseable() throws InvalidDataQualityRulesetException {
        DQRuleset dqRuleset = parser.parse("Rules = [ (IsComplete \"colA\") and (IsUnique \"colA\")]");
        String rulesetString = dqRuleset.toString();
        DQRuleset reparsed = parser.parse(rulesetString);
        String reStringed = reparsed.toString();
        assertEquals(reparsed, dqRuleset);
        assertEquals(reStringed, rulesetString);
    }

    @Test
    void test_constructorWithOriginalParameterMap() {
        String ruleType = "IsComplete";
        String columnKey = "TargetColumn";
        String column = "colA";
        String emptyCondition = "";

        Map<String, String> parameters = new HashMap<>();
        parameters.put(columnKey, column);

        Condition condition = new Condition(emptyCondition);
        Condition thresholdCondition = new Condition(emptyCondition);

        DQRuleLogicalOperator operator = DQRuleLogicalOperator.AND;
        List<DQRule> nestedRules = new ArrayList<>();

        String whereClause = null;

        DQRule rule = new DQRule(ruleType, parameters, condition, thresholdCondition, operator, nestedRules, whereClause);

        assertEquals(ruleType, rule.getRuleType());

        assertTrue(rule.getParameters().containsKey(columnKey));
        assertEquals(column, rule.getParameters().get(columnKey));
        assertTrue(rule.getParameterValueMap().containsKey(columnKey));
        assertEquals(column, rule.getParameterValueMap().get(columnKey).getValue());
        assertTrue(rule.getParameterValueMap().get(columnKey).getConnectorWord().isEmpty());
        assertTrue(rule.getParameterValueMap().get(columnKey).isQuoted());
        assertTrue(rule.getCondition().getConditionAsString().isEmpty());
        assertTrue(rule.getThresholdCondition().getConditionAsString().isEmpty());
        assertEquals(operator, rule.getOperator());
        assertTrue(rule.getNestedRules().isEmpty());
    }

    @Test
    void test_parametersWithoutQuotesAreParsed() throws InvalidDataQualityRulesetException {
        String colA = "colA";
        String colB = "col\\\"B";
        String colC = "col C";

        String allCols = "AllColumns";

        String rule1 = String.format("IsPrimaryKey %s \"%s\" \"%s\"", colA, colB, colC);
        String rule2 = String.format("ColumnValues %s between 1 and 10", colA);

        String analyzer1 = String.format("Completeness \"%s\"", colC);
        String analyzer2 = String.format("AllStatistics %s", allCols);

        String ruleset = String.format(
            "Rules = [ %s, %s ] Analyzers = [ %s, %s ]", rule1, rule2, analyzer1, analyzer2);

        DQRuleset dqRuleset = parser.parse(ruleset);

        DQRule parsedRule1 = dqRuleset.getRules().get(0);
        DQRule parsedRule2 = dqRuleset.getRules().get(1);

        DQAnalyzer parsedAnalyzer1 = dqRuleset.getAnalyzers().get(0);
        DQAnalyzer parsedAnalyzer2 = dqRuleset.getAnalyzers().get(1);

        assertTrue(Stream.of(colA, colB, colC).allMatch(c -> parsedRule1.getParameters().containsValue(c)));
        assertTrue(Stream.of(colA).allMatch(c -> parsedRule2.getParameters().containsValue(c)));

        assertTrue(Stream.of(colC).allMatch(c -> parsedAnalyzer1.getParameters().containsValue(c)));
        assertTrue(Stream.of(allCols).allMatch(c -> parsedAnalyzer2.getParameters().containsValue(c)));
    }

    @Test
    public void test_equalsAndHashCode() throws InvalidDataQualityRulesetException {
        String rule = "IsPrimaryKey \"colA\" \"colB\"";
        String ruleset = String.format("Rules = [ %s ]", rule);

        DQRuleset dqRuleset1 = parser.parse(ruleset);
        DQRuleset dqRuleset2 = parser.parse(ruleset);

        assertNotSame(dqRuleset1, dqRuleset2);
        assertEquals(dqRuleset1, dqRuleset2);
        assertEquals(dqRuleset1.hashCode(), dqRuleset2.hashCode());
    }

    @Test
    public void test_whereClause() throws InvalidDataQualityRulesetException {
        String rule = "IsComplete \"colA\" where \"colB is NOT NULL\"";
        String ruleset = String.format("Rules = [ %s ]", rule);

        DQRuleset dqRuleset1 = parser.parse(ruleset);
        DQRuleset dqRuleset2 = parser.parse(ruleset);

        assertNotSame(dqRuleset1, dqRuleset2);
        assertEquals(dqRuleset1, dqRuleset2);
        assertEquals(dqRuleset1.hashCode(), dqRuleset2.hashCode());
    }

    @Test
    public void test_whereClauseWithThreshold() throws InvalidDataQualityRulesetException {
        String rule = "ColumnValues \"colA\" in [10,20] where \"colB is NOT NULL\" with threshold > 0.5";
        String ruleset = String.format("Rules = [ %s ]", rule);

        DQRuleset dqRuleset1 = parser.parse(ruleset);

        assertEquals(dqRuleset1.getRules().get(0).toString(), rule);
    }

    @Test
    void test_whereClauseRuleToStringFromRule() throws InvalidDataQualityRulesetException {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("TargetColumn", "colA");
        DQRule dqRule = new DQRule("IsComplete", parameters, new Condition(""), null,
                DQRuleLogicalOperator.AND, null, "colB is NOT NULL");
        String ruleString = "IsComplete \"colA\" where \"colB is NOT NULL\"";
        assertEquals(dqRule.toString(), ruleString);
        assertEquals(dqRule.getWhereClause(), "colB is NOT NULL");
    }

    @Test
    void test_whereClauseRuleToStringFromRuleWithThreshold() throws InvalidDataQualityRulesetException {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("TargetColumn", "colA");
        DQRule dqRule = new DQRule("ColumnValues", parameters, new Condition("in [10,20]"), new Condition("> 0.5"),
                DQRuleLogicalOperator.AND, null, "colB is NOT NULL");
        String ruleString = "ColumnValues \"colA\" in [10,20] where \"colB is NOT NULL\" with threshold > 0.5";
        assertEquals(dqRule.toString(), ruleString);
        assertEquals(dqRule.getWhereClause(), "colB is NOT NULL");
    }

    @Test
    void test_whereClauseRuleToStringFromRuleset() throws InvalidDataQualityRulesetException {
        String ruleString = "IsComplete \"colA\" where \"colB is NOT NULL\"";
        String ruleset = String.format("Rules = [ %s ]", ruleString);
        DQRuleset dqRuleset = parser.parse(ruleset);
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(dqRule.toString(), ruleString);
        assertEquals(dqRule.getWhereClause(), "colB is NOT NULL");
    }

    @Test
    void test_whereClauseNeedsQuotedSQLStatement() {
        String rule = "IsComplete \"colA\" where \"\"";
        String ruleset = String.format("Rules = [ %s ]", rule);
        assertThrows(InvalidDataQualityRulesetException.class, () -> parser.parse(ruleset));
    }

    @Test
    void test_whereClauseCannotBeEmpty() {
        String rule = "IsComplete \"colA\" where \"\"";
        String ruleset = String.format("Rules = [ %s ]", rule);
        assertThrows(InvalidDataQualityRulesetException.class, () -> parser.parse(ruleset));
    }

    @Test
    void test_constructorWithWhereClause() {
        String ruleType = "IsComplete";
        String columnKey = "TargetColumn";
        String column = "colA";
        String emptyCondition = "";
        String whereClause = "\"colB is NOT NULL\"";

        Map<String, String> parameters = new HashMap<>();
        parameters.put(columnKey, column);

        Condition condition = new Condition(emptyCondition);
        Condition thresholdCondition = new Condition(emptyCondition);

        DQRuleLogicalOperator operator = DQRuleLogicalOperator.AND;
        List<DQRule> nestedRules = new ArrayList<>();

        DQRule rule = new DQRule(ruleType, parameters, condition, thresholdCondition, operator, nestedRules, whereClause);
        assertEquals(ruleType, rule.getRuleType());

        assertTrue(rule.getParameters().containsKey(columnKey));
        assertEquals(column, rule.getParameters().get(columnKey));
        assertTrue(rule.getParameterValueMap().containsKey(columnKey));
        assertEquals(column, rule.getParameterValueMap().get(columnKey).getValue());
        assertTrue(rule.getParameterValueMap().get(columnKey).getConnectorWord().isEmpty());
        assertTrue(rule.getParameterValueMap().get(columnKey).isQuoted());
        assertTrue(rule.getCondition().getConditionAsString().isEmpty());
        assertTrue(rule.getThresholdCondition().getConditionAsString().isEmpty());
        assertEquals(operator, rule.getOperator());
        assertTrue(rule.getNestedRules().isEmpty());
        assertEquals(rule.getWhereClause(), whereClause);
    }

    @Test
    void test_constructorWithParametersAndCondition() {
        String ruleType = "IsComplete";
        String columnKey = "TargetColumn";
        String column = "colA";
        String emptyCondition = "";

        Map<String, String> parameters = new HashMap<>();
        parameters.put(columnKey, column);

        Condition condition = new Condition(emptyCondition);
        Condition thresholdCondition = new Condition(emptyCondition);

        DQRule rule = new DQRule(ruleType, parameters, condition, thresholdCondition);
        assertEquals(ruleType, rule.getRuleType());

        assertTrue(rule.getParameters().containsKey(columnKey));
        assertEquals(column, rule.getParameters().get(columnKey));
        assertTrue(rule.getParameterValueMap().containsKey(columnKey));
        assertEquals(column, rule.getParameterValueMap().get(columnKey).getValue());
        assertTrue(rule.getParameterValueMap().get(columnKey).getConnectorWord().isEmpty());
        assertTrue(rule.getParameterValueMap().get(columnKey).isQuoted());
        assertTrue(rule.getCondition().getConditionAsString().isEmpty());
        assertTrue(rule.getThresholdCondition().getConditionAsString().isEmpty());
    }

    @Test
    void test_modifyNestedRules() throws InvalidDataQualityRulesetException {
        String rule1 = "IsComplete \"name\"";
        String rule2 = "IsUnique \"name\"";
        String rule3 = "IsPrimaryKey \"name\"";
        String ruleset = String.format("Rules = [" +
                "(%s) AND (%s)," +
                "%s ]", rule1, rule2, rule3);
        DQRuleset dqRuleset = parser.parse(ruleset);

        DQRule composite = dqRuleset.getRules().get(0);

        // Copy the list's elements into a new list, without copying the list itself
        List<DQRule> nested = new ArrayList<>(composite.getNestedRules());
        nested.add(dqRuleset.getRules().get(1)); // IsComplete AND IsUnique AND IsPrimaryKey

        DQRule modified = composite.withNestedRules(nested);

        // The original rule hasn't been modified
        assertEquals(composite.toString(), "(IsComplete \"name\") AND (IsUnique \"name\")");

        // The modified rule includes all subrules
        assertEquals(modified.toString(), "(IsComplete \"name\") AND (IsUnique \"name\") AND (IsPrimaryKey \"name\")");
        assertEquals(modified.getNestedRules().size(), 3);
    }

    @Test
    void test_withCondition() throws InvalidDataQualityRulesetException {
        DQRuleset ruleset = parser.parse("Rules = [RowCount > 20, RowCount > 10 + 10]");

        DQRule simple = ruleset.getRules().get(0);
        DQRule dynamic = ruleset.getRules().get(1);

        Condition simplified = simple.getCondition();
        assertEquals(simplified.getFormattedCondition(), "> 20");

        DQRule modified = dynamic.withCondition(simplified);

        // The original rule hasn't been modified
        assertEquals(dynamic.toString(), "RowCount > 10 + 10");

        // The modified rule uses the simplified condition
        assertEquals(modified.toString(), "RowCount > 20");
    }

    @Disabled
    void test_nullParametersAreCorrectlyHandled() {
        Map<String, String> parameters = null;
        Condition threshold = new Condition("=100");
        DQRule dqRule = new DQRule("JobDuration", parameters, threshold);
        String dqRuleAsString = dqRule.toString();
        assertEquals("JobDuration = 100", dqRuleAsString);
    }

    @Disabled
    void test_nullNestedRulesAreCorrectlyHandled() {
        Map<String, String> parameters = null;
        Condition threshold = new Condition("=100");
        DQRule dqRule = new DQRule("JobDuration", parameters, threshold);
        String dqRuleAsString = dqRule.toString();
        assertEquals("JobDuration = 100", dqRuleAsString);
    }

    private <T extends Serializable> byte[] serialize(T obj) throws IOException
    {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(stream);
        objectStream.writeObject(obj);
        objectStream.close();
        return stream.toByteArray();
    }

    private <T extends Serializable> T deserialize(byte[] b, Class<T> cls) throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream stream = new ByteArrayInputStream(b);
        ObjectInputStream objectStream = new ObjectInputStream(stream);
        Object o = objectStream.readObject();
        return cls.cast(o);
    }

    private String removeQuotes(String quotedString) {
        if (quotedString.startsWith("\"") && quotedString.endsWith("\"")) {
            quotedString = quotedString.substring(1);
            quotedString = quotedString.substring(0, quotedString.length() - 1);
        }
        return quotedString;
    }
}
