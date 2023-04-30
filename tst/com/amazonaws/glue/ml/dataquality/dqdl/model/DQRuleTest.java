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
import com.amazonaws.glue.ml.dataquality.dqdl.model.expression.DoubleNumericExpression;
import com.amazonaws.glue.ml.dataquality.dqdl.model.expression.StringSetExpression;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/*
 * The purpose of this test is to ensure that parsing a rule and
 * converting it back to a string yields the original raw rule.
 */
class DQRuleTest {
    DQDLParser parser = new DQDLParser();
    com.amazonaws.glue.ml.dataquality.dqdl.parser.updated.DQDLParser updatedParser =
        new com.amazonaws.glue.ml.dataquality.dqdl.parser.updated.DQDLParser();

    @ParameterizedTest
    @MethodSource("provideRawRules")
    void test_ruleParsingAndGenerating(String rule) {
        // We will remove the old parser anyway
        if (rule.contains("ColumnDataType") && rule.contains("with threshold")) return;
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
    void test_ruleParsingAndGeneratingWithUpdatedParser(String rule) {
        try {
            com.amazonaws.glue.ml.dataquality.dqdl.model.updated.DQRuleset dqRuleset =
                updatedParser.parse(String.format("Rules = [ %s ]", rule));
            assertEquals(1, dqRuleset.getRules().size());
            com.amazonaws.glue.ml.dataquality.dqdl.model.updated.DQRule dqRule =
                dqRuleset.getRules().get(0);
            String dqRuleAsString = dqRule.toString();
            assertEquals(rule, dqRuleAsString);
        } catch (InvalidDataQualityRulesetException e) {
            fail(e.getMessage());
        }
    }

    private static Stream<Arguments> provideRawRules() {
        return Stream.of(
            // Arguments.of("JobStatus = \"SUCCEEDED\""),
            // Arguments.of("JobStatus in [\"SUCCEEDED\",\"READY\"]"),
            // Arguments.of("JobDuration between 10 and 1000"),
            // Arguments.of("JobDuration between -10 and 1000"),
            // Arguments.of("FileCount between 10 and 100"),
            // Arguments.of("FileCount between -10000 and -1000"),
            Arguments.of("IsPrimaryKey \"colA\""),
            Arguments.of("IsPrimaryKey \"colA\" \"colB\""),
            Arguments.of("IsPrimaryKey \"colA\" \"colB\" \"colC\""),
            Arguments.of("RowCount = 100"),
            Arguments.of("RowCount = -100"),
            Arguments.of("RowCountMatch \"reference\" = 1.0"),
            Arguments.of("RowCountMatch \"reference\" >= 0.95"),
            Arguments.of("RowCountMatch \"reference\" between 0.8 and 0.98"),
            Arguments.of("Completeness \"col_1\" between 0.5 and 0.8"),
            Arguments.of("IsComplete \"col_1\""),
            Arguments.of("Completeness \"col_1\" between -0.5 and -0.4"),
            Arguments.of("ColumnDataType \"col_1\" = \"String\""),
            Arguments.of("ColumnDataType \"col_1\" = \"String\" with threshold between 0.4 and 0.8"),
            Arguments.of("ColumnDataType \"col_1\" in [\"Date\",\"String\"]"),
            Arguments.of("ColumnDataType \"col_1\" in [\"Date\",\"String\"] with threshold > 0.9"),
            Arguments.of("ColumnNamesMatchPattern \"aws_.*_[a-zA-Z0-9]+\""),
            Arguments.of("ColumnExists \"load_dt\""),
            Arguments.of("ColumnCount >= 100"),
            Arguments.of("ColumnCount > -100.123456"),
            Arguments.of("ColumnCorrelation \"col_1\" \"col_2\" between 0.4 and 0.8"),
            Arguments.of("ColumnCorrelation \"col_1\" \"col_2\" between -0.44444 and 0.888888"),
            Arguments.of("Uniqueness \"col_1\" between 0.1 and 0.2"),
            Arguments.of("IsUnique \"col_1\""),
            Arguments.of("Uniqueness \"col_1\" between -0.00000001 and 0.00000000000002"),
            Arguments.of("ColumnValues \"col_1\" between \"2022-06-01\" and \"2022-06-30\""),
            Arguments.of("ColumnValues \"load_dt\" > (now() - 1 days)"),
            Arguments.of("ColumnValues \"order-id\" in [1,2,3,4]"),
            Arguments.of("ColumnValues \"order-id\" in [\"1\",\"2\",\"3\",\"4\"]"),
            Arguments.of("Sum \"col_A-B.C\" > 100.0"),
            Arguments.of("Sum \"col_A-B.C\" > -100.0"),
            Arguments.of("Mean \"col_A-B.CD\" between 10 and 20"),
            Arguments.of("Mean \"col_A-B.CD\" between -20 and -10"),
            Arguments.of("StandardDeviation \"col_A-B.CD\" <= 10.0"),
            Arguments.of("StandardDeviation \"col_A-B.CD\" <= -10000.0"),
            Arguments.of("Entropy \"col_A-B.CD\" <= 10.0"),
            Arguments.of("Entropy \"col_A-B.CD\" between 10 and 30"),
            Arguments.of("DistinctValuesCount \"col_A-B.CD\" > 1000"),
            Arguments.of("DistinctValuesCount \"col_A-B.CD\" between 10 and 30"),
            Arguments.of("UniqueValueRatio \"col_A-B.CD\" < 0.5"),
            Arguments.of("UniqueValueRatio \"col_A-B.CD\" between 0.1 and 0.5"),
            Arguments.of("ColumnLength \"col_A-B.CD\" < 10"),
            Arguments.of("ColumnLength \"col_A-B.CD\" >= 100"),
            Arguments.of("ColumnValues \"col-A\" matches \"[a-zA-Z0-9]*\""),
            Arguments.of("ColumnValues \"col-A\" >= now()"),
            Arguments.of("ColumnValues \"col-A\" between (now() - 3 hours) and now()"),
            Arguments.of("ColumnValues \"col-A\" between now() and (now() + 3 hours)"),
            Arguments.of("ColumnValues \"col-A\" < (now() + 4 days)"),
            Arguments.of("ColumnValues \"col-A\" = (now() - 3 hours)"),
            Arguments.of("ColumnValues \"col-A\" in [now(),(now() - 3 hours),now(),(now() + 4 days)]"),
            Arguments.of("ColumnValues \"col-A\" between (now() - 3 hours) and (now() + 14 days)"),
            Arguments.of("ColumnValues \"col-A\" matches \"[a-z]*\" with threshold <= 0.4"),
            Arguments.of("ColumnValues \"col-A\" in [\"A\",\"B\"] with threshold <= 0.4"),
            Arguments.of("ColumnValues \"col-A\" in [1,2,3] with threshold > 0.98"),
            Arguments.of("ColumnValues \"col-A\" = \"A\" with threshold > 0.98"),
            Arguments.of("DataFreshness \"col-A\" <= 3 days"),
            Arguments.of("DataFreshness \"col-A\" > 30 hours"),
            Arguments.of("DataFreshness \"col-A\" between 2 days and 4 days"),
            Arguments.of("ReferentialIntegrity \"col-A\" \"reference.col-A1\" between 0.4 and 0.6"),
            Arguments.of("ReferentialIntegrity \"col-A\" \"reference.col-A1\" > 0.98"),
            Arguments.of("ReferentialIntegrity \"col-A\" \"reference.col-A1\" = 0.99"),
            Arguments.of("ReferentialIntegrity \"col-A,col-B\" \"reference.{col-A1,col-A2}\" = 0.99"),
            Arguments.of("DatasetMatch \"reference\" \"ID1,ID2\" = 0.99"),
            Arguments.of("DatasetMatch \"reference\" \"ID1,ID2\" \"colA,colB,colC\" = 0.99"),
            Arguments.of("DatasetMatch \"reference\" \"ID1->ID11,ID2->ID22\" \"colA->colAA\" between 0.4 and 0.8"),
            Arguments.of("AggregateMatch \"sum(col-A)\" \"sum(colB)\" > 0.9"),
            Arguments.of("AggregateMatch \"sum(col-A)\" \"sum(reference.colA)\" > 0.1"),
            Arguments.of("AggregateMatch \"avg(col-A)\" \"avg(reference.colA)\" between 0.8 and 0.9"),
            Arguments.of("AggregateMatch \"SUM(col-A)\" \"SUM(reference.colA)\" >= 0.95")
        );
    }

    @Test
    void test_toStringIgnoresSpacesOnlyThreshold() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("TargetColumn", "colA");
        String whitespaceThreshold = "     ";
        DQRule dqRule = new DQRule("IsPrimaryKey", parameters, whitespaceThreshold);
        String dqRuleAsString = dqRule.toString();
        assertEquals("IsPrimaryKey \"colA\"", dqRuleAsString);
    }

    @Test
    void test_toStringIgnoresTabsOnlyThreshold() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("TargetColumn", "colA");
        String whitespaceThreshold = "\t\t";
        DQRule dqRule = new DQRule("IsPrimaryKey", parameters, whitespaceThreshold);
        String dqRuleAsString = dqRule.toString();
        assertEquals("IsPrimaryKey \"colA\"", dqRuleAsString);
    }

    @Test
    void test_toStringIgnoresMixedWhitespaceThreshold() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("TargetColumn", "colA");
        String whitespaceThreshold = "\t\t  \t \t \n   \t";
        DQRule dqRule = new DQRule("IsPrimaryKey", parameters, whitespaceThreshold);
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
        assertEquals(StringSetExpression.class, dqRule.getExpression().getClass());
        assertEquals(
            Collections.singletonList("ColumnValues in [ \"col-A\" ]"),
            ((StringSetExpression) dqRule.getExpression()).getItems()
        );
    }

    @Test
    void test_setExpressionContainsDates() throws InvalidDataQualityRulesetException {
        DQRuleset dqRuleset = parser.parse(
            "Rules = [ ColumnValues \"col-A\" in [ now(), (now() - 4 days), \"2023-01-01\", \"2023-02-01\"]]"
        );
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(StringSetExpression.class, dqRule.getExpression().getClass());
        assertEquals(
            Arrays.asList("now()", "(now()-4days)", "2023-01-01", "2023-02-01"),
            ((StringSetExpression) dqRule.getExpression()).getItems()
        );
    }

    @Test
    void test_setExpressionContainsItemContainingEscapedQuotes() throws InvalidDataQualityRulesetException {
        DQRuleset dqRuleset = parser.parse("Rules = [ ColumnValues \"col-A\" in [\"a\\\"b\", \"c\", \"d\\\"e\"]]");
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(StringSetExpression.class, dqRule.getExpression().getClass());
        assertEquals(Arrays.asList("a\"b", "c", "d\"e"), ((StringSetExpression) dqRule.getExpression()).getItems());
    }

    @Test
    void test_setExpressionContainsItemContainingCommas() throws InvalidDataQualityRulesetException {
        DQRuleset dqRuleset = parser.parse("Rules = [ ColumnValues \"col-A\" in [\"a,,b\", \"c\", \"d,,,e\"]]");
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(StringSetExpression.class, dqRule.getExpression().getClass());
        assertEquals(Arrays.asList("a,,b", "c", "d,,,e"), ((StringSetExpression) dqRule.getExpression()).getItems());
    }

    @Test
    void test_serializationDeserializationWithExpressionFieldSet()
        throws InvalidDataQualityRulesetException, IOException, ClassNotFoundException {
        DQRuleset dqRuleset = parser.parse("Rules = [ ColumnValues \"col-A\" in [\"A\", \"B\"]]");
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(StringSetExpression.class, dqRule.getExpression().getClass());
        assertEquals(Arrays.asList("A", "B"), ((StringSetExpression) dqRule.getExpression()).getItems());
        byte[] serialized = serialize(dqRule);
        DQRule deserialized = deserialize(serialized, DQRule.class);
        assertEquals(dqRule.toString(), deserialized.toString());
        assertEquals(StringSetExpression.class, deserialized.getExpression().getClass());
    }

    @Test
    void test_serializationDeserializationWithNumericExpression()
        throws InvalidDataQualityRulesetException, IOException, ClassNotFoundException {
        DQRuleset dqRuleset = parser.parse("Rules = [ Completeness \"colA\" between 0.2 and 0.8]]");
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(DoubleNumericExpression.class, dqRule.getExpression().getClass());
        assertTrue(((DoubleNumericExpression) dqRule.getExpression()).evaluate(0.4));
        byte[] serialized = serialize(dqRule);
        DQRule deserialized = deserialize(serialized, DQRule.class);
        assertEquals(dqRule.toString(), deserialized.toString());
        assertEquals(DoubleNumericExpression.class, deserialized.getExpression().getClass());
        assertFalse(((DoubleNumericExpression) deserialized.getExpression()).evaluate(0.9));
    }

    @Test
    void test_compositeRulesAreReparseable() throws InvalidDataQualityRulesetException {
        DQRuleset dqRuleset = parser.parse("Rules = [ (IsComplete \"colA\") and (IsUnique \"colA\")]");
        String rulesetString = dqRuleset.toString();
        DQRuleset reparsed = parser.parse(rulesetString);
        String reStringed = reparsed.toString();
        assertTrue(dqRuleset.equals(reparsed));
        assertEquals(reStringed, rulesetString);
    }

    @Disabled
    void test_nullParametersAreCorrectlyHandled() {
        Map<String, String> parameters = null;
        String threshold = "=100";
        DQRule dqRule = new DQRule("JobDuration", parameters, threshold);
        String dqRuleAsString = dqRule.toString();
        assertEquals("JobDuration = 100", dqRuleAsString);
    }

    @Disabled
    void test_nullNestedRulesAreCorrectlyHandled() {
        Map<String, String> parameters = null;
        String threshold = "=100";
        List<DQRule> nestedRules = null;
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
}
