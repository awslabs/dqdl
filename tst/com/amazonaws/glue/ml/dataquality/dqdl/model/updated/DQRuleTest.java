/*
 * DQRuleTest.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model.updated;

import com.amazonaws.glue.ml.dataquality.dqdl.exception.InvalidDataQualityRulesetException;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.Condition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.date.DateBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.date.DateExpression;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.number.NumberBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string.StringBasedCondition;
import com.amazonaws.glue.ml.dataquality.dqdl.model.expression.DoubleNumericExpression;
import com.amazonaws.glue.ml.dataquality.dqdl.model.expression.StringSetExpression;
import com.amazonaws.glue.ml.dataquality.dqdl.parser.updated.DQDLParser;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

// This is a copy of the original DQRuleTest.
// Once the original parser is no longer being used, we should remove all associated tests.
public class DQRuleTest {
    DQDLParser updatedParser = new DQDLParser();

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
            Arguments.of("Completeness \"col_1\" between 0.5 and 0.8"),
            Arguments.of("IsComplete \"col_1\""),
            Arguments.of("Completeness \"col_1\" between -0.5 and -0.4"),
            Arguments.of("ColumnDataType \"col_1\" = \"String\""),
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
            Arguments.of("DataSynchronization \"reference\" \"ID1,ID2\" \"colA,colB,colC\" = 0.99"),
            Arguments.of("DataSynchronization \"reference\" \"ID1->ID11,ID2->ID22\" \"colA->colAA\" between 0.4 and 0.8")
        );
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
        DQRuleset dqRuleset = updatedParser.parse(
            "Rules = [ ColumnValues \"col-A\" in [ \"ColumnValues in [ \\\"col-A\\\" ]\" ]]"
        );
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(StringBasedCondition.class, dqRule.getCondition().getClass());
        assertEquals(
            Collections.singletonList("ColumnValues in [ \"col-A\" ]"),
            ((StringBasedCondition) dqRule.getCondition()).getOperands()
        );
    }

    @Test
    void test_setExpressionContainsDates() throws InvalidDataQualityRulesetException {
        DQRuleset dqRuleset = updatedParser.parse(
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
        DQRuleset dqRuleset = updatedParser.parse("Rules = [ ColumnValues \"col-A\" in [\"a\\\"b\", \"c\", \"d\\\"e\"]]");
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(StringBasedCondition.class, dqRule.getCondition().getClass());
        assertEquals(Arrays.asList("a\"b", "c", "d\"e"), ((StringBasedCondition) dqRule.getCondition()).getOperands());
    }

    @Test
    void test_setExpressionContainsItemContainingCommas() throws InvalidDataQualityRulesetException {
        DQRuleset dqRuleset = updatedParser.parse("Rules = [ ColumnValues \"col-A\" in [\"a,,b\", \"c\", \"d,,,e\"]]");
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(StringBasedCondition.class, dqRule.getCondition().getClass());
        assertEquals(Arrays.asList("a,,b", "c", "d,,,e"), ((StringBasedCondition) dqRule.getCondition()).getOperands());
    }

    @Test
    void test_serializationDeserializationWithExpressionFieldSet()
        throws InvalidDataQualityRulesetException, IOException, ClassNotFoundException {
        DQRuleset dqRuleset = updatedParser.parse("Rules = [ ColumnValues \"col-A\" in [\"A\", \"B\"]]");
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(StringBasedCondition.class, dqRule.getCondition().getClass());
        assertEquals(Arrays.asList("A", "B"), ((StringBasedCondition) dqRule.getCondition()).getOperands());
        byte[] serialized = serialize(dqRule);
        DQRule deserialized = deserialize(serialized, DQRule.class);
        assertEquals(dqRule.toString(), deserialized.toString());
        assertEquals(StringBasedCondition.class, deserialized.getCondition().getClass());
    }

    @Test
    void test_serializationDeserializationWithNumericExpression()
        throws InvalidDataQualityRulesetException, IOException, ClassNotFoundException {
        DQRuleset dqRuleset = updatedParser.parse("Rules = [ Completeness \"colA\" between 0.2 and 0.8]]");
        assertEquals(1, dqRuleset.getRules().size());
        DQRule dqRule = dqRuleset.getRules().get(0);
        assertEquals(NumberBasedCondition.class, dqRule.getCondition().getClass());
//        assertTrue(((NumberBasedCondition) dqRule.getCondition()).evaluate(0.4));
        byte[] serialized = serialize(dqRule);
        DQRule deserialized = deserialize(serialized, DQRule.class);
        assertEquals(dqRule.toString(), deserialized.toString());
        assertEquals(NumberBasedCondition.class, deserialized.getCondition().getClass());
//        assertFalse(((DoubleNumericExpression) deserialized.getCondition()).evaluate(0.9));
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
