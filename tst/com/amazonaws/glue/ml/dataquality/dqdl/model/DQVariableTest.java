/*
 * DQVariableTest.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class DQVariableTest {

    @Test
    void testConstructorAndGetters() {
        DQVariable<Integer> intVar = new DQVariable<>("age", DQVariable.VariableType.NUMBER, 30);
        assertEquals("age", intVar.getName());
        assertEquals(DQVariable.VariableType.NUMBER, intVar.getType());
        assertEquals(30, intVar.getValue());
    }

    @Test
    void testEqualsAndHashCode() {
        DQVariable<String> var1 = new DQVariable<>("name", DQVariable.VariableType.STRING, "John");
        DQVariable<String> var2 = new DQVariable<>("name", DQVariable.VariableType.STRING, "John");
        DQVariable<String> var3 = new DQVariable<>("name", DQVariable.VariableType.STRING, "Jane");

        assertEquals(var1, var2);
        assertNotEquals(var1, var3);
        assertEquals(var1.hashCode(), var2.hashCode());
    }

    @Test
    void testToStringForNumber() {
        DQVariable<Integer> intVar = new DQVariable<>("age", DQVariable.VariableType.NUMBER, 30);
        assertEquals("age = 30", intVar.toString());
    }

    @Test
    void testToStringForString() {
        DQVariable<String> stringVar = new DQVariable<>("name", DQVariable.VariableType.STRING, "John");
        assertEquals("name = \"John\"", stringVar.toString());
    }

    @Test
    void testToStringForDate() {
        LocalDate date = LocalDate.of(2023, 5, 15);
        DQVariable<LocalDate> dateVar = new DQVariable<>("birthdate", DQVariable.VariableType.DATE, date);
        assertEquals("birthdate = 2023-05-15", dateVar.toString());
    }

    @Test
    void testToStringForDuration() {
        Duration duration = Duration.ofHours(2);
        DQVariable<Duration> durationVar = new DQVariable<>("timeSpent", DQVariable.VariableType.DURATION, duration);
        assertEquals("timeSpent = PT2H", durationVar.toString());
    }

    @Test
    void testToStringForNumberArray() {
        List<Integer> numbers = Arrays.asList(1, 2, 3);
        DQVariable<List<Integer>> arrayVar = new DQVariable<>("numbers", DQVariable.VariableType.NUMBER_ARRAY, numbers);
        assertEquals("numbers = [1, 2, 3]", arrayVar.toString());
    }

    @Test
    void testToStringForStringArray() {
        List<String> names = Arrays.asList("John", "Jane", "Doe");
        DQVariable<List<String>> arrayVar = new DQVariable<>("names", DQVariable.VariableType.STRING_ARRAY, names);
        assertEquals("names = [John, Jane, Doe]", arrayVar.toString());
    }

    @Test
    void testToStringForNullValue() {
        DQVariable<String> nullVar = new DQVariable<>("nullValue", DQVariable.VariableType.STRING, null);
        assertEquals("nullValue = null", nullVar.toString());
    }
}
