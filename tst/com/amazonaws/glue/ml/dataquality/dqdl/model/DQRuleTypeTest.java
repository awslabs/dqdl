/*
 * DQRuleTypeTest.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DQRuleTypeTest {
    @Test
    public void test_generateRuleTypesThrowsRuntimeException() {
        IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class,
            () -> DQRuleType.generateRuleTypes("foo/bar.json")
        );

        assertTrue(thrown.getMessage().contains("Failed to load rule types"));
    }
}
