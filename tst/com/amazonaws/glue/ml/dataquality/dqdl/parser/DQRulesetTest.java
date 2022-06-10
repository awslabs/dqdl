/*
 * DQRuleset.java
 *
 * Copyright (c) 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.parser;

import com.amazonaws.glue.ml.dataquality.dqdl.model.DQRuleset;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DQRulesetTest {

    DQDLParser dqdlParser = new DQDLParser();

    @Test
    public void test() {
        String dqdl = "rules { IsComplete \"col_1\", IsUnique \"col_2\", HasRowCount 100 }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(3, dqRuleset.getRules().size());
        assertTrue(dqRuleset.getRules().get(2).getExpectedRowCount().isPresent());
        assertEquals(100, dqRuleset.getRules().get(2).getExpectedRowCount().get());
    }
}
