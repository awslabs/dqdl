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

public class DQRulesetTest {

    DQDLParser dqdlParser = new DQDLParser();

    @Test
    public void test() {
        String dqdl = "rules { IsComplete, IsUnique }";
        DQRuleset dqRuleset = dqdlParser.parse(dqdl);
        System.out.println(dqRuleset);
        assertEquals(2, dqRuleset.getRules().size());
    }
}
