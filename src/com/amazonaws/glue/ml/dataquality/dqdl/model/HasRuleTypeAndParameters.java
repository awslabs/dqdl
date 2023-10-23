/*
 * HasRuleTypeAndParameters.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All rights reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import java.util.Map;

public interface HasRuleTypeAndParameters {

    String getRuleType();

    Map<String, String> getParameters();
}
