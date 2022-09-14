/*
 * DQRuleset.java
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.glue.ml.dataquality.dqdl.util.StringUtils.isNotBlank;

@AllArgsConstructor
@Getter
public class DQRuleset {
    private final Map<String, String> metadata;
    private final String primarySourceName;
    private final List<String> additionalSourcesNames;
    private final List<DQRule> rules;

    private static final String LINE_SEP = System.lineSeparator();

    public DQRuleset(final List<DQRule> rules) {
        this.metadata = new HashMap<>();
        this.primarySourceName = null;
        this.additionalSourcesNames = new ArrayList<>();
        this.rules = rules;
    }

    @Override
    public String toString() {
        String metadataStr = "";
        if (metadata != null && metadata.size() > 0) {
            metadataStr = "Metadata = {" + LINE_SEP +
                metadata.keySet().stream()
                    .map(k -> "    \"" + k + "\": \"" + metadata.get(k) + "\"")
                    .collect(Collectors.joining("," + LINE_SEP)) +
                LINE_SEP + "}";
        }

        String sourcesStr = "";
        if (isNotBlank(primarySourceName) || (additionalSourcesNames != null && additionalSourcesNames.size() > 0)) {
            String additionalSourcesStr = "";
            if (additionalSourcesNames != null && additionalSourcesNames.size() > 0) {
                additionalSourcesStr = "    \"AdditionalSources\": [ " +
                    additionalSourcesNames.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(", ")) +
                    " ]" + LINE_SEP;
            }

            String primarySourceStr = "";
            if (isNotBlank(primarySourceName)) {
                primarySourceStr = "    \"Primary\": " + "\"" + primarySourceName + "\"";
                if (isNotBlank(additionalSourcesStr)) {
                     primarySourceStr += ",";
                }
                primarySourceStr += LINE_SEP;
            }

            sourcesStr = "Sources = {" + LINE_SEP +
                primarySourceStr + additionalSourcesStr +
                "}";
        }

        String rulesStr = "Rules = [" + LINE_SEP +
            rules.stream()
                .map(i -> "    " + i)
                .collect(Collectors.joining("," + LINE_SEP)) +
            LINE_SEP + "]";

        StringBuilder sb = new StringBuilder();

        if (!metadataStr.isEmpty()) {
            sb.append(metadataStr).append(LINE_SEP).append(LINE_SEP);
        }

        if (!sourcesStr.isEmpty()) {
            sb.append(sourcesStr).append(LINE_SEP).append(LINE_SEP);
        }

        sb.append(rulesStr);

        return sb.toString();
    }
}
