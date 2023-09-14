/*
 * DQRuleset.java
 *
 * Copyright (c) 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package com.amazonaws.glue.ml.dataquality.dqdl.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.glue.ml.dataquality.dqdl.util.StringUtils.isNotBlank;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class DQRuleset {
    private final Map<String, String> metadata;
    private final String primarySourceName;
    private final List<String> additionalDataSourcesNames;
    private final List<DQRule> rules;
    private final List<DQMonitor> monitors;

    private static final String LINE_SEP = System.lineSeparator();

    public DQRuleset(final List<DQRule> rules) {
        this(rules, new ArrayList<>());
    }

    public DQRuleset(final List<DQRule> rules, final List<DQMonitor> monitors) {
        this.metadata = new HashMap<>();
        this.primarySourceName = null;
        this.additionalDataSourcesNames = new ArrayList<>();
        this.rules = rules;
        this.monitors = monitors;
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
        if (isNotBlank(primarySourceName) ||
            (additionalDataSourcesNames != null && additionalDataSourcesNames.size() > 0)) {
            String additionalDataSourcesStr = "";
            if (additionalDataSourcesNames != null && additionalDataSourcesNames.size() > 0) {
                additionalDataSourcesStr = "    \"AdditionalDataSources\": [ " +
                    additionalDataSourcesNames.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(", ")) +
                    " ]" + LINE_SEP;
            }

            String primarySourceStr = "";
            if (isNotBlank(primarySourceName)) {
                primarySourceStr = "    \"Primary\": " + "\"" + primarySourceName + "\"";
                if (isNotBlank(additionalDataSourcesStr)) {
                    primarySourceStr += ",";
                }
                primarySourceStr += LINE_SEP;
            }

            sourcesStr = "DataSources = {" + LINE_SEP +
                primarySourceStr + additionalDataSourcesStr +
                "}";
        }

        String rulesStr = "Rules = [" + LINE_SEP +
            rules.stream()
                .map(i -> "    " + i)
                .collect(Collectors.joining("," + LINE_SEP)) +
            LINE_SEP + "]";

        String monitorsStr = "";
        if (!monitors.isEmpty()) {
            monitorsStr = "Monitors = [" + LINE_SEP +
                monitors.stream()
                    .map(i -> "    " + i)
                    .collect(Collectors.joining("," + LINE_SEP)) +
                LINE_SEP + "]";
        }
        StringBuilder sb = new StringBuilder();

        if (!metadataStr.isEmpty()) {
            sb.append(metadataStr).append(LINE_SEP).append(LINE_SEP);
        }

        if (!sourcesStr.isEmpty()) {
            sb.append(sourcesStr).append(LINE_SEP).append(LINE_SEP);
        }

        sb.append(rulesStr);

        if (!monitorsStr.isEmpty()) {
            sb.append(LINE_SEP).append(LINE_SEP).append(monitorsStr);
        }

        return sb.toString();
    }
}
