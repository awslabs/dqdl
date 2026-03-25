package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string;

import lombok.AllArgsConstructor;

import com.amazonaws.glue.ml.dataquality.dqdl.DataQualityDefinitionLanguageParser;
import org.antlr.v4.runtime.ParserRuleContext;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;

@AllArgsConstructor
public class Labels implements Serializable {
    private Map<String, String> rulesetDefaultLabels;
    private Map<String, String> ruleLabels;

    private static final int MAX_LABEL_KEY_LENGTH = 128;
    private static final int MAX_LABEL_VALUE_LENGTH = 256;
    private static final int MAX_LABELS_PER_RULE = 10;

    public Labels() {
        this.rulesetDefaultLabels = new HashMap<>();
        this.ruleLabels = new HashMap<>();
    }

    private Map<String, String> getCombinedLabels() {
        Map<String, String> combined = new HashMap<>();
        if (rulesetDefaultLabels != null) combined.putAll(rulesetDefaultLabels);
        if (ruleLabels != null) combined.putAll(ruleLabels);
        return combined;
    }

    public void setRulesetDefaultLabels(Map<String, String> rulesetDefaultLabels) {
        this.rulesetDefaultLabels = rulesetDefaultLabels != null
                ? new HashMap<>(rulesetDefaultLabels)
                : new HashMap<>();
    }

    public void setRuleLabels(Map<String, String> ruleLabels) {
        this.ruleLabels = ruleLabels != null
                ? new HashMap<>(ruleLabels)
                : new HashMap<>();
    }

    public Map<String, String> getRulesetDefaultLabels() {
        return rulesetDefaultLabels;
    }

    public Map<String, String> getRuleLabels() {
        return ruleLabels;
    }

    public static Map<String, String> convertToStringMap(Labels labels) {
        if (labels == null) {
            return new HashMap<>();
        }
        return labels.getCombinedLabels();
    }

    private static String removeQuotes(String quotedString) {
        if (quotedString.startsWith("\"") && quotedString.endsWith("\"")) {
            quotedString = quotedString.substring(1);
            quotedString = quotedString.substring(0, quotedString.length() - 1);
        }
        return quotedString;
    }

    private static boolean validateLabel(String key, String value, List<String> errorMessages) {
        if (key.isEmpty() || value.isEmpty()) {
            errorMessages.add("Label key or value cannot be empty");
            return false;
        }

        if (key.length() > MAX_LABEL_KEY_LENGTH) {
            errorMessages.add(String.format(
                    "Label key has length %d exceeding the maximum length of 128 characters", key.length()
            ));
            return false;
        }

        if (value.length() > MAX_LABEL_VALUE_LENGTH) {
            errorMessages.add(String.format(
                    "Label value has length %d exceeding the maximum length of 256 characters", value.length()
            ));
            return false;
        }
        return true;
    }

    public static Map<String, String> parseLabels(
            Labels labels, ParserRuleContext context, Map<String, String> output, List<String> errorMessages
    ) {
        if (context == null) {
            return output;
        }

        List<DataQualityDefinitionLanguageParser.LabelContext> ctx;
        if (context instanceof DataQualityDefinitionLanguageParser.LabelsContext) {
            ctx = ((DataQualityDefinitionLanguageParser.LabelsContext) context).label();
        } else if (context instanceof DataQualityDefinitionLanguageParser.DefaultLabelsContext) {
            ctx = ((DataQualityDefinitionLanguageParser.DefaultLabelsContext) context).label();
        } else {
            throw new IllegalArgumentException("Unsupported context type");
        }

        if (ctx.isEmpty()) {
            errorMessages.add("Labels must have at least one label");
            return output;
        }

        int labelSize = (labels != null ? labels.getCombinedLabels().size() : 0) + ctx.size();
        if (labelSize > MAX_LABELS_PER_RULE) {
            errorMessages.add("Number of labels exceed maximum allowed (MAX: 10)");
            return output;
        }

        Set<String> seenKeys = new HashSet<>();

        for (DataQualityDefinitionLanguageParser.LabelContext labelCtx : ctx) {
            List<DataQualityDefinitionLanguageParser.QuotedStringContext> quotedStrings = labelCtx.quotedString();
            String key = removeQuotes(quotedStrings.get(0).getText());
            String value = removeQuotes(quotedStrings.get(1).getText());

            if (seenKeys.contains(key)) {
                errorMessages.add("Duplicate label key. Key must be unique.");
                return output;
            }

            if (validateLabel(key, value, errorMessages)) {
                seenKeys.add(key);
                output.put(key, value);
            }
        }
        return output;
    }
}
