package com.amazonaws.glue.ml.dataquality.dqdl.model.condition.string;

import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
public class Tag implements Serializable {
    private final String key;
    private final String value;

    public static Map<String, String> convertToStringMap(Map<String, Tag> tags) {
        if (tags == null) {
            return Collections.emptyMap();
        }
        return tags.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getValue().getKey(),
                        entry -> entry.getValue().getValue()
                ));
    }

    public String getKey() {
        return removeQuotes(this.key);
    }

    public String getValue() {
        return removeQuotes(this.value);
    }

    @Override
    public String toString() {
        return String.format(" with %s = %s", key, value);
    }

    private String removeQuotes(String quotedString) {
        if (quotedString.startsWith("\"") && quotedString.endsWith("\"")) {
            quotedString = quotedString.substring(1);
            quotedString = quotedString.substring(0, quotedString.length() - 1);
        }
        return quotedString;
    }
}
