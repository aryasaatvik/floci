package io.github.hectorvent.floci.services.eventbridge;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.hectorvent.floci.core.common.AwsException;

import java.util.ArrayList;
import java.util.List;

record WildcardPattern(List<String> literals) {

    static WildcardPattern parse(JsonNode operand) {
        if (!operand.isTextual()) {
            throw invalid("Wildcard operand must be a string.");
        }
        String pattern = operand.textValue();
        List<String> literals = new ArrayList<>();
        StringBuilder literal = new StringBuilder();
        for (int index = 0; index < pattern.length(); index++) {
            char character = pattern.charAt(index);
            if (character == '*') {
                if (index > 0 && literal.isEmpty()) {
                    throw invalid("Consecutive wildcards are unsupported.");
                }
                literals.add(literal.toString());
                literal.setLength(0);
                continue;
            }
            if (character == '\\') {
                index++;
                if (index == pattern.length()) {
                    throw invalid("Wildcard pattern ends with an incomplete escape.");
                }
                character = pattern.charAt(index);
                if (character != '*' && character != '\\') {
                    throw invalid("Wildcard patterns can only escape an asterisk or a backslash.");
                }
            }
            literal.append(character);
        }
        literals.add(literal.toString());
        return new WildcardPattern(literals);
    }

    boolean matches(String value) {
        String prefix = literals.getFirst();
        if (literals.size() == 1) {
            return value.equals(prefix);
        }
        String suffix = literals.getLast();
        if (!value.startsWith(prefix) || !value.endsWith(suffix)) {
            return false;
        }

        // Reserve the anchored suffix, then find the earliest nonoverlapping occurrence
        // of each intervening literal. This avoids regex backtracking across wildcards.
        int offset = prefix.length();
        int suffixStart = value.length() - suffix.length();
        if (offset > suffixStart) {
            return false;
        }
        for (int index = 1; index < literals.size() - 1; index++) {
            String literal = literals.get(index);
            int found = value.indexOf(literal, offset, suffixStart);
            if (found == -1) {
                return false;
            }
            offset = found + literal.length();
        }
        return true;
    }

    static AwsException invalid(String message) {
        return new AwsException("InvalidEventPatternException", message, 400);
    }
}
