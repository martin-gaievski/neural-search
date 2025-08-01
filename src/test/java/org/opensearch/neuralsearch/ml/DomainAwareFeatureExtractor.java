/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Domain-aware feature extractor that provides industry-specific features
 * for hybrid search weight optimization
 */
public class DomainAwareFeatureExtractor extends QueryFeatureExtractor {

    public enum Domain {
        ECOMMERCE,
        QA_CONVERSATIONAL,
        MEDICAL_SCIENTIFIC,
        LEGAL,
        TECHNICAL,
        GENERAL
    }

    // E-commerce patterns
    private static final Pattern CURRENCY_PATTERN = Pattern.compile("[$€£¥₹]\\d+");
    private static final Pattern SIZE_PATTERN = Pattern.compile("\\b(XS|S|M|L|XL|XXL|\\d+GB|\\d+TB|\\d+MB)\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern SKU_PATTERN = Pattern.compile("\\b[A-Z0-9]{6,}\\b");

    // Q&A patterns
    private static final Pattern QUESTION_PATTERN = Pattern.compile(
        "^(what|how|why|when|where|who|which|can|should|would|could)\\b",
        Pattern.CASE_INSENSITIVE
    );

    // Medical patterns
    private static final Pattern MEDICAL_ACRONYM_PATTERN = Pattern.compile("\\b[A-Z]{2,5}\\b");
    private static final Pattern DOSAGE_PATTERN = Pattern.compile("\\b\\d+\\s?(mg|ml|mcg|iu|units?)\\b", Pattern.CASE_INSENSITIVE);

    // Legal patterns
    private static final Pattern CITATION_PATTERN = Pattern.compile("\\b\\d+\\s+[A-Z]\\.\\s*\\d+[a-z]?\\s+\\d+\\b");
    private static final Pattern SECTION_PATTERN = Pattern.compile("§\\s*\\d+|section\\s+\\d+", Pattern.CASE_INSENSITIVE);

    private final Domain domain;

    public DomainAwareFeatureExtractor(Domain domain) {
        this.domain = domain;
    }

    /**
     * Extract features based on the configured domain
     */
    @Override
    public Map<String, Double> extractFeatures(String queryText) {
        // Start with basic features
        Map<String, Double> features = super.extractFeatures(queryText);

        // Add domain-specific features
        switch (domain) {
            case ECOMMERCE:
                features.putAll(extractEcommerceFeatures(queryText));
                break;
            case QA_CONVERSATIONAL:
                features.putAll(extractQAFeatures(queryText));
                break;
            case MEDICAL_SCIENTIFIC:
                features.putAll(extractMedicalFeatures(queryText));
                break;
            case LEGAL:
                features.putAll(extractLegalFeatures(queryText));
                break;
            case TECHNICAL:
                features.putAll(extractTechnicalFeatures(queryText));
                break;
            default:
                // GENERAL uses only basic features
                break;
        }

        return features;
    }

    private Map<String, Double> extractEcommerceFeatures(String queryText) {
        Map<String, Double> features = new HashMap<>();

        features.put("has_currency", CURRENCY_PATTERN.matcher(queryText).find() ? 1.0 : 0.0);
        features.put("has_size_terms", SIZE_PATTERN.matcher(queryText).find() ? 1.0 : 0.0);
        features.put("has_sku_pattern", SKU_PATTERN.matcher(queryText).find() ? 1.0 : 0.0);
        features.put("contains_brand_keywords", containsBrandKeywords(queryText) ? 1.0 : 0.0);
        features.put("is_product_search", isProductSearch(queryText) ? 1.0 : 0.0);

        return features;
    }

    private Map<String, Double> extractQAFeatures(String queryText) {
        Map<String, Double> features = new HashMap<>();

        features.put("is_question", queryText.trim().endsWith("?") ? 1.0 : 0.0);
        features.put("has_question_word", QUESTION_PATTERN.matcher(queryText).find() ? 1.0 : 0.0);
        features.put("sentence_count", (double) queryText.split("[.!?]").length);
        features.put("conversational_score", calculateConversationalScore(queryText));
        features.put("subjectivity_indicator", hasSubjectiveTerms(queryText) ? 1.0 : 0.0);

        return features;
    }

    private Map<String, Double> extractMedicalFeatures(String queryText) {
        Map<String, Double> features = new HashMap<>();

        features.put("medical_acronym_count", (double) countMatches(MEDICAL_ACRONYM_PATTERN, queryText));
        features.put("has_dosage", DOSAGE_PATTERN.matcher(queryText).find() ? 1.0 : 0.0);
        features.put("has_symptom_keywords", hasSymptomKeywords(queryText) ? 1.0 : 0.0);
        features.put("has_treatment_keywords", hasTreatmentKeywords(queryText) ? 1.0 : 0.0);
        features.put("clinical_term_density", calculateClinicalTermDensity(queryText));

        return features;
    }

    private Map<String, Double> extractLegalFeatures(String queryText) {
        Map<String, Double> features = new HashMap<>();

        features.put("has_citation", CITATION_PATTERN.matcher(queryText).find() ? 1.0 : 0.0);
        features.put("has_section_ref", SECTION_PATTERN.matcher(queryText).find() ? 1.0 : 0.0);
        features.put("legal_entity_count", (double) countLegalEntities(queryText));
        features.put("has_date_range", hasDateRange(queryText) ? 1.0 : 0.0);
        features.put("formality_score", calculateFormalityScore(queryText));

        return features;
    }

    private Map<String, Double> extractTechnicalFeatures(String queryText) {
        Map<String, Double> features = new HashMap<>();

        features.put("has_code_pattern", hasCodePattern(queryText) ? 1.0 : 0.0);
        features.put("technical_term_ratio", calculateTechnicalTermRatio(queryText));
        features.put("has_version_number", hasVersionNumber(queryText) ? 1.0 : 0.0);
        features.put("camelcase_count", (double) countCamelCaseWords(queryText));
        features.put("has_error_pattern", hasErrorPattern(queryText) ? 1.0 : 0.0);

        return features;
    }

    // Helper methods
    private boolean containsBrandKeywords(String text) {
        String lower = text.toLowerCase();
        return lower.contains("apple")
            || lower.contains("samsung")
            || lower.contains("nike")
            || lower.contains("amazon")
            || lower.contains("brand");
    }

    private boolean isProductSearch(String text) {
        String lower = text.toLowerCase();
        return lower.contains("buy")
            || lower.contains("price")
            || lower.contains("cheap")
            || lower.contains("best")
            || lower.contains("review");
    }

    private double calculateConversationalScore(String text) {
        String lower = text.toLowerCase();
        double score = 0.0;
        if (lower.contains("i ") || lower.contains("my ") || lower.contains("me ")) score += 0.3;
        if (lower.contains("please") || lower.contains("thanks")) score += 0.2;
        if (lower.contains("anyone") || lower.contains("someone")) score += 0.2;
        if (lower.contains("help") || lower.contains("need")) score += 0.3;
        return Math.min(1.0, score);
    }

    private boolean hasSubjectiveTerms(String text) {
        String lower = text.toLowerCase();
        return lower.contains("think")
            || lower.contains("believe")
            || lower.contains("opinion")
            || lower.contains("feel")
            || lower.contains("seems");
    }

    private boolean hasSymptomKeywords(String text) {
        String lower = text.toLowerCase();
        return lower.contains("pain")
            || lower.contains("symptom")
            || lower.contains("fever")
            || lower.contains("ache")
            || lower.contains("discomfort");
    }

    private boolean hasTreatmentKeywords(String text) {
        String lower = text.toLowerCase();
        return lower.contains("treatment")
            || lower.contains("therapy")
            || lower.contains("medication")
            || lower.contains("drug")
            || lower.contains("cure");
    }

    private double calculateClinicalTermDensity(String text) {
        String[] clinicalTerms = {
            "diagnosis",
            "prognosis",
            "etiology",
            "pathogenesis",
            "clinical",
            "patient",
            "therapeutic",
            "intervention" };
        String lower = text.toLowerCase();
        double count = 0;
        for (String term : clinicalTerms) {
            if (lower.contains(term)) count++;
        }
        return count / Math.max(1, text.split("\\s+").length);
    }

    private int countLegalEntities(String text) {
        String lower = text.toLowerCase();
        int count = 0;
        String[] entities = { "plaintiff", "defendant", "court", "judge", "attorney", "counsel" };
        for (String entity : entities) {
            if (lower.contains(entity)) count++;
        }
        return count;
    }

    private boolean hasDateRange(String text) {
        return text.matches(".*\\b\\d{4}\\s*[-–]\\s*\\d{4}\\b.*") || text.toLowerCase().contains("between") && text.matches(".*\\d{4}.*");
    }

    private double calculateFormalityScore(String text) {
        String lower = text.toLowerCase();
        double score = 0.5; // baseline

        // Formal indicators
        if (lower.contains("pursuant") || lower.contains("whereas") || lower.contains("herein")) score += 0.2;
        if (lower.contains("shall") || lower.contains("thereof")) score += 0.1;

        // Informal indicators
        if (lower.contains("gonna") || lower.contains("wanna") || lower.contains("!")) score -= 0.2;

        return Math.max(0.0, Math.min(1.0, score));
    }

    private boolean hasCodePattern(String text) {
        return text.contains("()") || text.contains("{}") || text.contains("[]") || text.contains("->") || text.contains("::");
    }

    private double calculateTechnicalTermRatio(String text) {
        String[] techTerms = {
            "api",
            "sdk",
            "framework",
            "library",
            "function",
            "method",
            "class",
            "interface",
            "debug",
            "compile",
            "runtime" };
        String lower = text.toLowerCase();
        double count = 0;
        for (String term : techTerms) {
            if (lower.contains(term)) count++;
        }
        return count / Math.max(1, text.split("\\s+").length);
    }

    private boolean hasVersionNumber(String text) {
        return text.matches(".*\\bv?\\d+\\.\\d+(\\.\\d+)?\\b.*");
    }

    private int countCamelCaseWords(String text) {
        Pattern camelCase = Pattern.compile("\\b[a-z]+[A-Z][a-zA-Z]*\\b");
        return countMatches(camelCase, text);
    }

    private boolean hasErrorPattern(String text) {
        String lower = text.toLowerCase();
        return lower.contains("error") || lower.contains("exception") || lower.contains("failed") || text.matches(".*\\b\\d{3,4}\\b.*"); // HTTP
                                                                                                                                         // error
                                                                                                                                         // codes
    }

    private int countMatches(Pattern pattern, String text) {
        int count = 0;
        var matcher = pattern.matcher(text);
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    /**
     * Get default weights for a domain
     */
    public static double[] getDefaultWeights(Domain domain) {
        switch (domain) {
            case ECOMMERCE:
                return new double[] { 0.7, 0.3 }; // Favor lexical for product search
            case QA_CONVERSATIONAL:
                return new double[] { 0.3, 0.7 }; // Favor neural for understanding
            case MEDICAL_SCIENTIFIC:
                return new double[] { 0.6, 0.4 }; // Balanced with slight lexical preference
            case LEGAL:
                return new double[] { 0.8, 0.2 }; // Strong lexical for precise terms
            case TECHNICAL:
                return new double[] { 0.5, 0.5 }; // Balanced
            default:
                return new double[] { 0.5, 0.5 }; // Default balanced
        }
    }
}
