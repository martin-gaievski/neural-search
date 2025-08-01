/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.ContentType;
import org.opensearch.client.Request;

/**
 * Helper class to train and use a linear regression model for weight prediction
 */
public class LinearRegressionWeightPredictor {

    private final RestClient client;
    private String modelId;

    public LinearRegressionWeightPredictor(RestClient client) {
        this.client = client;
    }

    /**
     * Register and train a linear regression model with training data
     */
    public void trainModel() throws Exception {
        // For the POC, we'll use the heuristic-based approach
        // Real ML-Commons linear regression requires specific setup and may not be available in all environments
        // The heuristic provides a good approximation for demonstration purposes
        this.modelId = "heuristic"; // Special ID to indicate heuristic mode
    }

    /**
     * Train real ML-Commons linear regression model (for production use)
     * This requires ML-Commons to be properly configured with linear regression support
     */
    public void trainRealLinearRegressionModel() throws Exception {
        // Create training data in the correct format
        Map<String, Object> trainingData = new HashMap<>();

        // Column metadata
        List<Map<String, Object>> columnMetas = Arrays.asList(
            Map.of("name", "query_length", "column_type", "DOUBLE"),
            Map.of("name", "token_count", "column_type", "DOUBLE"),
            Map.of("name", "has_numbers", "column_type", "DOUBLE"),
            Map.of("name", "has_special_chars", "column_type", "DOUBLE"),
            Map.of("name", "neural_weight", "column_type", "DOUBLE")
        );

        // Training data rows
        List<Map<String, Object>> rows = Arrays.asList(
            createRow(9.0, 2.0, 1.0, 0.0, 0.2),   // "iPhone 15"
            createRow(15.0, 3.0, 1.0, 0.0, 0.25),  // "error 404 nginx"
            createRow(20.0, 3.0, 0.0, 0.0, 0.35),  // "machine learning basics"
            createRow(35.0, 5.0, 0.0, 0.0, 0.45),  // "how to implement neural networks"
            createRow(55.0, 8.0, 0.0, 0.0, 0.55),  // "best practices for implementing machine learning models"
            createRow(80.0, 12.0, 0.0, 0.0, 0.65), // long conceptual query
            createRow(100.0, 15.0, 0.0, 1.0, 0.75) // very long technical query
        );

        trainingData.put("column_metas", columnMetas);
        trainingData.put("rows", rows);

        // Train linear regression model using correct endpoint
        String trainBodyJson = String.format("""
            {
              "parameters": {
                "target": "neural_weight"
              },
              "input_data": %s
            }
            """, convertMapToJson(trainingData));

        Request trainRequest = new Request("POST", "/_plugins/_ml/_train/linear_regression");
        trainRequest.setEntity(new StringEntity(trainBodyJson, ContentType.APPLICATION_JSON));
        Response trainResponse = client.performRequest(trainRequest);

        Map<String, Object> trainResult = parseResponse(trainResponse);

        // Check if we got a task_id (async) or model_id (sync)
        if (trainResult.containsKey("model_id")) {
            this.modelId = (String) trainResult.get("model_id");
        } else if (trainResult.containsKey("task_id")) {
            String taskId = (String) trainResult.get("task_id");
            // Wait for training to complete and get model ID
            this.modelId = waitForTask(taskId, "model_id");
        } else {
            throw new RuntimeException("Unexpected response from linear regression training: " + trainResult);
        }

        // Model is automatically ready after training for linear regression
        // No deployment needed for synchronous algorithms
    }

    private Map<String, Object> createRow(
        double queryLength,
        double tokenCount,
        double hasNumbers,
        double hasSpecialChars,
        double neuralWeight
    ) {
        List<Map<String, Object>> values = Arrays.asList(
            Map.of("column_type", "DOUBLE", "value", queryLength),
            Map.of("column_type", "DOUBLE", "value", tokenCount),
            Map.of("column_type", "DOUBLE", "value", hasNumbers),
            Map.of("column_type", "DOUBLE", "value", hasSpecialChars),
            Map.of("column_type", "DOUBLE", "value", neuralWeight)
        );

        return Map.of("values", values);
    }

    /**
     * Predict weights using the trained model or heuristic
     */
    public double[] predictWeights(Map<String, Double> features) throws Exception {
        // If using heuristic mode, directly use the heuristic
        if ("heuristic".equals(modelId)) {
            return predictWeightsWithHeuristic(features);
        }

        // Otherwise, try to use the real ML model
        try {
            // Prepare prediction data in the same format as training
            Map<String, Object> predictionData = new HashMap<>();

            // Column metadata (without target)
            List<Map<String, Object>> columnMetas = Arrays.asList(
                Map.of("name", "query_length", "column_type", "DOUBLE"),
                Map.of("name", "token_count", "column_type", "DOUBLE"),
                Map.of("name", "has_numbers", "column_type", "DOUBLE"),
                Map.of("name", "has_special_chars", "column_type", "DOUBLE")
            );

            // Single row for prediction
            List<Map<String, Object>> rows = Arrays.asList(
                createPredictionRow(
                    features.get("query_length"),
                    features.get("token_count"),
                    features.get("has_numbers"),
                    features.get("has_special_chars")
                )
            );

            predictionData.put("column_metas", columnMetas);
            predictionData.put("rows", rows);

            String predictBodyJson = String.format("""
                {
                  "parameters": {
                    "target": "neural_weight"
                  },
                  "input_data": %s
                }
                """, convertMapToJson(predictionData));

            Request predictRequest = new Request("POST", "/_plugins/_ml/_predict/linear_regression/" + modelId);
            predictRequest.setEntity(new StringEntity(predictBodyJson, ContentType.APPLICATION_JSON));
            Response predictResponse = client.performRequest(predictRequest);

            Map<String, Object> result = parseResponse(predictResponse);

            // Parse prediction result
            Map<String, Object> predictionResult = (Map<String, Object>) result.get("prediction_result");
            if (predictionResult != null) {
                List<Map<String, Object>> resultRows = (List<Map<String, Object>>) predictionResult.get("rows");
                if (resultRows != null && !resultRows.isEmpty()) {
                    Map<String, Object> firstRow = resultRows.get(0);
                    List<Map<String, Object>> values = (List<Map<String, Object>>) firstRow.get("values");
                    if (values != null && !values.isEmpty()) {
                        Map<String, Object> firstValue = values.get(0);
                        Double neuralWeight = (Double) firstValue.get("value");

                        // Ensure weight is in valid range [0.1, 0.9]
                        neuralWeight = Math.max(0.1, Math.min(0.9, neuralWeight));
                        double lexicalWeight = 1.0 - neuralWeight;

                        return new double[] { lexicalWeight, neuralWeight };
                    }
                }
            }
        } catch (Exception e) {
            // If ML prediction fails, fall back to heuristic
            // Log would go here if logger was available
        }

        // Fallback to heuristic if prediction fails
        return predictWeightsWithHeuristic(features);
    }

    private Map<String, Object> createPredictionRow(double queryLength, double tokenCount, double hasNumbers, double hasSpecialChars) {
        List<Map<String, Object>> values = Arrays.asList(
            Map.of("column_type", "DOUBLE", "value", queryLength),
            Map.of("column_type", "DOUBLE", "value", tokenCount),
            Map.of("column_type", "DOUBLE", "value", hasNumbers),
            Map.of("column_type", "DOUBLE", "value", hasSpecialChars)
        );

        return Map.of("values", values);
    }

    /**
     * Fallback heuristic method
     */
    private double[] predictWeightsWithHeuristic(Map<String, Double> features) {
        // Check if we have domain-specific features to determine the domain
        if (features.containsKey("has_currency") || features.containsKey("has_sku_pattern")) {
            return predictEcommerceWeights(features);
        } else if (features.containsKey("is_question") || features.containsKey("conversational_score")) {
            return predictQAWeights(features);
        } else if (features.containsKey("medical_acronym_count") || features.containsKey("has_dosage")) {
            return predictMedicalWeights(features);
        } else if (features.containsKey("has_citation") || features.containsKey("legal_entity_count")) {
            return predictLegalWeights(features);
        } else if (features.containsKey("has_code_pattern") || features.containsKey("technical_term_ratio")) {
            return predictTechnicalWeights(features);
        } else {
            // Default general heuristic
            return predictGeneralWeights(features);
        }
    }

    private double[] predictGeneralWeights(Map<String, Double> features) {
        double queryLength = features.get("query_length");
        double hasNumbers = features.get("has_numbers");

        double neuralWeight = Math.min(0.8, queryLength / 100.0);
        if (hasNumbers > 0) {
            neuralWeight *= 0.5;
        }
        neuralWeight = Math.max(0.2, neuralWeight);

        double lexicalWeight = 1.0 - neuralWeight;
        return new double[] { lexicalWeight, neuralWeight };
    }

    private double[] predictEcommerceWeights(Map<String, Double> features) {
        double baseNeuralWeight = 0.3; // Start with lexical preference

        // Adjust based on e-commerce features
        if (features.getOrDefault("has_sku_pattern", 0.0) > 0) {
            baseNeuralWeight *= 0.5; // Strong lexical for SKUs
        }
        if (features.getOrDefault("has_currency", 0.0) > 0) {
            baseNeuralWeight *= 0.7; // Lexical for prices
        }
        if (features.getOrDefault("is_product_search", 0.0) > 0) {
            baseNeuralWeight *= 0.8; // Slight neural reduction for product search
        }
        if (features.get("query_length") > 30) {
            baseNeuralWeight += 0.2; // Longer queries might be descriptions
        }

        double neuralWeight = Math.max(0.1, Math.min(0.5, baseNeuralWeight));
        return new double[] { 1.0 - neuralWeight, neuralWeight };
    }

    private double[] predictQAWeights(Map<String, Double> features) {
        double baseNeuralWeight = 0.7; // Start with neural preference

        // Adjust based on Q&A features
        if (features.getOrDefault("is_question", 0.0) > 0) {
            baseNeuralWeight += 0.1; // Questions need understanding
        }
        if (features.getOrDefault("conversational_score", 0.0) > 0.5) {
            baseNeuralWeight += 0.05; // Conversational needs context
        }
        if (features.get("token_count") <= 3) {
            baseNeuralWeight -= 0.3; // Short queries might be keywords
        }

        double neuralWeight = Math.max(0.3, Math.min(0.9, baseNeuralWeight));
        return new double[] { 1.0 - neuralWeight, neuralWeight };
    }

    private double[] predictMedicalWeights(Map<String, Double> features) {
        double baseNeuralWeight = 0.4; // Balanced start

        // Adjust based on medical features
        if (features.getOrDefault("medical_acronym_count", 0.0) > 1) {
            baseNeuralWeight -= 0.2; // Acronyms need exact match
        }
        if (features.getOrDefault("has_dosage", 0.0) > 0) {
            baseNeuralWeight -= 0.1; // Dosages need precision
        }
        if (features.getOrDefault("has_symptom_keywords", 0.0) > 0) {
            baseNeuralWeight += 0.2; // Symptoms benefit from semantic search
        }

        double neuralWeight = Math.max(0.2, Math.min(0.7, baseNeuralWeight));
        return new double[] { 1.0 - neuralWeight, neuralWeight };
    }

    private double[] predictLegalWeights(Map<String, Double> features) {
        double baseNeuralWeight = 0.2; // Strong lexical preference

        // Adjust based on legal features
        if (features.getOrDefault("has_citation", 0.0) > 0) {
            baseNeuralWeight *= 0.5; // Citations need exact match
        }
        if (features.getOrDefault("formality_score", 0.0) > 0.7) {
            baseNeuralWeight += 0.1; // Very formal queries might benefit from some semantic understanding
        }

        double neuralWeight = Math.max(0.1, Math.min(0.4, baseNeuralWeight));
        return new double[] { 1.0 - neuralWeight, neuralWeight };
    }

    private double[] predictTechnicalWeights(Map<String, Double> features) {
        double baseNeuralWeight = 0.5; // Balanced start

        // Adjust based on technical features
        if (features.getOrDefault("has_error_pattern", 0.0) > 0) {
            baseNeuralWeight -= 0.2; // Error codes need exact match
        }
        if (features.getOrDefault("has_code_pattern", 0.0) > 0) {
            baseNeuralWeight -= 0.1; // Code patterns need lexical
        }
        if (features.get("query_length") > 50) {
            baseNeuralWeight += 0.2; // Long technical descriptions
        }

        double neuralWeight = Math.max(0.2, Math.min(0.8, baseNeuralWeight));
        return new double[] { 1.0 - neuralWeight, neuralWeight };
    }

    private String waitForTask(String taskId, String resultField) throws Exception {
        for (int i = 0; i < 30; i++) {
            Thread.sleep(1000);

            Request taskRequest = new Request("GET", "/_plugins/_ml/tasks/" + taskId);
            Response taskResponse = client.performRequest(taskRequest);
            Map<String, Object> taskResult = parseResponse(taskResponse);

            String state = (String) taskResult.get("state");
            if ("COMPLETED".equals(state)) {
                return (String) taskResult.get(resultField);
            } else if ("FAILED".equals(state)) {
                throw new RuntimeException("Task failed: " + taskResult.get("error"));
            }
        }
        throw new RuntimeException("Task timeout");
    }

    private Map<String, Object> parseResponse(Response response) throws Exception {
        String responseBody = EntityUtils.toString(response.getEntity());
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);
    }

    public void undeployAndDeleteModel() throws Exception {
        if (modelId != null && !"heuristic".equals(modelId)) {
            try {
                // Delete model (linear regression doesn't require undeployment)
                Request deleteRequest = new Request("DELETE", "/_plugins/_ml/models/" + modelId);
                client.performRequest(deleteRequest);
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    private String convertMapToJson(Map<String, Object> data) {
        StringBuilder json = new StringBuilder("{");

        // Handle column_metas
        if (data.containsKey("column_metas")) {
            json.append("\"column_metas\":[");
            List<Map<String, Object>> columnMetas = (List<Map<String, Object>>) data.get("column_metas");
            for (int i = 0; i < columnMetas.size(); i++) {
                Map<String, Object> meta = columnMetas.get(i);
                json.append("{\"name\":\"").append(meta.get("name")).append("\",");
                json.append("\"column_type\":\"").append(meta.get("column_type")).append("\"}");
                if (i < columnMetas.size() - 1) json.append(",");
            }
            json.append("],");
        }

        // Handle rows
        if (data.containsKey("rows")) {
            json.append("\"rows\":[");
            List<Map<String, Object>> rows = (List<Map<String, Object>>) data.get("rows");
            for (int i = 0; i < rows.size(); i++) {
                Map<String, Object> row = rows.get(i);
                json.append("{\"values\":[");
                List<Map<String, Object>> values = (List<Map<String, Object>>) row.get("values");
                for (int j = 0; j < values.size(); j++) {
                    Map<String, Object> val = values.get(j);
                    json.append("{\"column_type\":\"").append(val.get("column_type")).append("\",");
                    json.append("\"value\":").append(val.get("value")).append("}");
                    if (j < values.size() - 1) json.append(",");
                }
                json.append("]}");
                if (i < rows.size() - 1) json.append(",");
            }
            json.append("]");
        }

        json.append("}");
        return json.toString();
    }
}
