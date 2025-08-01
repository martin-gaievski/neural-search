/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * POC Test for Dynamic Hybrid Search with ML-based weight prediction
 * This demonstrates the concept using ml-commons linear regression
 */
public class DynamicHybridSearchPOCTest {

    private static final QueryFeatureExtractor featureExtractor = new QueryFeatureExtractor();

    public static void main(String[] args) {
        System.out.println("=== Dynamic Hybrid Search POC Test ===\n");

        // Step 1: Create test queries
        List<TestQuery> testQueries = createTestQueries();

        // Step 2: Extract features and predict weights
        System.out.println("Query Feature Extraction and Weight Prediction:\n");

        for (TestQuery query : testQueries) {
            // Extract features
            Map<String, Double> features = featureExtractor.extractFeatures(query.text);

            // Predict weights using simple heuristic (in production, use ml-commons linear regression)
            double[] weights = predictWeightsWithLinearRegression(features);

            System.out.printf("Query: %s\n", query.text);
            System.out.printf(
                "Features: length=%.0f, tokens=%.0f, has_numbers=%.0f, has_special=%.0f\n",
                features.get("query_length"),
                features.get("token_count"),
                features.get("has_numbers"),
                features.get("has_special_chars")
            );
            System.out.printf("Predicted Weights: lexical=%.2f, neural=%.2f\n\n", weights[0], weights[1]);
        }

        // Step 3: Show sample ml-commons linear regression configuration
        System.out.println("ML-Commons Linear Regression Configuration:\n");
        showLinearRegressionConfig();

        // Step 4: Show sample search pipeline configuration
        System.out.println("\nSearch Pipeline Configuration:\n");
        showSearchPipelineConfig();
    }

    private static List<TestQuery> createTestQueries() {
        return Arrays.asList(
            new TestQuery("iPhone 15", "Short product query - should favor lexical"),
            new TestQuery("error 404 nginx", "Technical query with numbers - should favor lexical"),
            new TestQuery(
                "What are the best practices for implementing machine learning models in production",
                "Long descriptive query - should favor neural"
            ),
            new TestQuery("How to optimize database performance for high-traffic applications", "Another long query - should favor neural")
        );
    }

    /**
     * Simple heuristic to simulate linear regression prediction
     * In production, this would call ml-commons linear regression model
     */
    private static double[] predictWeightsWithLinearRegression(Map<String, Double> features) {
        // Simulate linear regression: w = b0 + b1*length + b2*tokens + b3*numbers + b4*special
        // Coefficients learned from training data
        double b0 = 0.5;     // intercept
        double b1 = -0.005;  // query_length coefficient
        double b2 = -0.02;   // token_count coefficient
        double b3 = 0.3;     // has_numbers coefficient
        double b4 = 0.1;     // has_special_chars coefficient

        double neuralWeight = b0 + b1 * features.get("query_length") + b2 * features.get("token_count") + b3 * (1 - features.get(
            "has_numbers"
        ))  // Invert: no numbers = more neural
            + b4 * (1 - features.get("has_special_chars"));

        // Constrain to [0.2, 0.8] range
        neuralWeight = Math.max(0.2, Math.min(0.8, neuralWeight));
        double lexicalWeight = 1.0 - neuralWeight;

        return new double[] { lexicalWeight, neuralWeight };
    }

    private static void showLinearRegressionConfig() {
        String config = """
            // Register linear regression model in ml-commons
            POST /_plugins/_ml/models/_register
            {
              "name": "hybrid_weight_predictor",
              "function_name": "LINEAR_REGRESSION",
              "version": "1.0.0",
              "description": "Predicts optimal weights for hybrid search",
              "model_format": "TORCH_SCRIPT",
              "model_config": {
                "model_type": "linear_regression",
                "embedding_dimension": 4,
                "framework_type": "linear_regression",
                "all_config": "{\\"target\\":\\"neural_weight\\",\\"train_test_split\\":0.2}"
              }
            }

            // Training data format (CSV):
            // query_length,token_count,has_numbers,has_special_chars,neural_weight
            // 15,2,1,0,0.2
            // 100,15,0,0,0.8
            // ...
            """;
        System.out.println(config);
    }

    private static void showSearchPipelineConfig() {
        String pipeline = """
            // Create search pipeline with ml_inference
            PUT /_search/pipeline/dynamic_hybrid_pipeline
            {
              "description": "Dynamic hybrid search with ML weight prediction",
              "request_processors": [
                {
                  "ml_inference": {
                    "model_id": "<model_id>",
                    "function_name": "LINEAR_REGRESSION",
                    "model_input": "{ \\"parameters\\": {\\"input\\": ${input_map.features}}}",
                    "input_map": [
                      {
                        "features": "query.features"  // Pass extracted features
                      }
                    ],
                    "output_map": [
                      {
                        "predicted_neural_weight": "$.inference_results[0].output[0]"
                      }
                    ],
                    "query_template": {
                      "query": {
                        "hybrid": {
                          "queries": [
                            {
                              "match": {
                                "content": "${query_text}"
                              }
                            },
                            {
                              "neural": {
                                "content_embedding": {
                                  "query_text": "${query_text}",
                                  "k": 50
                                }
                              }
                            }
                          ]
                        }
                      },
                      "search_pipeline": {
                        "phase_results_processors": [
                          {
                            "normalization-processor": {
                              "normalization": {
                                "technique": "l2"
                              },
                              "combination": {
                                "technique": "arithmetic_mean",
                                "parameters": {
                                  "weights": [
                                    ${1 - predicted_neural_weight},  // lexical weight
                                    ${predicted_neural_weight}        // neural weight
                                  ]
                                }
                              }
                            }
                          }
                        ]
                      }
                    }
                  }
                }
              ]
            }
            """;
        System.out.println(pipeline);
    }

    private static class TestQuery {
        final String text;
        final String description;

        TestQuery(String text, String description) {
            this.text = text;
            this.description = description;
        }
    }
}
