/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil;
import org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizer;
import org.opensearch.test.OpenSearchTestCase;

public class ScalarNormalizerTests extends OpenSearchTestCase {

    private static final float DELTA = 1e-6f;

    private ScoreCombinationTechnique arithmeticMean() {
        return new ArithmeticMeanScoreCombinationTechnique(Map.of(), new ScoreCombinationUtil());
    }

    private Map<String, Float> leg(Object... idScorePairs) {
        Map<String, Float> leg = new LinkedHashMap<>();
        for (int i = 0; i < idScorePairs.length; i += 2) {
            leg.put((String) idScorePairs[i], (Float) idScorePairs[i + 1]);
        }
        return leg;
    }

    // ---- factory ----

    public void testFactory_resolvesMinMax() {
        ScalarNormalizer normalizer = ScalarNormalizerFactory.create("min_max");
        assertSame(MinMaxScalarNormalizer.INSTANCE, normalizer);
        assertEquals("min_max", normalizer.techniqueName());
    }

    public void testFactory_whenTechniqueNotWiredYet_thenThrows() {
        // z_score / l2 / rrf parse but have no coordinator implementation yet — the caller rejects them at rewrite, so
        // this is the defense-in-depth backstop.
        for (String notWired : List.of("z_score", "l2", "rrf", "none")) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ScalarNormalizerFactory.create(notWired));
            assertTrue(e.getMessage().contains("is not supported in fused mode"));
        }
    }

    public void testFactory_whenNullTechnique_thenThrows() {
        expectThrows(IllegalArgumentException.class, () -> ScalarNormalizerFactory.create(null));
    }

    // ---- min_max normalizer ----

    public void testMinMaxNormalizeLeg_matchesSharedScalarMath() {
        // The normalizer must be a pure re-expression of the shared classic math, per score.
        Map<String, Float> raw = leg("a", 1.0f, "b", 1.5f, "c", 2.0f);

        Map<String, Float> normalized = MinMaxScalarNormalizer.INSTANCE.normalizeLeg(raw);

        assertEquals(raw.keySet(), normalized.keySet());
        assertEquals(MinMaxScoreNormalizer.normalizeSingleScore(1.0f, 1.0f, 2.0f), normalized.get("a"), DELTA);
        assertEquals(MinMaxScoreNormalizer.normalizeSingleScore(1.5f, 1.0f, 2.0f), normalized.get("b"), DELTA);
        assertEquals(MinMaxScoreNormalizer.normalizeSingleScore(2.0f, 1.0f, 2.0f), normalized.get("c"), DELTA);
    }

    public void testMinMaxNormalizeLeg_whenEmptyLeg_thenEmptyResult() {
        // A leg that matched nothing must stay empty — CoordinatorScoreFusion reads a missing key as "leg didn't match".
        assertTrue(MinMaxScalarNormalizer.INSTANCE.normalizeLeg(Map.of()).isEmpty());
    }

    public void testMinMaxNormalizeLeg_whenSingleScore_thenOne() {
        // min == max single-result edge case: classic returns 1.0.
        Map<String, Float> normalized = MinMaxScalarNormalizer.INSTANCE.normalizeLeg(leg("only", 0.7f));
        assertEquals(1.0f, normalized.get("only"), DELTA);
    }

    public void testMinMaxNormalizeLeg_preservesEveryKey() {
        Map<String, Float> raw = leg("a", 0.1f, "b", 0.2f, "c", 0.3f, "d", 0.4f);
        assertEquals(raw.size(), MinMaxScalarNormalizer.INSTANCE.normalizeLeg(raw).size());
    }

    // ---- fuse(normalizer) is behavior-identical to the fuseMinMax it replaced ----

    public void testFuse_withMinMaxNormalizer_equalsFuseMinMax() {
        // Locks in that routing normalization through the ScalarNormalizer seam changed nothing: same fused scores,
        // same key order. This is the guard that the refactor stayed behavior-preserving.
        List<Map<String, Float>> legs = List.of(leg("1", 0.9f, "2", 0.5f), leg("2", 0.8f, "3", 0.4f));

        Map<String, Float> viaSeam = CoordinatorScoreFusion.fuse(legs, MinMaxScalarNormalizer.INSTANCE, arithmeticMean());
        Map<String, Float> viaConvenience = CoordinatorScoreFusion.fuseMinMax(legs, arithmeticMean());

        assertEquals(viaConvenience.keySet(), viaSeam.keySet());
        assertEquals(List.copyOf(viaConvenience.keySet()), List.copyOf(viaSeam.keySet()));
        for (String id : viaConvenience.keySet()) {
            assertEquals(viaConvenience.get(id), viaSeam.get(id), 0.0f);
        }
    }

    public void testFuse_withCustomNormalizer_isPluggedIn() {
        // A stand-in normalizer proves the seam is real: fusion uses whatever the normalizer returns, and a key the
        // normalizer omits is treated as "leg did not match" (0.0 slot).
        ScalarNormalizer constantHalf = new ScalarNormalizer() {
            @Override
            public Map<String, Float> normalizeLeg(Map<String, Float> legRawScores) {
                Map<String, Float> out = new LinkedHashMap<>();
                legRawScores.keySet().forEach(id -> out.put(id, 0.5f));
                return out;
            }

            @Override
            public String techniqueName() {
                return "constant_half";
            }
        };
        List<Map<String, Float>> legs = List.of(leg("1", 0.9f), leg("1", 0.2f));

        Map<String, Float> fused = CoordinatorScoreFusion.fuse(legs, constantHalf, arithmeticMean());

        // Both legs matched doc 1 and both normalized to 0.5 → unweighted arithmetic mean is 0.5.
        assertEquals(0.5f, fused.get("1"), DELTA);
    }

    public void testFuse_whenKeysAreOpaqueComposites_thenCarriedThroughUnchanged() {
        // Keys are opaque to fusion and to normalizers: a composite identity string round-trips untouched, which is what
        // lets the caller change its document identity scheme without touching this layer.
        List<Map<String, Float>> legs = List.of(leg("idx-a#1", 0.9f), leg("idx-b#1", 0.8f));

        Map<String, Float> fused = CoordinatorScoreFusion.fuse(legs, MinMaxScalarNormalizer.INSTANCE, arithmeticMean());

        assertEquals(2, fused.size());
        assertTrue(fused.containsKey("idx-a#1"));
        assertTrue(fused.containsKey("idx-b#1"));
    }
}
