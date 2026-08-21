/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Resolves a coordinator-side {@link ScalarNormalizer} by technique name — the extension point for widening fused-mode
 * normalization support. Adding a technique is a new {@link ScalarNormalizer} plus one entry here; neither
 * {@link CoordinatorScoreFusion} nor the orchestrator changes. A static holder rather than a factory, since nothing is
 * constructed: every technique is a stateless singleton, so the lookup hands back a shared instance.
 *
 * <p>Only {@code min_max} is wired today, matching the fused-mode scope gate in
 * {@code HybridQueryBuilder#requireSupportedTechniques} (which rejects other techniques earlier, at rewrite). The throw
 * below is therefore a defense-in-depth backstop, not the user-facing validation.
 *
 * <p>When adding techniques, two things need a decision that this seam intentionally leaves open:
 * <ul>
 *   <li><b>{@code l2}/{@code z_score}</b> drop in as plain {@link ScalarNormalizer}s. But note
 *       {@link CoordinatorScoreFusion} encodes "leg did not match this doc" as a {@code 0.0} slot, and {@code l2} can
 *       legitimately normalize a real score to {@code 0.0}. That is harmless for arithmetic-mean (which counts
 *       {@code >= 0.0} slots, matching classic) but ambiguous for geometric/harmonic mean, which skip {@code <= 0}
 *       scores. Wiring those combiners likely needs an explicit presence mask rather than the {@code 0.0} sentinel —
 *       changing the sentinel is a deliberate parity decision, since the current behavior is what the classic-vs-fused
 *       differential test pins.</li>
 *   <li><b>RRF</b> is rank-based and is modelled as {@code combination=rrf, normalization=none}, so it does not arrive
 *       here under a normalization name. It fits the {@link ScalarNormalizer} shape (sort the leg, emit
 *       {@code 1/(rank_constant + rank + 1)} by position) but needs its own routing plus a rank_constant parameter, and
 *       the classic {@code (doc, shardId)} rank tie-break has no equivalent over the coordinator's already-merged view —
 *       so a tie-break must be defined explicitly.</li>
 * </ul>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ScalarNormalizers {

    private static final Map<String, ScalarNormalizer> NORMALIZERS = Map.of(
        MinMaxScalarNormalizer.INSTANCE.techniqueName(),
        MinMaxScalarNormalizer.INSTANCE
    );

    /**
     * @param techniqueName normalization technique name from the resolved fusion config
     * @return the normalizer for that technique
     * @throws IllegalArgumentException when the technique has no coordinator-side implementation yet
     */
    public static ScalarNormalizer forTechnique(final String techniqueName) {
        ScalarNormalizer normalizer = Objects.isNull(techniqueName) ? null : NORMALIZERS.get(techniqueName);
        if (Objects.isNull(normalizer)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "normalization technique [%s] is not supported in fused mode; supported: %s",
                    techniqueName,
                    NORMALIZERS.keySet()
                )
            );
        }
        return normalizer;
    }
}
