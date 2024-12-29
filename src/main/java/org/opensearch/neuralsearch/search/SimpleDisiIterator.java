/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class SimpleDisiIterator implements Iterable<DisiWrapper> {
    private final DisiWrapper[] iterators;
    private final int size;

    public SimpleDisiIterator(DisiWrapper... iterators) {
        this.iterators = iterators;
        this.size = iterators.length;
        try {
            for (int i = 0; i < size; i++) {
                if (iterators[i] != null && iterators[i].doc == -1) {
                    iterators[i].doc = iterators[i].iterator.nextDoc();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public DisiWrapper top() {
        if (size == 0) {
            return null;
        }

        DisiWrapper top = null;
        int minDoc = DocIdSetIterator.NO_MORE_DOCS;

        for (int i = 0; i < size; i++) {
            DisiWrapper wrapper = iterators[i];
            if (wrapper != null && wrapper.doc != DocIdSetIterator.NO_MORE_DOCS) {
                if (minDoc == DocIdSetIterator.NO_MORE_DOCS || wrapper.doc < minDoc) {
                    minDoc = wrapper.doc;
                    top = wrapper;
                }
            }
        }
        return top;
    }

    public DisiWrapper topList() {
        DisiWrapper top = top();
        if (top == null) {
            return null;
        }

        int minDoc = top.doc;
        DisiWrapper list = null;

        try {
            // First, collect all matching wrappers and their scores
            float totalScore = 0;
            int matchCount = 0;

            // First pass: calculate total score
            for (int i = 0; i < size; i++) {
                DisiWrapper current = iterators[i];
                if (current != null && current.doc == minDoc) {
                    float score = current.scorer.score();
                    totalScore += score;
                    matchCount++;
                    list = current;
                }
            }

            // Advance all matching iterators
            /*for (int i = 0; i < size; i++) {
                DisiWrapper current = iterators[i];
                if (current != null && current.doc == minDoc) {
                    current.doc = current.iterator.nextDoc();
                }
            }*/

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return list;
    }

    @Override
    public Iterator<DisiWrapper> iterator() {
        return new Iterator<>() {
            private DisiWrapper current = null;
            private boolean initialized = false;

            private void initializeIfNeeded() {
                if (!initialized) {
                    current = topList();
                    initialized = true;
                }
            }

            @Override
            public boolean hasNext() {
                initializeIfNeeded();
                return current != null;
            }

            @Override
            public DisiWrapper next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                DisiWrapper result = current;
                current = topList();
                return result;
            }
        };
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }
}
