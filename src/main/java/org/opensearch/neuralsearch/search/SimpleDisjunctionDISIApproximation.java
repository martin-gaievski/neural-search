/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

public class SimpleDisjunctionDISIApproximation extends DocIdSetIterator {

    private final SimpleDisiIterator iterator;
    private final long cost;
    private int doc = -1;

    public SimpleDisjunctionDISIApproximation(SimpleDisiIterator iterator) {
        this.iterator = iterator;

        // Calculate total cost
        long totalCost = 0;
        DisiWrapper top = iterator.top();
        if (top != null) {
            DisiWrapper current = iterator.topList();
            while (current != null) {
                totalCost += current.cost;
                current = current.next;
            }
        }
        this.cost = totalCost;
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        DisiWrapper top = iterator.top();
        if (top == null) {
            return doc = NO_MORE_DOCS;
        }

        final int current = top.doc;

        // Advance all iterators that are at current doc
        DisiWrapper matchingList = iterator.topList();
        while (matchingList != null) {
            matchingList.doc = matchingList.approximation.nextDoc();
            matchingList = matchingList.next;
        }

        return doc = iterator.top() != null ? iterator.top().doc : NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) throws IOException {
        DisiWrapper top = iterator.top();
        if (top == null) {
            return doc = NO_MORE_DOCS;
        }

        // If we're already at or past target, just do nextDoc()
        if (top.doc >= target) {
            return nextDoc();
        }

        // Advance all iterators to target
        DisiWrapper matchingList = iterator.topList();
        while (matchingList != null) {
            matchingList.doc = matchingList.approximation.advance(target);
            matchingList = matchingList.next;
        }

        return doc = iterator.top() != null ? iterator.top().doc : NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return cost;
    }

    /**
     * Returns the number of sub-iterators
     */
    public int getSubIteratorCount() {
        return iterator.size();
    }

    /**
     * Returns list of matching sub-iterators at current position
     */
    public DisiWrapper getMatches() {
        return iterator.topList();
    }
}
