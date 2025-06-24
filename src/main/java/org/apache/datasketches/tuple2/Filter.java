/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.tuple2;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.function.Predicate;

/**
 * Class for filtering entries from a {@link Sketch} given a {@link Summary}
 *
 * @param <T> Summary type against which apply the {@link Predicate}
 */
public class Filter<T extends Summary> {
    private final Predicate<T> predicate;

    /**
     * Filter constructor with a {@link Predicate}
     *  @param predicate Predicate to use in this filter. If the Predicate returns False, the
     *  element is discarded. If the Predicate returns True, then the element is kept in the
     *  {@link Sketch}
     */
    public Filter(final Predicate<T> predicate) {
        this.predicate = predicate;
    }

    /**
     * Filters elements on the provided {@link Sketch}
     *
     * @param sketchIn The sketch against which apply the {@link Predicate}
     * @return A new Sketch with some of the entries filtered out based on the {@link Predicate}
     */
    @SuppressWarnings("unchecked")
    public CompactSketch<T> filter(final Sketch<T> sketchIn) {
        if (sketchIn == null) {
            return new CompactSketch<>(null, null, Long.MAX_VALUE, true);
        }
        final long[] hashes = new long[sketchIn.getRetainedEntries()];
        T[] summaries = null; // lazy init to get class from the first entry
        int i = 0;
        final TupleSketchIterator<T> it = sketchIn.iterator();
        while (it.next()) {
            final T summary = it.getSummary();
            if (predicate.test(summary)) {
              hashes[i] = it.getHash();
              if (summaries == null) {
                summaries = (T[]) Array.newInstance(summary.getClass(), sketchIn.getRetainedEntries());
              }
              summaries[i++] = (T) summary.copy();
            }
        }
        final boolean isEmpty = i == 0 && !sketchIn.isEstimationMode(); 
        if (i == 0) {
          return new CompactSketch<>(null, null, sketchIn.getThetaLong(), isEmpty);
        }
        return new CompactSketch<>(Arrays.copyOf(hashes, i), Arrays.copyOf(summaries, i), sketchIn.getThetaLong(), isEmpty);
    }
}

