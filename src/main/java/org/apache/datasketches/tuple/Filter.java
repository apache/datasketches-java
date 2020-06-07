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

package org.apache.datasketches.tuple;

import java.util.function.Predicate;

import org.apache.datasketches.ResizeFactor;

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

        final QuickSelectSketch<T> sketch =
            new QuickSelectSketch<>(sketchIn.getRetainedEntries(), ResizeFactor.X1.lg(), null);
        final SketchIterator<T> it = sketchIn.iterator();
        while (it.next()) {
            final T summary = it.getSummary();
            if (predicate.test(summary)) {
                sketch.insert(it.getHash(), (T)summary.copy());
            }
        }

        sketch.setThetaLong(sketchIn.getThetaLong());
        if (!sketchIn.isEmpty()) {
            sketch.setEmpty(false);
        }

        return sketch.compact();
    }
}

