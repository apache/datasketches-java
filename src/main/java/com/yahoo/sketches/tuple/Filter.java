package com.yahoo.sketches.tuple;

import java.util.function.Predicate;

import com.yahoo.sketches.ResizeFactor;

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
                sketch.insert(it.getKey(), (T)summary.copy());
            }
        }

        sketch.setThetaLong(sketchIn.getThetaLong());
        if (!sketchIn.isEmpty()) {
            sketch.setNotEmpty();
        }

        return sketch.compact();
    }
}

