package com.yahoo.sketches.tuple;

import java.util.function.Predicate;

/**
 * Class for filtering values from a {@link Sketch} given a {@link Summary}
 *
 * @param <T> Summary type against to which apply the {@link Predicate}
 */
public class Filter<T extends Summary> {
    private Predicate<T> predicate;
    private SummaryFactory<T> summaryFactory;

    /**
     * Filter constructor with a {@link Predicate} and {@link SummaryFactory}
     *
     * @param predicate Predicate to use in this filter. If the Predicate returns False, the element is discarded.
     *                 If the Predicate returns True, then the element is kept in the {@link Sketch}
     * @param summaryFactory Summary Factory to use to build a new summary
     */
    public Filter(Predicate<T> predicate, SummaryFactory<T> summaryFactory) {
        this.predicate = predicate;
        this.summaryFactory = summaryFactory;
    }

    /**
     * Filters elements on the provided {@link Sketch}
     * 
     * @param sketchIn The sketch against to which apply the {@link Predicate}
     * @return A new Sketch with some of the elements filtered based on the {@link Predicate}
     */
    public CompactSketch<T> filter(Sketch<T> sketchIn) {
        QuickSelectSketch<T> sketch = new QuickSelectSketch<>(sketchIn.getRetainedEntries(), 0, summaryFactory);

        final SketchIterator<T> it = sketchIn.iterator();
        while (it.next()) {
            final T summary = it.getSummary();
            if (predicate.test(summary)) {
                sketch.insert(it.getKey(), summary.copy());
            }
        }

        sketch.setThetaLong(sketchIn.getThetaLong());
        sketch.setNotEmpty();

        return sketch.compact();
    }
}

