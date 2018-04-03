package com.yahoo.sketches.tuple;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.function.Predicate;

import static org.testng.Assert.*;

public class FilterTest {

    private final int numberOfElements = 100;

    @Test
    public void testFilterEmptySketch() {
        DoubleSummaryFactory factory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum);
        Sketch<DoubleSummary> sketch = Sketches.createEmptySketch();

        Filter<DoubleSummary> filter = new Filter<>(o -> true, factory);

        Sketch<DoubleSummary> filteredSketch = filter.filter(sketch);

        Assert.assertEquals(filteredSketch.getEstimate(), 0.0d);
        Assert.assertEquals(filteredSketch.getThetaLong(), sketch.getThetaLong());
        Assert.assertTrue(filteredSketch.isEmpty());
    }

    @Test
    public void testFilterFilledSketchShouldBehaveTheSame() {
        DoubleSummaryFactory factory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum);
        UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();

        fillSketch(sketch, numberOfElements, 0.0d);

        Filter<DoubleSummary> filter = new Filter<>(o -> true, factory);

        Sketch<DoubleSummary> filteredSketch = filter.filter(sketch);

        Assert.assertEquals(filteredSketch.getEstimate(), sketch.getEstimate());
        Assert.assertEquals(filteredSketch.getThetaLong(), sketch.getThetaLong());
        Assert.assertFalse(filteredSketch.isEmpty());
    }

    @Test
    public void testFilterFilledSketchShouldFilterOutElements() {
        DoubleSummaryFactory factory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum);
        UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();

        fillSketch(sketch, numberOfElements, 0.0d);
        fillSketch(sketch, 2 * numberOfElements, 1.0d);

        Filter<DoubleSummary> filter = new Filter<>(o -> o.getValue() < 0.5d, factory);

        Sketch<DoubleSummary> filteredSketch = filter.filter(sketch);

        Assert.assertEquals(filteredSketch.getEstimate(), 100.0d);
        Assert.assertEquals(filteredSketch.getThetaLong(), sketch.getThetaLong());
        Assert.assertFalse(filteredSketch.isEmpty());
    }

    private void fillSketch(UpdatableSketch<Double, DoubleSummary> sketch, int numberOfElements, Double sketchValue) {
        Random random = new Random();

        for (int cont = 0; cont < numberOfElements; cont++) {
            sketch.update(random.nextLong(), sketchValue);
        }
    }
}