package com.yahoo.sketches.tuple;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Random;

public class FilterTest {

    private final int numberOfElements = 100;

    @Test
    public void testFilterEmptySketch() {
        Sketch<DoubleSummary> sketch = Sketches.createEmptySketch();

        Filter<DoubleSummary> filter = new Filter<>(o -> true);

        Sketch<DoubleSummary> filteredSketch = filter.filter(sketch);

        Assert.assertEquals(filteredSketch.getEstimate(), 0.0d);
        Assert.assertEquals(filteredSketch.getThetaLong(), sketch.getThetaLong());
        Assert.assertTrue(filteredSketch.isEmpty());
    }

    @Test
    public void testFilterNullSketch() {
        Filter<DoubleSummary> filter = new Filter<>(o -> true);

        Sketch<DoubleSummary> filteredSketch = filter.filter(null);

        Assert.assertEquals(filteredSketch.getEstimate(), 0.0d);
        Assert.assertEquals(filteredSketch.getThetaLong(), Long.MAX_VALUE);
        Assert.assertTrue(filteredSketch.isEmpty());
    }

    @Test
    public void testFilterFilledSketchShouldBehaveTheSame() {
        UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();

        fillSketch(sketch, numberOfElements, 0.0d);

        Filter<DoubleSummary> filter = new Filter<>(o -> true);

        Sketch<DoubleSummary> filteredSketch = filter.filter(sketch);

        Assert.assertEquals(filteredSketch.getEstimate(), sketch.getEstimate());
        Assert.assertEquals(filteredSketch.getThetaLong(), sketch.getThetaLong());
        Assert.assertFalse(filteredSketch.isEmpty());
    }

    @Test
    public void testFilterFilledSketchShouldFilterOutElements() {
        UpdatableSketch<Double, DoubleSummary> sketch = new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();

        fillSketch(sketch, numberOfElements, 0.0d);
        fillSketch(sketch, 2 * numberOfElements, 1.0d);

        Filter<DoubleSummary> filter = new Filter<>(o -> o.getValue() < 0.5d);

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