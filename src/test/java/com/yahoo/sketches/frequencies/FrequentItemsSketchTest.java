package com.yahoo.sketches.frequencies;

import org.testng.annotations.Test;

import com.yahoo.sketches.memory.NativeMemory;

import org.testng.Assert;

public class FrequentItemsSketchTest {

  @Test
  public void empty() {
    FrequentItemsSketch<String> sketch = new FrequentItemsSketch<String>(8);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
    Assert.assertEquals(sketch.getLowerBound("a"), 0);
    Assert.assertEquals(sketch.getUpperBound("a"), 0);
  }

  @Test
  public void nullInput() {
    FrequentItemsSketch<String> sketch = new FrequentItemsSketch<String>(8);
    sketch.update(null);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
    Assert.assertEquals(sketch.getLowerBound(null), 0);
    Assert.assertEquals(sketch.getUpperBound(null), 0);
  }

  @Test
  public void oneItem() {
    FrequentItemsSketch<String> sketch = new FrequentItemsSketch<String>(8);
    sketch.update("a");
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 1);
    Assert.assertEquals(sketch.getStreamLength(), 1);
    Assert.assertEquals(sketch.getEstimate("a"), 1);
    Assert.assertEquals(sketch.getLowerBound("a"), 1);
  }

  @Test
  public void severalItems() {
    FrequentItemsSketch<String> sketch = new FrequentItemsSketch<String>(8);
    sketch.update("a");
    sketch.update("b");
    sketch.update("c");
    sketch.update("d");
    sketch.update("b");
    sketch.update("c");
    sketch.update("b");
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 4);
    Assert.assertEquals(sketch.getStreamLength(), 7);
    Assert.assertEquals(sketch.getEstimate("a"), 1);
    Assert.assertEquals(sketch.getEstimate("b"), 3);
    Assert.assertEquals(sketch.getEstimate("c"), 2);
    Assert.assertEquals(sketch.getEstimate("d"), 1);
  }

  @Test
  public void serializeDeserialize() {
    FrequentItemsSketch<String> sketch1 = new FrequentItemsSketch<String>(8);
    sketch1.update("a");
    sketch1.update("b");
    sketch1.update("c");
    sketch1.update("d");

    byte[] bytes = sketch1.serializeToByteArray(new StringArraySerDe());
    FrequentItemsSketch<String> sketch2 = FrequentItemsSketch.getInstance(new NativeMemory(bytes), new StringArraySerDe());
    sketch2.update("b");
    sketch2.update("c");
    sketch2.update("b");

    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 4);
    Assert.assertEquals(sketch2.getStreamLength(), 7);
    Assert.assertEquals(sketch2.getEstimate("a"), 1);
    Assert.assertEquals(sketch2.getEstimate("b"), 3);
    Assert.assertEquals(sketch2.getEstimate("c"), 2);
    Assert.assertEquals(sketch2.getEstimate("d"), 1);
  }

}
