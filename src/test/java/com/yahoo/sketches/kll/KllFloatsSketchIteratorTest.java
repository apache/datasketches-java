package com.yahoo.sketches.kll;

import org.testng.Assert;
import org.testng.annotations.Test;

public class KllFloatsSketchIteratorTest {

  @Test
  public void emptySketch() {
    KllFloatsSketch sketch = new KllFloatsSketch();
    KllFloatsSketchIterator it = sketch.iterator();
    Assert.assertFalse(it.next());
  }

  @Test
  public void oneItemSketch() {
    KllFloatsSketch sketch = new KllFloatsSketch();
    sketch.update(0);
    KllFloatsSketchIterator it = sketch.iterator();
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getValue(), 0f);
    Assert.assertEquals(it.getWeight(), 1);
    Assert.assertFalse(it.next());
  }

  @Test
  public void bigSketches() {
    for (int n = 1000; n < 100000; n += 2000) {
      KllFloatsSketch sketch = new KllFloatsSketch();
      for (int i = 0; i < n; i++) { 
        sketch.update(i);
      }
      KllFloatsSketchIterator it = sketch.iterator();
      int count = 0;
      while (it.next()) {
        count++;
      }
      Assert.assertEquals(count, sketch.getNumRetained());
    }
  }

}
