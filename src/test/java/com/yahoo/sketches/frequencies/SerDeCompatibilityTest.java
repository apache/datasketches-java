/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfLongsSerDe;

public class SerDeCompatibilityTest {

  static final ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();

  @Test
  public void itemsToLongs() {
    ItemsSketch<Long> sketch1 = new ItemsSketch<Long>(8);
    sketch1.update(1L);
    sketch1.update(2L);
    sketch1.update(3L);
    sketch1.update(4L);

    byte[] bytes = sketch1.toByteArray(serDe);
    LongsSketch sketch2 = LongsSketch.getInstance(WritableMemory.wrap(bytes));
    sketch2.update(2L);
    sketch2.update(3L);
    sketch2.update(2L);

    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 4);
    Assert.assertEquals(sketch2.getStreamLength(), 7);
    Assert.assertEquals(sketch2.getEstimate(1L), 1);
    Assert.assertEquals(sketch2.getEstimate(2L), 3);
    Assert.assertEquals(sketch2.getEstimate(3L), 2);
    Assert.assertEquals(sketch2.getEstimate(4L), 1);
  }

  @Test
  public void longsToItems() {
    LongsSketch sketch1 = new LongsSketch(8);
    sketch1.update(1L);
    sketch1.update(2L);
    sketch1.update(3L);
    sketch1.update(4L);

    byte[] bytes = sketch1.toByteArray();
    ItemsSketch<Long> sketch2 = ItemsSketch.getInstance(WritableMemory.wrap(bytes), serDe);
    sketch2.update(2L);
    sketch2.update(3L);
    sketch2.update(2L);

    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 4);
    Assert.assertEquals(sketch2.getStreamLength(), 7);
    Assert.assertEquals(sketch2.getEstimate(1L), 1);
    Assert.assertEquals(sketch2.getEstimate(2L), 3);
    Assert.assertEquals(sketch2.getEstimate(3L), 2);
    Assert.assertEquals(sketch2.getEstimate(4L), 1);
  }

  @Test
  public void checkInsertSerDeId() {
    long pre0 = 0L;
    pre0 = PreambleUtil.insertSerDeId((short) -1, pre0);
    short serDeId = PreambleUtil.extractSerDeId(pre0);
    Assert.assertEquals(serDeId, -1);
  }
}
