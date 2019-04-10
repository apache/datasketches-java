/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fun;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;
import com.yahoo.sketches.tuple.strings.ArrayOfStringsSummary;
import com.yahoo.sketches.tuple.strings.ArrayOfStringsSummaryDeserializer;
import com.yahoo.sketches.tuple.strings.ArrayOfStringsSummaryFactory;

/**
 * @author Lee Rhodes
 */
public class FunSketchTest {

  @Test
  public void checkFunSketch() {
    final int lgK = 14;
    final int k = 1 << lgK;
    final UpdatableSketchBuilder<String[], ArrayOfStringsSummary> bldr =
        new UpdatableSketchBuilder<>(new ArrayOfStringsSummaryFactory());
    bldr.setNominalEntries(k);
    final UpdatableSketch<String[], ArrayOfStringsSummary> sketch = bldr.build();

    final String[] nodesArr = {"abc", "def" };
    final int[] key = FunSketch.computeKey(nodesArr);
    sketch.update(key, nodesArr);

    final SketchIterator<ArrayOfStringsSummary> it = sketch.iterator();
    int count = 0;
    while (it.next()) {
      final String[] nodesArr2 = it.getSummary().getValue();
      assertEquals(nodesArr2, nodesArr);
      count++;
    }
    assertEquals(count, 1);

    //serialize
    final byte[] byteArr = sketch.toByteArray();
    //deserialize
    Memory mem = Memory.wrap(byteArr);
    Sketch<ArrayOfStringsSummary> sketch2 = Sketches.heapifySketch(mem, new ArrayOfStringsSummaryDeserializer());

    //check output
    final SketchIterator<ArrayOfStringsSummary> it2 = sketch2.iterator();
    int count2 = 0;
    while (it2.next()) {
      final String[] nodesArr2 = it2.getSummary().getValue();
      assertEquals(nodesArr2, nodesArr);
      count2++;
    }
    assertEquals(count, count2);
  }

  @Test
  public void checkLgKcompute() {
    final int lgK = FunSketch.computeLgK(.02, .05);
    println("LgK: " + lgK);
  }

  static void println(String s) { System.out.println(s); }

}
