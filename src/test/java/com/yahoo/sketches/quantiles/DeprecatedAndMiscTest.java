/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.HeapUpdateDoublesSketchTest.buildAndLoadQS;

import java.util.Comparator;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class DeprecatedAndMiscTest {

  @SuppressWarnings({ "deprecation", "unused" })
  @Test
  public void checkDeprecatedRankError() {
    DoublesSketch ds = buildAndLoadQS(64, 64);
    double err = ds.getNormalizedRankError();
    err = DoublesSketch.getNormalizedRankError(64);
    DoublesUnion du1 = DoublesUnionBuilder.heapify(ds);

    Memory mem = Memory.wrap(ds.toByteArray());
    DoublesUnion du2 = DoublesUnionBuilder.heapify(mem);

    DoublesUnion du3 = DoublesUnionBuilder.wrap(mem);

    WritableMemory wmem = WritableMemory.wrap(ds.toByteArray());
    DoublesUnion du4 = DoublesUnionBuilder.wrap(wmem);

    ItemsSketch<String> is = ItemsSketch.getInstance(64, Comparator.naturalOrder());
    err = is.getNormalizedRankError();
    err = ItemsSketch.getNormalizedRankError(64);
  }

}
