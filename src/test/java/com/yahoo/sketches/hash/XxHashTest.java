/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hash;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;

/**
 * @author Lee Rhodes
 */
public class XxHashTest {

  @Test
  public void longCheck() {
    long seed = 0;
    long hash1 = XxHash.hash(123L, seed);
    long[] arr = new long[1];
    arr[0] = 123L;
    Memory mem = Memory.wrap(arr);
    long hash2 = XxHash.hash(mem, 0, 8, 0);
    assertEquals(hash2, hash1);
  }

}
