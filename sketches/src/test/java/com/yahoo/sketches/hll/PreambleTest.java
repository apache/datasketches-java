/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import com.yahoo.sketches.hll.Preamble.Builder;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;
import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;

import org.testng.Assert;
import org.testng.annotations.Test;

public class PreambleTest
{
  @Test
  public void testSerDe() {
    Preamble preamble = new Preamble.Builder()
        .setLogConfigK((byte) 10)
        .setSeed(Util.DEFAULT_UPDATE_SEED)
        .setFlags((byte) 12).build();

    byte[] bytes = new byte[10];
    int initOffset = 1;
    int newOffset = preamble.intoByteArray(bytes, initOffset);
    Assert.assertEquals(newOffset, initOffset + (Preamble.PREAMBLE_LONGS << 3));

    Memory mem = new MemoryRegion(new NativeMemory(bytes), 1, bytes.length - 1);
    Preamble serdePreamble = Preamble.fromMemory(mem);

    Assert.assertEquals(serdePreamble, preamble);
  }

  @Test
  public void testMaxAuxSize() {
    Preamble preamble10 = new Preamble.Builder()
        .setLogConfigK((byte) 10).build();

    final int intByteSize = 4;
    Assert.assertEquals(preamble10.getMaxAuxSize(), 16 * intByteSize);

    Preamble preamble13 = new Preamble.Builder()
        .setLogConfigK((byte) 13).build();
    Assert.assertEquals(preamble13.getMaxAuxSize(), 32 * intByteSize);

    Preamble preamble16 = new Preamble.Builder()
        .setLogConfigK((byte) 16).build();
    Assert.assertEquals(preamble16.getMaxAuxSize(), 256 * intByteSize);

    Preamble preamble26 = new Preamble.Builder()
        .setLogConfigK((byte) 21).build();
    Assert.assertEquals(preamble26.getMaxAuxSize(), 8192 * intByteSize);

  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void testSharedPreambleTooLarge() {
    Preamble.fromLogK(256);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void testSharedPreambleTooLarge2() {
    Preamble.fromLogK(50);
  }

  @Test
  public void testHashCodeAndEquals() {
    Preamble preamble = Preamble.fromLogK(13);
    Assert.assertEquals(preamble.hashCode(), Preamble.fromLogK(13).hashCode());
    Assert.assertEquals(preamble, preamble);

    Assert.assertTrue(preamble.equals(Preamble.fromLogK(13)));
    Assert.assertNotNull(preamble);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIntoByteArray() {
    Preamble preamble = new Preamble.Builder()
        .setLogConfigK((byte) 10)
        .setSeed(Util.DEFAULT_UPDATE_SEED)
        .setFlags((byte) 12).build();
    byte[] bytes = new byte[10];
    int initOffset = 3;
    preamble.intoByteArray(bytes, initOffset);
  }
  
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSeedHashFromSeed() {
    Builder bldr = new Preamble.Builder();
    //In the first 64K values 50541 produces a seedHash of 0, 
    bldr.setSeed(50541L);
  }
  
  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }
  
  /**
   * @param s value to print 
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
  
}
