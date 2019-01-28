/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.DefaultMemoryRequestServer;
import com.yahoo.memory.MemoryRequestServer;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;
/**
 * @author Lee Rhodes
 */
public class UpdateSketchTest {

  @Test
  public void checkOtherUpdates() {
    int k = 512;
    UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(k).build();
    sk1.update(1L);   //#1 long
    sk1.update(1.5);  //#2 double
    sk1.update(0.0);
    sk1.update(-0.0); //#3 double
    String s = null;
    sk1.update(s);    //null string
    s = "";
    sk1.update(s);    //empty string
    s = "String";
    sk1.update(s);    //#4 actual string

    byte[] byteArr = null;
    sk1.update(byteArr); //null byte[]
    byteArr = new byte[0];
    sk1.update(byteArr); //empty byte[]
    byteArr = "Byte Array".getBytes(UTF_8);
    sk1.update(byteArr); //#5 actual byte[]

    char[] charArr = null;
    sk1.update(charArr); //null char[]
    charArr = new char[0];
    sk1.update(charArr); //empty char[]
    charArr = "String".toCharArray();
    sk1.update(charArr); //#6 actual char[]

    int[] intArr = null;
    sk1.update(intArr); //null int[]
    intArr = new int[0];
    sk1.update(intArr); //empty int[]
    int[] intArr2 = { 1, 2, 3, 4, 5 };
    sk1.update(intArr2); //#7 actual int[]

    long[] longArr = null;
    sk1.update(longArr); //null long[]
    longArr = new long[0];
    sk1.update(longArr); //empty long[]
    long[] longArr2 = { 6, 7, 8, 9 };
    sk1.update(longArr2); //#8 actual long[]

    double est = sk1.getEstimate();
    assertEquals(est, 8.0, 0.0);
  }

  @Test
  public void checkStartingSubMultiple() {
    int lgSubMul;
    lgSubMul = Util.startingSubMultiple(10, ResizeFactor.X1, 5);
    assertEquals(lgSubMul, 10);
    lgSubMul = Util.startingSubMultiple(10, ResizeFactor.X2, 5);
    assertEquals(lgSubMul, 5);
    lgSubMul = Util.startingSubMultiple(10, ResizeFactor.X4, 5);
    assertEquals(lgSubMul, 6);
    lgSubMul = Util.startingSubMultiple(10, ResizeFactor.X8, 5);
    assertEquals(lgSubMul, 7);
    lgSubMul = Util.startingSubMultiple(4, ResizeFactor.X1, 5);
    assertEquals(lgSubMul, 5);
  }

  @Test
  public void checkBuilder() {
    UpdateSketchBuilder bldr = UpdateSketch.builder();

    long seed = 12345L;
    bldr.setSeed(seed);
    assertEquals(bldr.getSeed(), seed);

    float p = (float)0.5;
    bldr.setP(p);
    assertEquals(bldr.getP(), p);

    ResizeFactor rf = ResizeFactor.X4;
    bldr.setResizeFactor(rf);
    assertEquals(bldr.getResizeFactor(), rf);

    Family fam = Family.ALPHA;
    bldr.setFamily(fam);
    assertEquals(bldr.getFamily(), fam);

    int lgK = 10;
    int k = 1 << lgK;
    bldr.setNominalEntries(k);
    assertEquals(bldr.getLgNominalEntries(), lgK);

    MemoryRequestServer mrs = new DefaultMemoryRequestServer();
    bldr.setMemoryRequestServer(mrs);
    assertEquals(bldr.getMemoryRequestServer(), mrs);

    println(bldr.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBuilderNomEntries() {
    UpdateSketchBuilder bldr = UpdateSketch.builder();
    int k = 1 << 27;
    bldr.setNominalEntries(k);
  }

  @Test
  public void checkCompact() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    CompactSketch csk = sk.compact();
    assertEquals(csk.getCurrentBytes(true), 8);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIncompatibleFamily() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    sk.update(1);
    WritableMemory wmem = WritableMemory.wrap(sk.compact().toByteArray());
    UpdateSketch.wrap(wmem, DEFAULT_UPDATE_SEED);
  }

  @Test
  public void checkCorruption() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    sk.update(1);
    WritableMemory wmem = WritableMemory.wrap(sk.toByteArray());
    try {
      wmem.putByte(SER_VER_BYTE, (byte) 2);
      UpdateSketch.wrap(wmem, DEFAULT_UPDATE_SEED);
      fail();
    } catch (SketchesArgumentException e) { }
    try {
      wmem.putByte(SER_VER_BYTE, (byte) 3);
      wmem.putByte(PREAMBLE_LONGS_BYTE, (byte) 2);
      UpdateSketch.wrap(wmem, DEFAULT_UPDATE_SEED);
      fail();
    } catch (SketchesArgumentException e) { }
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
