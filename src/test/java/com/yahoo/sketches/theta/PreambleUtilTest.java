/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.PreambleUtil.clearEmpty;
import static com.yahoo.sketches.theta.PreambleUtil.extractCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.extractFlags;
import static com.yahoo.sketches.theta.PreambleUtil.extractFlagsV1;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgNomLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgResizeFactor;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgResizeRatioV1;
import static com.yahoo.sketches.theta.PreambleUtil.extractP;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.theta.PreambleUtil.extractThetaLong;
import static com.yahoo.sketches.theta.PreambleUtil.extractUnionThetaLong;
import static com.yahoo.sketches.theta.PreambleUtil.insertCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.insertFlags;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgNomLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgResizeFactor;
import static com.yahoo.sketches.theta.PreambleUtil.insertP;
import static com.yahoo.sketches.theta.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.theta.PreambleUtil.insertThetaLong;
import static com.yahoo.sketches.theta.PreambleUtil.insertUnionThetaLong;
import static com.yahoo.sketches.theta.PreambleUtil.isEmpty;
import static com.yahoo.sketches.theta.PreambleUtil.setEmpty;
import static com.yahoo.sketches.theta.SetOperation.getMaxUnionBytes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * @author Lee Rhodes
 */
public class PreambleUtilTest {

  @Test
  public void checkToString() {
    int k = 4096;
    int u = 2*k;
    int bytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    byte[] byteArray = new byte[bytes];
    WritableMemory mem = WritableMemory.wrap(byteArray);

    UpdateSketch quick1 = UpdateSketch.builder().setNominalEntries(k).build(mem);
    println(Sketch.toString(byteArray));

    Assert.assertTrue(quick1.isEmpty());

    for (int i = 0; i< u; i++) {
      quick1.update(i);
    }
    println("U: "+quick1.getEstimate());

    assertEquals(quick1.getEstimate(), u, .05*u);
    assertTrue(quick1.getRetainedEntries(false) > k);
    println(quick1.toString());
    println(PreambleUtil.preambleToString(mem));

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]);
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);
    union.update(quick1);
    println(PreambleUtil.preambleToString(uMem));
  }

  @Test
  public void checkToStringWithPrelongsOf2() {
    int k = 16;
    int u = k;
    UpdateSketch quick1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i< u; i++) {
      quick1.update(i);
    }
    byte[] bytes = quick1.compact().toByteArray();
    println(Sketch.toString(bytes));
  }

  @Test
  public void checkPreambleToStringExceptions() {
    byte[] byteArr = new byte[7];
    try { //check preLongs < 8 fails
      Sketch.toString(byteArr);
      fail("Did not throw SketchesArgumentException.");
    } catch (SketchesArgumentException e) {
      //expected
    }
    byteArr = new byte[8];
    byteArr[0] = (byte) 2; //needs min capacity of 16
    try { //check preLongs == 2 fails
      Sketch.toString(Memory.wrap(byteArr));
      fail("Did not throw SketchesArgumentException.");
    } catch (SketchesArgumentException e) {
      //expected
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSeedHashFromSeed() {
    //In the first 64K values 50541 produces a seedHash of 0,
    Util.computeSeedHash(50541);
  }

  @Test
  public void checkPreLongs() {
    UpdateSketch sketch = UpdateSketch.builder().setNominalEntries(16).build();
    CompactSketch comp = sketch.compact(false, null);
    byte[] byteArr = comp.toByteArray();
    println(Sketch.toString(byteArr)); //PreLongs = 1

    sketch.update(1);
    comp = sketch.compact(false, null);
    byteArr = comp.toByteArray();
    println(Sketch.toString(byteArr)); //PreLongs = 2

    for (int i=2; i<=32; i++) {
      sketch.update(i);
    }
    comp = sketch.compact(false, null);
    byteArr = comp.toByteArray();
    println(Sketch.toString(Memory.wrap(byteArr))); //PreLongs = 3
  }

  @Test
  public void checkInsertsAndExtracts() {
    byte[] arr = new byte[32];
    WritableMemory mem = WritableMemory.wrap(arr);

    int v = 0;
    insertPreLongs(mem, ++v);
    assertEquals(extractPreLongs(mem), v);

    insertLgResizeFactor(mem, 3); //limited to 2 bits
    assertEquals(extractLgResizeFactor(mem), 3);

    insertSerVer(mem, ++v);
    assertEquals(extractSerVer(mem), v);

    insertFamilyID(mem, ++v);
    assertEquals(extractFamilyID(mem), v);

    insertLgNomLongs(mem, ++v);
    assertEquals(extractLgNomLongs(mem), v);

    insertLgArrLongs(mem, ++v);
    assertEquals(extractLgArrLongs(mem), v);

    insertFlags(mem, 3);
    assertEquals(extractFlags(mem), 3);
    assertEquals(extractLgResizeRatioV1(mem), 3); //also at byte 5, limited to 2 bits

    insertSeedHash(mem, ++v);
    assertEquals(extractSeedHash(mem), v);
    assertEquals(extractFlagsV1(mem), v); //also at byte 6

    insertCurCount(mem, ++v);
    assertEquals(extractCurCount(mem), v);

    insertP(mem, (float) 1.0);
    assertEquals(extractP(mem), (float) 1.0);

    insertThetaLong(mem, ++v);
    assertEquals(extractThetaLong(mem), v);

    insertUnionThetaLong(mem, ++v);
    assertEquals(extractUnionThetaLong(mem), v);

    setEmpty(mem);
    assertTrue(isEmpty(mem));

    clearEmpty(mem);
    assertFalse(isEmpty(mem));
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
