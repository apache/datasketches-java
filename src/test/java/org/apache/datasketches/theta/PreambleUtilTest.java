/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.theta;

import static org.apache.datasketches.theta.PreambleUtil.clearEmpty;
import static org.apache.datasketches.theta.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.extractFlags;
import static org.apache.datasketches.theta.PreambleUtil.extractFlagsV1;
import static org.apache.datasketches.theta.PreambleUtil.extractLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractLgNomLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractLgResizeFactor;
import static org.apache.datasketches.theta.PreambleUtil.extractLgResizeRatioV1;
import static org.apache.datasketches.theta.PreambleUtil.extractP;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.extractSerVer;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.extractUnionThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.insertCurCount;
import static org.apache.datasketches.theta.PreambleUtil.insertFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.insertFlags;
import static org.apache.datasketches.theta.PreambleUtil.insertLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertLgNomLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertLgResizeFactor;
import static org.apache.datasketches.theta.PreambleUtil.insertP;
import static org.apache.datasketches.theta.PreambleUtil.insertPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.insertSerVer;
import static org.apache.datasketches.theta.PreambleUtil.insertThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.insertUnionThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.isEmptyFlag;
import static org.apache.datasketches.theta.PreambleUtil.setEmpty;
import static org.apache.datasketches.theta.SetOperation.getMaxUnionBytes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.PreambleUtil;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class PreambleUtilTest {

  @Test
  public void checkToString() {
    final int k = 4096;
    final int u = 2*k;
    final int bytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    final byte[] byteArray = new byte[bytes];
    final MemorySegment seg = MemorySegment.ofArray(byteArray);

    final UpdateSketch quick1 = UpdateSketch.builder().setNominalEntries(k).build(seg);
    println(Sketch.toString(byteArray));

    Assert.assertTrue(quick1.isEmpty());

    for (int i = 0; i< u; i++) {
      quick1.update(i);
    }
    println("U: "+quick1.getEstimate());

    assertEquals(quick1.getEstimate(), u, .05*u);
    assertTrue(quick1.getRetainedEntries(false) > k);
    println(quick1.toString());
    println(PreambleUtil.preambleToString(seg));

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]);
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);
    union.union(quick1);
    println(PreambleUtil.preambleToString(uSeg));
  }

  @Test
  public void checkToStringWithPrelongsOf2() {
    final int k = 16;
    final int u = k;
    final UpdateSketch quick1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i< u; i++) {
      quick1.update(i);
    }
    final byte[] bytes = quick1.compact().toByteArray();
    println(Sketch.toString(bytes));
  }

  @Test
  public void checkPreambleToStringExceptions() {
    byte[] byteArr = new byte[7];
    try { //check preLongs < 8 fails
      Sketch.toString(byteArr);
      fail("Did not throw SketchesArgumentException.");
    } catch (final SketchesArgumentException e) {
      //expected
    }
    byteArr = new byte[8];
    byteArr[0] = (byte) 2; //needs min capacity of 16
    try { //check preLongs == 2 fails
      Sketch.toString(MemorySegment.ofArray(byteArr).asReadOnly());
      fail("Did not throw SketchesArgumentException.");
    } catch (final SketchesArgumentException e) {
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
    final UpdateSketch sketch = UpdateSketch.builder().setNominalEntries(16).build();
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
    println(Sketch.toString(MemorySegment.ofArray(byteArr).asReadOnly())); //PreLongs = 3
  }

  @Test
  public void checkInsertsAndExtracts() {
    final byte[] arr = new byte[32];
    final MemorySegment wseg = MemorySegment.ofArray(arr);

    int v = 0;
    insertPreLongs(wseg, ++v);
    assertEquals(extractPreLongs(wseg), v);
    insertPreLongs(wseg, 0);

    insertLgResizeFactor(wseg, 3); //limited to 2 bits
    assertEquals(extractLgResizeFactor(wseg), 3);
    insertLgResizeFactor(wseg, 0);

    insertSerVer(wseg, ++v);
    assertEquals(extractSerVer(wseg), v);
    insertSerVer(wseg, 0);

    insertFamilyID(wseg, ++v);
    assertEquals(extractFamilyID(wseg), v);
    insertFamilyID(wseg, 0);

    insertLgNomLongs(wseg, ++v);
    assertEquals(extractLgNomLongs(wseg), v);
    insertLgNomLongs(wseg, 0);

    insertLgArrLongs(wseg, ++v);
    assertEquals(extractLgArrLongs(wseg), v);
    insertLgArrLongs(wseg, 0);

    insertFlags(wseg, 3);
    assertEquals(extractFlags(wseg), 3);
    assertEquals(extractLgResizeRatioV1(wseg), 3); //also at byte 5, limited to 2 bits
    insertFlags(wseg, 0);

    insertSeedHash(wseg, ++v);
    assertEquals(extractSeedHash(wseg), v);
    assertEquals(extractFlagsV1(wseg), v); //also at byte 6
    insertSeedHash(wseg, 0);

    insertCurCount(wseg, ++v);
    assertEquals(extractCurCount(wseg), v);
    insertCurCount(wseg, 0);

    insertP(wseg, (float) 1.0);
    assertEquals(extractP(wseg), (float) 1.0);
    insertP(wseg, (float) 0.0);

    insertThetaLong(wseg, ++v);
    assertEquals(extractThetaLong(wseg), v);
    insertThetaLong(wseg, 0L);

    insertUnionThetaLong(wseg, ++v);
    assertEquals(extractUnionThetaLong(wseg), v);
    insertUnionThetaLong(wseg, 0L);

    setEmpty(wseg);
    assertTrue(isEmptyFlag(wseg));

    clearEmpty(wseg);
    assertFalse(isEmptyFlag(wseg));
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
