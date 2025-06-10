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

package org.apache.datasketches.theta2;

import static org.apache.datasketches.theta2.PreambleUtil.clearEmpty;
import static org.apache.datasketches.theta2.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta2.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta2.PreambleUtil.extractFlags;
import static org.apache.datasketches.theta2.PreambleUtil.extractFlagsV1;
import static org.apache.datasketches.theta2.PreambleUtil.extractLgArrLongs;
import static org.apache.datasketches.theta2.PreambleUtil.extractLgNomLongs;
import static org.apache.datasketches.theta2.PreambleUtil.extractLgResizeFactor;
import static org.apache.datasketches.theta2.PreambleUtil.extractLgResizeRatioV1;
import static org.apache.datasketches.theta2.PreambleUtil.extractP;
import static org.apache.datasketches.theta2.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta2.PreambleUtil.extractSeedHash;
import static org.apache.datasketches.theta2.PreambleUtil.extractSerVer;
import static org.apache.datasketches.theta2.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta2.PreambleUtil.extractUnionThetaLong;
import static org.apache.datasketches.theta2.PreambleUtil.insertCurCount;
import static org.apache.datasketches.theta2.PreambleUtil.insertFamilyID;
import static org.apache.datasketches.theta2.PreambleUtil.insertFlags;
import static org.apache.datasketches.theta2.PreambleUtil.insertLgArrLongs;
import static org.apache.datasketches.theta2.PreambleUtil.insertLgNomLongs;
import static org.apache.datasketches.theta2.PreambleUtil.insertLgResizeFactor;
import static org.apache.datasketches.theta2.PreambleUtil.insertP;
import static org.apache.datasketches.theta2.PreambleUtil.insertPreLongs;
import static org.apache.datasketches.theta2.PreambleUtil.insertSeedHash;
import static org.apache.datasketches.theta2.PreambleUtil.insertSerVer;
import static org.apache.datasketches.theta2.PreambleUtil.insertThetaLong;
import static org.apache.datasketches.theta2.PreambleUtil.insertUnionThetaLong;
import static org.apache.datasketches.theta2.PreambleUtil.isEmptyFlag;
import static org.apache.datasketches.theta2.PreambleUtil.setEmpty;
import static org.apache.datasketches.theta2.SetOperation.getMaxUnionBytes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.thetacommon.ThetaUtil;
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
    final MemorySegment mem = MemorySegment.ofArray(byteArray);

    final UpdateSketch quick1 = UpdateSketch.builder().setNominalEntries(k).build(mem);
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

    final MemorySegment uMem = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]);
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);
    union.union(quick1);
    println(PreambleUtil.preambleToString(uMem));
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
    ThetaUtil.computeSeedHash(50541);
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
    final MemorySegment wmem = MemorySegment.ofArray(arr);

    int v = 0;
    insertPreLongs(wmem, ++v);
    assertEquals(extractPreLongs(wmem), v);
    insertPreLongs(wmem, 0);

    insertLgResizeFactor(wmem, 3); //limited to 2 bits
    assertEquals(extractLgResizeFactor(wmem), 3);
    insertLgResizeFactor(wmem, 0);

    insertSerVer(wmem, ++v);
    assertEquals(extractSerVer(wmem), v);
    insertSerVer(wmem, 0);

    insertFamilyID(wmem, ++v);
    assertEquals(extractFamilyID(wmem), v);
    insertFamilyID(wmem, 0);

    insertLgNomLongs(wmem, ++v);
    assertEquals(extractLgNomLongs(wmem), v);
    insertLgNomLongs(wmem, 0);

    insertLgArrLongs(wmem, ++v);
    assertEquals(extractLgArrLongs(wmem), v);
    insertLgArrLongs(wmem, 0);

    insertFlags(wmem, 3);
    assertEquals(extractFlags(wmem), 3);
    assertEquals(extractLgResizeRatioV1(wmem), 3); //also at byte 5, limited to 2 bits
    insertFlags(wmem, 0);

    insertSeedHash(wmem, ++v);
    assertEquals(extractSeedHash(wmem), v);
    assertEquals(extractFlagsV1(wmem), v); //also at byte 6
    insertSeedHash(wmem, 0);

    insertCurCount(wmem, ++v);
    assertEquals(extractCurCount(wmem), v);
    insertCurCount(wmem, 0);

    insertP(wmem, (float) 1.0);
    assertEquals(extractP(wmem), (float) 1.0);
    insertP(wmem, (float) 0.0);

    insertThetaLong(wmem, ++v);
    assertEquals(extractThetaLong(wmem), v);
    insertThetaLong(wmem, 0L);

    insertUnionThetaLong(wmem, ++v);
    assertEquals(extractUnionThetaLong(wmem), v);
    insertUnionThetaLong(wmem, 0L);

    setEmpty(wmem);
    assertTrue(isEmptyFlag(wmem));

    clearEmpty(wmem);
    assertFalse(isEmptyFlag(wmem));
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
