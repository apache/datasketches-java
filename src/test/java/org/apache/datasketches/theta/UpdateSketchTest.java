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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.common.Util.equalContents;
import static org.apache.datasketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.insertLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertLgNomLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertLgResizeFactor;
import static org.apache.datasketches.theta.UpdateSketch.isResizeFactorIncorrect;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.theta.CompactOperations;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class UpdateSketchTest {

  @Test
  public void checkOtherUpdates() {
    final int k = 512;
    final UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(k).build();
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
    sk1.update(ByteBuffer.wrap(byteArr)); // empty byte[]
    byteArr = "Byte Array".getBytes(UTF_8);
    sk1.update(byteArr); //#5 actual byte[]
    sk1.update(ByteBuffer.wrap(byteArr, 0, 10));  // whole byte array
    sk1.update(ByteBuffer.wrap(byteArr, 2, 6));   // #6 byte array slice

    char[] charArr = null;
    sk1.update(charArr); //null char[]
    charArr = new char[0];
    sk1.update(charArr); //empty char[]
    charArr = "String".toCharArray();
    sk1.update(charArr); //#7 actual char[]

    int[] intArr = null;
    sk1.update(intArr); //null int[]
    intArr = new int[0];
    sk1.update(intArr); //empty int[]
    final int[] intArr2 = { 1, 2, 3, 4, 5 };
    sk1.update(intArr2); //#8 actual int[]

    long[] longArr = null;
    sk1.update(longArr); //null long[]
    longArr = new long[0];
    sk1.update(longArr); //empty long[]
    final long[] longArr2 = { 6, 7, 8, 9 };
    sk1.update(longArr2); //#9 actual long[]

    final double est = sk1.getEstimate();
    assertEquals(est, 9.0, 0.0);
  }

  @Test
  public void checkStartingSubMultiple() {
    int lgSubMul;
    lgSubMul = ThetaUtil.startingSubMultiple(10, ResizeFactor.X1.lg(), 5);
    assertEquals(lgSubMul, 10);
    lgSubMul = ThetaUtil.startingSubMultiple(10, ResizeFactor.X2.lg(), 5);
    assertEquals(lgSubMul, 5);
    lgSubMul = ThetaUtil.startingSubMultiple(10, ResizeFactor.X4.lg(), 5);
    assertEquals(lgSubMul, 6);
    lgSubMul = ThetaUtil.startingSubMultiple(10, ResizeFactor.X8.lg(), 5);
    assertEquals(lgSubMul, 7);
    lgSubMul = ThetaUtil.startingSubMultiple(4, ResizeFactor.X1.lg(), 5);
    assertEquals(lgSubMul, 5);
  }

  @Test
  public void checkBuilder() {
    final UpdateSketchBuilder bldr = UpdateSketch.builder();

    final long seed = 12345L;
    bldr.setSeed(seed);
    assertEquals(bldr.getSeed(), seed);

    final float p = (float)0.5;
    bldr.setP(p);
    assertEquals(bldr.getP(), p);

    final ResizeFactor rf = ResizeFactor.X4;
    bldr.setResizeFactor(rf);
    assertEquals(bldr.getResizeFactor(), rf);

    final Family fam = Family.ALPHA;
    bldr.setFamily(fam);
    assertEquals(bldr.getFamily(), fam);

    final int lgK = 10;
    final int k = 1 << lgK;
    bldr.setNominalEntries(k);
    assertEquals(bldr.getLgNominalEntries(), lgK);

    println(bldr.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBuilderNomEntries() {
    final UpdateSketchBuilder bldr = UpdateSketch.builder();
    final int k = 1 << 27;
    bldr.setNominalEntries(k);
  }

  @Test
  public void checkCompact() {
    final UpdateSketch sk = UpdateSketch.builder().build();
    final CompactSketch csk = sk.compact();
    assertEquals(csk.getCompactBytes(), 8);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIncompatibleFamily() {
    final UpdateSketch sk = UpdateSketch.builder().build();
    sk.update(1);
    final MemorySegment wseg = MemorySegment.ofArray(sk.compact().toByteArray());
    UpdateSketch.wrap(wseg, null, Util.DEFAULT_UPDATE_SEED);
  }

  @Test
  public void checkCorruption() {
    final UpdateSketch sk = UpdateSketch.builder().build();
    sk.update(1);
    final MemorySegment wseg = MemorySegment.ofArray(sk.toByteArray());
    try {
      wseg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 2);
      Sketch.wrap(wseg, Util.DEFAULT_UPDATE_SEED);
      fail();
    } catch (final SketchesArgumentException e) { }
    try {
      wseg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 3);
      wseg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 2);
      Sketch.wrap(wseg, Util.DEFAULT_UPDATE_SEED);
      fail();
    } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkIsResizeFactorIncorrect() {
    final MemorySegment wseg = MemorySegment.ofArray(new byte[8]);
    insertLgNomLongs(wseg, 26);
    for (int lgK = 4; lgK <= 26; lgK++) {
      insertLgNomLongs(wseg, lgK);
      final int lgT = lgK + 1;
      for (int lgA = 5; lgA <= lgT; lgA++) {
        insertLgArrLongs(wseg, lgA);
        for (int lgR = 0; lgR <= 3; lgR++) {
          insertLgResizeFactor(wseg, lgR);
          final boolean lgRbad = isResizeFactorIncorrect(wseg, lgK, lgA);
          final boolean rf123 = lgR > 0 && (lgT - lgA) % lgR != 0;
          final boolean rf0 = lgR == 0 && lgA != lgT;
          assertTrue(lgRbad == rf0 || lgRbad == rf123);
        }
      }
    }
  }


  @SuppressWarnings("unused")
  @Test
  public void checkCompactOpsMemorySegmentToCompact() {
    MemorySegment skwseg, cskwseg1, cskwseg2, cskwseg3;
    CompactSketch csk1, csk2, csk3;
    final int lgK = 6;
    final UpdateSketch sk = UpdateSketch.builder().setLogNominalEntries(lgK).build();
    final int n = 1 << lgK + 1;
    for (int i = 2; i < n; i++) { sk.update(i); }
    final int cbytes = sk.getCompactBytes();
    final byte[] byteArr = sk.toByteArray();
    skwseg = MemorySegment.ofArray(byteArr);
    cskwseg1 = MemorySegment.ofArray(new byte[cbytes]);
    cskwseg2 = MemorySegment.ofArray(new byte[cbytes]);
    cskwseg3 = MemorySegment.ofArray(new byte[cbytes]);
    csk1 = sk.compact(true, cskwseg1);
    csk2 = CompactOperations.segmentToCompact(skwseg, true, cskwseg2);
    csk3 = CompactOperations.segmentToCompact(cskwseg1, true, cskwseg3);
    assertTrue(equalContents(cskwseg1,cskwseg2));
    assertTrue(equalContents(cskwseg1, cskwseg3));
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
