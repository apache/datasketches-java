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

import static org.apache.datasketches.common.Family.ALPHA;
import static org.apache.datasketches.common.Family.COMPACT;
import static org.apache.datasketches.common.Family.QUICKSELECT;
import static org.apache.datasketches.common.ResizeFactor.X1;
import static org.apache.datasketches.common.ResizeFactor.X2;
import static org.apache.datasketches.common.ResizeFactor.X4;
import static org.apache.datasketches.common.ResizeFactor.X8;
import static org.apache.datasketches.common.Util.LONG_MAX_VALUE_AS_DOUBLE;
import static org.apache.datasketches.theta.CompactOperations.computeCompactPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static org.apache.datasketches.theta.ThetaSketch.getMaxCompactSketchBytes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class SketchTest {

  @Test
  public void checkGetMaxBytesWithEntries() {
    assertEquals(getMaxCompactSketchBytes(10), (10*8) + (Family.COMPACT.getMaxPreLongs() << 3) );
  }

  @Test
  public void checkGetCurrentBytes() {
    final int k = 64;
    final int lowQSPreLongs = Family.QUICKSELECT.getMinPreLongs();
    final int lowCompPreLongs = Family.COMPACT.getMinPreLongs();
    final UpdatableThetaSketch sketch = UpdatableThetaSketch.builder().setNominalEntries(k).build(); // QuickSelectThetaSketch
    assertEquals(sketch.getCurrentPreambleLongs(), lowQSPreLongs);
    assertEquals(sketch.getCompactPreambleLongs(), 1); //compact form
    assertEquals(sketch.getCurrentDataLongs(), k*2);
    assertEquals(sketch.getCurrentBytes(), (k*2*8) + (lowQSPreLongs << 3));
    assertEquals(sketch.getCompactBytes(), lowCompPreLongs << 3);

    final CompactThetaSketch compSk = sketch.compact(false, null);
    assertEquals(compSk.getCompactBytes(), 8);
    assertEquals(compSk.getCurrentBytes(), 8);
    assertEquals(compSk.getCurrentDataLongs(), 0);

    int compPreLongs = computeCompactPreLongs(sketch.isEmpty(), sketch.getRetainedEntries(true),
        sketch.getThetaLong());
    assertEquals(compPreLongs, 1);

    for (int i=0; i<k; i++) {
      sketch.update(i);
    }

    assertEquals(sketch.getCurrentPreambleLongs(), lowQSPreLongs);
    assertEquals(sketch.getCompactPreambleLongs(), 2); //compact form
    assertEquals(sketch.getCurrentDataLongs(), k*2);
    assertEquals(sketch.getCurrentBytes(), (k*2*8) + (lowQSPreLongs << 3));
    assertEquals(sketch.getCompactBytes(), (k*8) + (2*8)); //compact form  //FAILS HERE

    compPreLongs = computeCompactPreLongs(sketch.isEmpty(), sketch.getRetainedEntries(true),
        sketch.getThetaLong());
    assertEquals(compPreLongs, 2);

    for (int i = k; i < (2*k); i++) {
      sketch.update(i); //go estimation mode
    }
    final int curCount = sketch.getRetainedEntries(true);

    assertEquals(sketch.getCurrentPreambleLongs(), lowQSPreLongs);
    assertEquals(sketch.getCompactPreambleLongs(), 3); //compact form
    assertEquals(sketch.getCurrentDataLongs(), k*2);
    assertEquals(sketch.getCurrentBytes(), (k*2*8) + (lowQSPreLongs << 3));
    assertEquals(sketch.getCompactBytes(), (curCount*8) + (3*8)); //compact form

    compPreLongs = computeCompactPreLongs(sketch.isEmpty(), sketch.getRetainedEntries(true),
        sketch.getThetaLong());
    assertEquals(compPreLongs, 3);

    for (int i=0; i<3; i++) {
      final int maxCompBytes = ThetaSketch.getMaxCompactSketchBytes(i);
      if (i == 0) { assertEquals(maxCompBytes,  8); }
      if (i == 1) { assertEquals(maxCompBytes, 16); }
      if (i > 1) { assertEquals(maxCompBytes, 24 + (i * 8)); } //assumes maybe estimation mode
    }
  }

  @Test
  public void checkBuilder() {
    final int k = 2048;
    final int lgK = Integer.numberOfTrailingZeros(k);
    final long seed = 1021;
    final float p = (float)0.5;
    final ResizeFactor rf = X4;
    final Family fam = Family.ALPHA;

    UpdatableThetaSketch sk1 = UpdatableThetaSketch.builder().setSeed(seed)
        .setP(p).setResizeFactor(rf).setFamily(fam).setNominalEntries(k).build();
    String nameS1 = sk1.getClass().getSimpleName();
    assertEquals(nameS1, "HeapAlphaSketch");
    assertEquals(sk1.getLgNomLongs(), lgK);
    assertEquals(sk1.getSeed(), seed);
    assertEquals(sk1.getP(), p);

    //check reset of defaults

    sk1 = UpdatableThetaSketch.builder().build();
    nameS1 = sk1.getClass().getSimpleName();
    assertEquals(nameS1, "HeapQuickSelectSketch");
    assertEquals(sk1.getLgNomLongs(), Integer.numberOfTrailingZeros(ThetaUtil.DEFAULT_NOMINAL_ENTRIES));
    assertEquals(sk1.getSeed(), Util.DEFAULT_UPDATE_SEED);
    assertEquals(sk1.getP(), (float)1.0);
    assertEquals(sk1.getResizeFactor(), ResizeFactor.X8);
  }

  @Test
  public void checkBuilderNonPowerOf2() {
    final int k = 1000;
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    assertEquals(sk.getLgNomLongs(), 10);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBuilderIllegalP() {
    final float p = (float)1.5;
    UpdatableThetaSketch.builder().setP(p).build();
  }

  @Test
  public void checkBuilderResizeFactor() {
    ResizeFactor rf;
    rf = X1;
    assertEquals(rf.getValue(), 1);
    assertEquals(rf.lg(), 0);
    assertEquals(ResizeFactor.getRF(0), X1);
    rf = X2;
    assertEquals(rf.getValue(), 2);
    assertEquals(rf.lg(), 1);
    assertEquals(ResizeFactor.getRF(1), X2);
    rf = X4;
    assertEquals(rf.getValue(), 4);
    assertEquals(rf.lg(), 2);
    assertEquals(ResizeFactor.getRF(2), X4);
    rf = X8;
    assertEquals(rf.getValue(), 8);
    assertEquals(rf.lg(), 3);
    assertEquals(ResizeFactor.getRF(3), X8);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapBadFamily() {
    final UpdatableThetaSketch sketch = UpdatableThetaSketch.builder().setFamily(Family.ALPHA).setNominalEntries(1024).build();
    final byte[] byteArr = sketch.toByteArray();
    final MemorySegment srcSeg = MemorySegment.ofArray(byteArr);
    ThetaSketch.wrap(srcSeg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadFamily() {
    UpdatableThetaSketch.builder().setFamily(Family.INTERSECTION).setNominalEntries(1024).build();
  }

  @SuppressWarnings("static-access")
  @Test
  public void checkSerVer() {
    final UpdatableThetaSketch sketch = UpdatableThetaSketch.builder().setNominalEntries(1024).build();
    final byte[] sketchArray = sketch.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(sketchArray);
    int serVer = ThetaSketch.getSerializationVersion(seg);
    assertEquals(serVer, 3);
    final MemorySegment wseg = MemorySegment.ofArray(sketchArray);
    final UpdatableThetaSketch sk2 = UpdatableThetaSketch.wrap(wseg);
    serVer = sk2.getSerializationVersion(wseg);
    assertEquals(serVer, 3);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyAlphaCompactExcep() {
    final int k = 512;
    final ThetaSketch sketch1 = UpdatableThetaSketch.builder().setFamily(ALPHA).setNominalEntries(k).build();
    final byte[] byteArray = sketch1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //corrupt:
    Util.setBits(seg, FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    ThetaSketch.heapify(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyQSCompactExcep() {
    final int k = 512;
    final ThetaSketch sketch1 = UpdatableThetaSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    final byte[] byteArray = sketch1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //corrupt:
    Util.setBits(seg, FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    ThetaSketch.heapify(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyNotCompactExcep() {
    final int k = 512;
    final UpdatableThetaSketch sketch1 = UpdatableThetaSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    final int bytes = ThetaSketch.getMaxCompactSketchBytes(0);
    final byte[] byteArray = new byte[bytes];
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    sketch1.compact(false, seg);
    //corrupt:
    Util.clearBits(seg, FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    ThetaSketch.heapify(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyFamilyExcep() {
    final int k = 512;
    final ThetaUnion union = ThetaSetOperation.builder().setNominalEntries(k).buildUnion();
    final byte[] byteArray = union.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //Improper use
    ThetaSketch.heapify(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapAlphaCompactExcep() {
    final int k = 512;
    final ThetaSketch sketch1 = UpdatableThetaSketch.builder().setFamily(ALPHA).setNominalEntries(k).build();
    final byte[] byteArray = sketch1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //corrupt:
    Util.setBits(seg, FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    ThetaSketch.wrap(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapQSCompactExcep() {
    final int k = 512;
    final ThetaSketch sketch1 = UpdatableThetaSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    final byte[] byteArray = sketch1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //corrupt:
    Util.setBits(seg, FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    ThetaSketch.wrap(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapNotCompactExcep() {
    final int k = 512;
    final UpdatableThetaSketch sketch1 = UpdatableThetaSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    final int bytes = ThetaSketch.getMaxCompactSketchBytes(0);
    final byte[] byteArray = new byte[bytes];
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    sketch1.compact(false, seg);
    //corrupt:
    Util.clearBits(seg, FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    ThetaSketch.wrap(seg);
  }

  @Test
  public void checkValidSketchID() {
    assertFalse(ThetaSketch.isValidSketchID(0));
    assertTrue(ThetaSketch.isValidSketchID(ALPHA.getID()));
    assertTrue(ThetaSketch.isValidSketchID(QUICKSELECT.getID()));
    assertTrue(ThetaSketch.isValidSketchID(COMPACT.getID()));
  }

  @Test
  public void checkIsSameResource() {
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[(k*16) + 24]); //280
    final MemorySegment cseg = MemorySegment.ofArray(new byte[32]);
    final UpdatableThetaSketch sketch = UpdatableThetaSketch.builder().setNominalEntries(k).build(seg);
    sketch.update(1);
    sketch.update(2);
    assertTrue(sketch.isSameResource(seg));
    final DirectCompactSketch dcos = (DirectCompactSketch) sketch.compact(true, cseg);
    assertTrue(MemorySegmentStatus.isSameResource(dcos.getMemorySegment(), cseg));
    assertTrue(dcos.isOrdered());
    //never create 2 sketches with the same MemorySegment, so don't do as I do :)
    final DirectCompactSketch dcs = (DirectCompactSketch) sketch.compact(false, cseg);
    assertTrue(MemorySegmentStatus.isSameResource(dcs.getMemorySegment(), cseg));
    assertFalse(dcs.isOrdered());
  }

  @Test
  public void checkCountLessThanTheta() {
    final int k = 512;
    final UpdatableThetaSketch sketch1 = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < (2*k); i++) { sketch1.update(i); }

    final double theta = sketch1.rebuild().getTheta();
    final long thetaLong = (long) (LONG_MAX_VALUE_AS_DOUBLE * theta);
    final int count = sketch1.getCountLessThanThetaLong(thetaLong);
    assertEquals(count, k);
  }

  private static MemorySegment createCompactSketchMemorySegment(final int k, final int u) {
    final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < u; i++) { usk.update(i); }
    final int bytes = ThetaSketch.getMaxCompactSketchBytes(usk.getRetainedEntries(true));
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    usk.compact(true, wseg);
    return wseg;
  }

  @Test
  public void checkCompactFlagsOnWrap() {
    final MemorySegment wseg = createCompactSketchMemorySegment(16, 32);
    ThetaSketch sk = ThetaSketch.wrap(wseg);
    assertTrue(sk instanceof CompactThetaSketch);
    final int flags = PreambleUtil.extractFlags(wseg);

    final int flagsNoCompact = flags & ~COMPACT_FLAG_MASK;
    PreambleUtil.insertFlags(wseg, flagsNoCompact);
    try {
      sk = ThetaSketch.wrap(wseg);
      fail();
    } catch (final SketchesArgumentException e) { }

    final int flagsNoReadOnly = flags & ~READ_ONLY_FLAG_MASK;
    PreambleUtil.insertFlags(wseg, flagsNoReadOnly);
    try {
      sk = ThetaSketch.wrap(wseg);
      fail();
    } catch (final SketchesArgumentException e) { }
    PreambleUtil.insertFlags(wseg, flags); //repair to original
    PreambleUtil.insertSerVer(wseg, 5);
    try {
      sk = ThetaSketch.wrap(wseg);
      fail();
    } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkCompactSizeAndFlagsOnHeapify() {
    MemorySegment wseg = createCompactSketchMemorySegment(16, 32);
    ThetaSketch sk = ThetaSketch.heapify(wseg);
    assertTrue(sk instanceof CompactThetaSketch);
    final int flags = PreambleUtil.extractFlags(wseg);

    final int flagsNoCompact = flags & ~READ_ONLY_FLAG_MASK;
    PreambleUtil.insertFlags(wseg, flagsNoCompact);
    try {
      sk = ThetaSketch.heapify(wseg);
      fail();
    } catch (final SketchesArgumentException e) { }

    wseg = MemorySegment.ofArray(new byte[7]);
    PreambleUtil.insertSerVer(wseg, 3);
    //PreambleUtil.insertFamilyID(wseg, 3);
    try {
      sk = ThetaSketch.heapify(wseg);
      fail();
    } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void check2Methods() {
    final int k = 16;
    final ThetaSketch sk = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    final int bytes1 = sk.getCompactBytes();
    final int bytes2 = sk.getCurrentBytes();
    assertEquals(bytes1, 8);
    assertEquals(bytes2, 280); //32*8 + 24
    final int retEnt = sk.getRetainedEntries();
    assertEquals(retEnt, 0);
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
