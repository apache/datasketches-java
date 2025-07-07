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
import static org.apache.datasketches.theta.BackwardConversions.convertSerVer3toSerVer1;
import static org.apache.datasketches.theta.BackwardConversions.convertSerVer3toSerVer2;
import static org.apache.datasketches.theta.CompactOperations.computeCompactPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static org.apache.datasketches.theta.Sketch.getMaxCompactSketchBytes;
import static org.apache.datasketches.common.Util.LONG_MAX_VALUE_AS_DOUBLE;
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
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.DirectCompactSketch;
import org.apache.datasketches.theta.PreambleUtil;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
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
    final UpdateSketch sketch = UpdateSketch.builder().setNominalEntries(k).build(); // QS Sketch
    assertEquals(sketch.getCurrentPreambleLongs(), lowQSPreLongs);
    assertEquals(sketch.getCompactPreambleLongs(), 1); //compact form
    assertEquals(sketch.getCurrentDataLongs(), k*2);
    assertEquals(sketch.getCurrentBytes(), (k*2*8) + (lowQSPreLongs << 3));
    assertEquals(sketch.getCompactBytes(), lowCompPreLongs << 3);

    final CompactSketch compSk = sketch.compact(false, null);
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
      final int maxCompBytes = Sketch.getMaxCompactSketchBytes(i);
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

    UpdateSketch sk1 = UpdateSketch.builder().setSeed(seed)
        .setP(p).setResizeFactor(rf).setFamily(fam).setNominalEntries(k).build();
    String nameS1 = sk1.getClass().getSimpleName();
    assertEquals(nameS1, "HeapAlphaSketch");
    assertEquals(sk1.getLgNomLongs(), lgK);
    assertEquals(sk1.getSeed(), seed);
    assertEquals(sk1.getP(), p);

    //check reset of defaults

    sk1 = UpdateSketch.builder().build();
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
    final UpdateSketch sk = UpdateSketch.builder().setNominalEntries(k).build();
    assertEquals(sk.getLgNomLongs(), 10);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBuilderIllegalP() {
    final float p = (float)1.5;
    UpdateSketch.builder().setP(p).build();
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
    final UpdateSketch sketch = UpdateSketch.builder().setFamily(Family.ALPHA).setNominalEntries(1024).build();
    final byte[] byteArr = sketch.toByteArray();
    final MemorySegment srcSeg = MemorySegment.ofArray(byteArr);
    Sketch.wrap(srcSeg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadFamily() {
    UpdateSketch.builder().setFamily(Family.INTERSECTION).setNominalEntries(1024).build();
  }

  @SuppressWarnings("static-access")
  @Test
  public void checkSerVer() {
    final UpdateSketch sketch = UpdateSketch.builder().setNominalEntries(1024).build();
    final byte[] sketchArray = sketch.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(sketchArray);
    int serVer = Sketch.getSerializationVersion(seg);
    assertEquals(serVer, 3);
    final MemorySegment wseg = MemorySegment.ofArray(sketchArray);
    final UpdateSketch sk2 = UpdateSketch.wrap(wseg);
    serVer = sk2.getSerializationVersion(wseg);
    assertEquals(serVer, 3);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyAlphaCompactExcep() {
    final int k = 512;
    final Sketch sketch1 = UpdateSketch.builder().setFamily(ALPHA).setNominalEntries(k).build();
    final byte[] byteArray = sketch1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //corrupt:
    Util.setBits(seg, FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    Sketch.heapify(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyQSCompactExcep() {
    final int k = 512;
    final Sketch sketch1 = UpdateSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    final byte[] byteArray = sketch1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //corrupt:
    Util.setBits(seg, FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    Sketch.heapify(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyNotCompactExcep() {
    final int k = 512;
    final UpdateSketch sketch1 = UpdateSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    final int bytes = Sketch.getMaxCompactSketchBytes(0);
    final byte[] byteArray = new byte[bytes];
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    sketch1.compact(false, seg);
    //corrupt:
    Util.clearBits(seg, FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    Sketch.heapify(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyFamilyExcep() {
    final int k = 512;
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion();
    final byte[] byteArray = union.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //Improper use
    Sketch.heapify(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapAlphaCompactExcep() {
    final int k = 512;
    final Sketch sketch1 = UpdateSketch.builder().setFamily(ALPHA).setNominalEntries(k).build();
    final byte[] byteArray = sketch1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //corrupt:
    Util.setBits(seg, FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    Sketch.wrap(seg);

  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapQSCompactExcep() {
    final int k = 512;
    final Sketch sketch1 = UpdateSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    final byte[] byteArray = sketch1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //corrupt:
    Util.setBits(seg, FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    Sketch.wrap(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapNotCompactExcep() {
    final int k = 512;
    final UpdateSketch sketch1 = UpdateSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    final int bytes = Sketch.getMaxCompactSketchBytes(0);
    final byte[] byteArray = new byte[bytes];
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    sketch1.compact(false, seg);
    //corrupt:
    Util.clearBits(seg, FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    Sketch.wrap(seg);
  }

  @Test
  public void checkValidSketchID() {
    assertFalse(Sketch.isValidSketchID(0));
    assertTrue(Sketch.isValidSketchID(ALPHA.getID()));
    assertTrue(Sketch.isValidSketchID(QUICKSELECT.getID()));
    assertTrue(Sketch.isValidSketchID(COMPACT.getID()));
  }

  @Test
  public void checkWrapToHeapifyConversion1() {
    final int k = 512;
    final UpdateSketch sketch1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < k; i++) {
      sketch1.update(i);
    }
    final double uest1 = sketch1.getEstimate();

    final CompactSketch csk = sketch1.compact();
    assertEquals(csk.getEstimate(), uest1);

    final MemorySegment v1seg = convertSerVer3toSerVer1(csk);
    Sketch csk2 = Sketch.wrap(v1seg); //fails
    assertFalse(csk2.isOffHeap());
    assertFalse(csk2.hasMemorySegment());
    assertEquals(uest1, csk2.getEstimate(), 0.0);

    final MemorySegment v2seg = convertSerVer3toSerVer2(csk, Util.DEFAULT_UPDATE_SEED);
    csk2 = Sketch.wrap(v2seg);
    assertFalse(csk2.isOffHeap());
    assertFalse(csk2.hasMemorySegment());
    assertEquals(uest1, csk2.getEstimate(), 0.0);
  }

  @Test
  public void checkIsSameResource() {
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[(k*16) + 24]); //280
    final MemorySegment cseg = MemorySegment.ofArray(new byte[32]);
    final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build(seg);
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
    final UpdateSketch sketch1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < (2*k); i++) { sketch1.update(i); }

    final double theta = sketch1.rebuild().getTheta();
    final long thetaLong = (long) (LONG_MAX_VALUE_AS_DOUBLE * theta);
    final int count = sketch1.getCountLessThanThetaLong(thetaLong);
    assertEquals(count, k);
  }

  private static MemorySegment createCompactSketchMemorySegment(final int k, final int u) {
    final UpdateSketch usk = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i = 0; i < u; i++) { usk.update(i); }
    final int bytes = Sketch.getMaxCompactSketchBytes(usk.getRetainedEntries(true));
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    usk.compact(true, wseg);
    return wseg;
  }

  @Test
  public void checkCompactFlagsOnWrap() {
    final MemorySegment wseg = createCompactSketchMemorySegment(16, 32);
    Sketch sk = Sketch.wrap(wseg);
    assertTrue(sk instanceof CompactSketch);
    final int flags = PreambleUtil.extractFlags(wseg);

    final int flagsNoCompact = flags & ~COMPACT_FLAG_MASK;
    PreambleUtil.insertFlags(wseg, flagsNoCompact);
    try {
      sk = Sketch.wrap(wseg);
      fail();
    } catch (final SketchesArgumentException e) { }

    final int flagsNoReadOnly = flags & ~READ_ONLY_FLAG_MASK;
    PreambleUtil.insertFlags(wseg, flagsNoReadOnly);
    try {
      sk = Sketch.wrap(wseg);
      fail();
    } catch (final SketchesArgumentException e) { }
    PreambleUtil.insertFlags(wseg, flags); //repair to original
    PreambleUtil.insertSerVer(wseg, 5);
    try {
      sk = Sketch.wrap(wseg);
      fail();
    } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkCompactSizeAndFlagsOnHeapify() {
    MemorySegment wseg = createCompactSketchMemorySegment(16, 32);
    Sketch sk = Sketch.heapify(wseg);
    assertTrue(sk instanceof CompactSketch);
    final int flags = PreambleUtil.extractFlags(wseg);

    final int flagsNoCompact = flags & ~READ_ONLY_FLAG_MASK;
    PreambleUtil.insertFlags(wseg, flagsNoCompact);
    try {
      sk = Sketch.heapify(wseg);
      fail();
    } catch (final SketchesArgumentException e) { }

    wseg = MemorySegment.ofArray(new byte[7]);
    PreambleUtil.insertSerVer(wseg, 3);
    //PreambleUtil.insertFamilyID(wseg, 3);
    try {
      sk = Sketch.heapify(wseg);
      fail();
    } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void check2Methods() {
    final int k = 16;
    final Sketch sk = Sketches.updateSketchBuilder().setNominalEntries(k).build();
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
