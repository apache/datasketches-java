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

import static org.apache.datasketches.Family.ALPHA;
import static org.apache.datasketches.Family.COMPACT;
import static org.apache.datasketches.Family.QUICKSELECT;
import static org.apache.datasketches.ResizeFactor.X1;
import static org.apache.datasketches.ResizeFactor.X2;
import static org.apache.datasketches.ResizeFactor.X4;
import static org.apache.datasketches.ResizeFactor.X8;
import static org.apache.datasketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.theta.BackwardConversions.convertSerVer3toSerVer1;
import static org.apache.datasketches.theta.BackwardConversions.convertSerVer3toSerVer2;
import static org.apache.datasketches.theta.CompactOperations.computeCompactPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static org.apache.datasketches.theta.Sketch.getMaxCompactSketchBytes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.Family;
import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings({"javadoc","deprecation"})
public class SketchTest {

  @Test
  public void checkGetMaxBytesWithEntries() {
    assertEquals(getMaxCompactSketchBytes(10), (10*8) + (Family.COMPACT.getMaxPreLongs() << 3) );
  }

  @Test
  public void checkGetCurrentBytes() {
    int k = 64;
    int lowQSPreLongs = Family.QUICKSELECT.getMinPreLongs();
    int lowCompPreLongs = Family.COMPACT.getMinPreLongs();
    UpdateSketch sketch = UpdateSketch.builder().setNominalEntries(k).build(); // QS Sketch
    assertEquals(sketch.getCurrentPreambleLongs(), lowQSPreLongs);
    assertEquals(sketch.getCompactPreambleLongs(), 1); //compact form
    assertEquals(sketch.getCurrentDataLongs(), k*2);
    assertEquals(sketch.getCurrentBytes(), (k*2*8) + (lowQSPreLongs << 3));
    assertEquals(sketch.getCompactBytes(), lowCompPreLongs << 3);

    CompactSketch compSk = sketch.compact(false, null);
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
    int curCount = sketch.getRetainedEntries(true);

    assertEquals(sketch.getCurrentPreambleLongs(), lowQSPreLongs);
    assertEquals(sketch.getCompactPreambleLongs(), 3); //compact form
    assertEquals(sketch.getCurrentDataLongs(), k*2);
    assertEquals(sketch.getCurrentBytes(), (k*2*8) + (lowQSPreLongs << 3));
    assertEquals(sketch.getCompactBytes(), (curCount*8) + (3*8)); //compact form

    compPreLongs = computeCompactPreLongs(sketch.isEmpty(), sketch.getRetainedEntries(true),
        sketch.getThetaLong());
    assertEquals(compPreLongs, 3);

    for (int i=0; i<3; i++) {
      int maxCompBytes = Sketch.getMaxCompactSketchBytes(i);
      if (i == 0) { assertEquals(maxCompBytes,  8); }
      if (i == 1) { assertEquals(maxCompBytes, 16); }
      if (i > 1) { assertEquals(maxCompBytes, 24 + (i * 8)); } //assumes maybe estimation mode
    }
  }

  @Test
  public void checkBuilder() {
    int k = 2048;
    int lgK = Integer.numberOfTrailingZeros(k);
    long seed = 1021;
    float p = (float)0.5;
    ResizeFactor rf = X4;
    Family fam = Family.ALPHA;

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
    assertEquals(sk1.getLgNomLongs(), Integer.numberOfTrailingZeros(DEFAULT_NOMINAL_ENTRIES));
    assertEquals(sk1.getSeed(), DEFAULT_UPDATE_SEED);
    assertEquals(sk1.getP(), (float)1.0);
    assertEquals(sk1.getResizeFactor(), ResizeFactor.X8);
  }

  @Test
  public void checkBuilderNonPowerOf2() {
    int k = 1000;
    UpdateSketch sk = UpdateSketch.builder().setNominalEntries(k).build();
    assertEquals(sk.getLgNomLongs(), 10);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBuilderIllegalP() {
    float p = (float)1.5;
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
    UpdateSketch sketch = UpdateSketch.builder().setFamily(Family.ALPHA).setNominalEntries(1024).build();
    byte[] byteArr = sketch.toByteArray();
    Memory srcMem = Memory.wrap(byteArr);
    Sketch.wrap(srcMem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadFamily() {
    UpdateSketch.builder().setFamily(Family.INTERSECTION).setNominalEntries(1024).build();
  }

  @SuppressWarnings("static-access")
  @Test
  public void checkSerVer() {
    UpdateSketch sketch = UpdateSketch.builder().setNominalEntries(1024).build();
    byte[] sketchArray = sketch.toByteArray();
    Memory mem = Memory.wrap(sketchArray);
    int serVer = Sketch.getSerializationVersion(mem);
    assertEquals(serVer, 3);
    WritableMemory wmem = WritableMemory.wrap(sketchArray);
    UpdateSketch sk2 = UpdateSketch.wrap(wmem);
    serVer = sk2.getSerializationVersion(wmem);
    assertEquals(serVer, 3);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyAlphaCompactExcep() {
    int k = 512;
    Sketch sketch1 = UpdateSketch.builder().setFamily(ALPHA).setNominalEntries(k).build();
    byte[] byteArray = sketch1.toByteArray();
    WritableMemory mem = WritableMemory.wrap(byteArray);
    //corrupt:
    mem.setBits(FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    Sketch.heapify(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyQSCompactExcep() {
    int k = 512;
    Sketch sketch1 = UpdateSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    byte[] byteArray = sketch1.toByteArray();
    WritableMemory mem = WritableMemory.wrap(byteArray);
    //corrupt:
    mem.setBits(FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    Sketch.heapify(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyNotCompactExcep() {
    int k = 512;
    UpdateSketch sketch1 = UpdateSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    int bytes = Sketch.getMaxCompactSketchBytes(0);
    byte[] byteArray = new byte[bytes];
    WritableMemory mem = WritableMemory.wrap(byteArray);
    sketch1.compact(false, mem);
    //corrupt:
    mem.clearBits(FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    Sketch.heapify(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyFamilyExcep() {
    int k = 512;
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();
    byte[] byteArray = union.toByteArray();
    Memory mem = Memory.wrap(byteArray);
    //Improper use
    Sketch.heapify(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapAlphaCompactExcep() {
    int k = 512;
    Sketch sketch1 = UpdateSketch.builder().setFamily(ALPHA).setNominalEntries(k).build();
    byte[] byteArray = sketch1.toByteArray();
    WritableMemory mem = WritableMemory.wrap(byteArray);
    //corrupt:
    mem.setBits(FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    Sketch.wrap(mem);

  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapQSCompactExcep() {
    int k = 512;
    Sketch sketch1 = UpdateSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    byte[] byteArray = sketch1.toByteArray();
    WritableMemory mem = WritableMemory.wrap(byteArray);
    //corrupt:
    mem.setBits(FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    Sketch.wrap(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapNotCompactExcep() {
    int k = 512;
    UpdateSketch sketch1 = UpdateSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    int bytes = Sketch.getMaxCompactSketchBytes(0);
    byte[] byteArray = new byte[bytes];
    WritableMemory mem = WritableMemory.wrap(byteArray);
    sketch1.compact(false, mem);
    //corrupt:
    mem.clearBits(FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    Sketch.wrap(mem);
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
    int k = 512;
    UpdateSketch sketch1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      sketch1.update(i);
    }
    double uest1 = sketch1.getEstimate();

    CompactSketch csk = sketch1.compact();

    Memory v1mem = convertSerVer3toSerVer1(csk);
    Sketch csk2 = Sketch.wrap(v1mem);
    assertFalse(csk2.isDirect());
    assertFalse(csk2.hasMemory());
    assertEquals(uest1, csk2.getEstimate(), 0.0);

    Memory v2mem = convertSerVer3toSerVer2(csk, Util.DEFAULT_UPDATE_SEED);
    csk2 = Sketch.wrap(v2mem);
    assertFalse(csk2.isDirect());
    assertFalse(csk2.hasMemory());
    assertEquals(uest1, csk2.getEstimate(), 0.0);
  }

  @Test
  public void checkIsSameResource() {
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*16) + 24]);
    WritableMemory cmem = WritableMemory.wrap(new byte[32]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build(mem);
    sketch.update(1);
    sketch.update(2);
    assertTrue(sketch.isSameResource(mem));
    DirectCompactSketch dcos = (DirectCompactSketch) sketch.compact(true, cmem);
    assertTrue(dcos.isSameResource(cmem));
    assertTrue(dcos.isOrdered());
    //never create 2 sketches with the same memory, so don't do as I do :)
    DirectCompactSketch dcs = (DirectCompactSketch) sketch.compact(false, cmem);
    assertTrue(dcs.isSameResource(cmem));
    assertFalse(dcs.isOrdered());

    Sketch sk = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    assertFalse(sk.isSameResource(mem));
  }

  @Test
  public void checkCountLessThanTheta() {
    int k = 512;
    UpdateSketch sketch1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < (2*k); i++) { sketch1.update(i); }

    double theta = sketch1.rebuild().getTheta();
    int count = sketch1.getCountLessThanTheta(theta);
    assertEquals(count, k);
  }

  private static WritableMemory createCompactSketchMemory(int k, int u) {
    UpdateSketch usk = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i = 0; i < u; i++) { usk.update(i); }
    int bytes = Sketch.getMaxCompactSketchBytes(usk.getRetainedEntries(true));
    WritableMemory wmem = WritableMemory.allocate(bytes);
    usk.compact(true, wmem);
    return wmem;
  }

  @Test
  public void checkCompactFlagsOnWrap() {
    WritableMemory wmem = createCompactSketchMemory(16, 32);
    Sketch sk = Sketch.wrap(wmem);
    assertTrue(sk instanceof CompactSketch);
    int flags = PreambleUtil.extractFlags(wmem);

    int flagsNoCompact = flags & ~COMPACT_FLAG_MASK;
    PreambleUtil.insertFlags(wmem, flagsNoCompact);
    try {
      sk = Sketch.wrap(wmem);
      fail();
    } catch (SketchesArgumentException e) { }

    int flagsNoReadOnly = flags & ~READ_ONLY_FLAG_MASK;
    PreambleUtil.insertFlags(wmem, flagsNoReadOnly);
    try {
      sk = Sketch.wrap(wmem);
      fail();
    } catch (SketchesArgumentException e) { }
    PreambleUtil.insertFlags(wmem, flags); //repair to original
    PreambleUtil.insertSerVer(wmem, 5);
    try {
      sk = Sketch.wrap(wmem);
      fail();
    } catch (SketchesArgumentException e) { }
  }

  @Test
  public void checkCompactSizeAndFlagsOnHeapify() {
    WritableMemory wmem = createCompactSketchMemory(16, 32);
    Sketch sk = Sketch.heapify(wmem);
    assertTrue(sk instanceof CompactSketch);
    int flags = PreambleUtil.extractFlags(wmem);

    int flagsNoCompact = flags & ~READ_ONLY_FLAG_MASK;
    PreambleUtil.insertFlags(wmem, flagsNoCompact);
    try {
      sk = Sketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { }

    wmem = WritableMemory.allocate(7);
    PreambleUtil.insertSerVer(wmem, 3);
    //PreambleUtil.insertFamilyID(wmem, 3);
    try {
      sk = Sketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { }
  }

  @Test
  public void check2Methods() {
    int k = 16;
    Sketch sk = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    int bytes1 = sk.getCurrentBytes(true);
    int bytes2 = sk.getCurrentBytes(false);
    assertEquals(bytes1, 8);
    assertEquals(bytes2, 280); //32*8 + 24
    int retEnt = sk.getRetainedEntries();
    assertEquals(retEnt, 0);
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
