/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Family.ALPHA;
import static com.yahoo.sketches.Family.COMPACT;
import static com.yahoo.sketches.Family.QUICKSELECT;
import static com.yahoo.sketches.ResizeFactor.X1;
import static com.yahoo.sketches.ResizeFactor.X2;
import static com.yahoo.sketches.ResizeFactor.X4;
import static com.yahoo.sketches.ResizeFactor.X8;
import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.Sketch.getMaxCompactSketchBytes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;

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
    int k = 64;
    int lowQSPreLongs = Family.QUICKSELECT.getMinPreLongs();
    int lowCompPreLongs = Family.COMPACT.getMinPreLongs();
    UpdateSketch sketch = UpdateSketch.builder().setNominalEntries(k).build(); // QS Sketch
    assertEquals(sketch.getCurrentPreambleLongs(false), lowQSPreLongs);
    assertEquals(sketch.getCurrentPreambleLongs(true), 1); //compact form
    assertEquals(sketch.getCurrentDataLongs(false), k*2);
    assertEquals(sketch.getCurrentDataLongs(true), 0); //compact form
    assertEquals(sketch.getCurrentBytes(false), (k*2*8) + (lowQSPreLongs << 3));
    assertEquals(sketch.getCurrentBytes(true), lowCompPreLongs << 3);

    CompactSketch compSk = sketch.compact(false, null);
    assertEquals(compSk.getCurrentBytes(true), 8);
    assertEquals(compSk.getCurrentBytes(false), 8);

    int compPreLongs = Sketch.computeCompactPreLongs(sketch.getThetaLong(), sketch.isEmpty(),
        sketch.getRetainedEntries(true));
    assertEquals(compPreLongs, 1);

    for (int i=0; i<k; i++) {
      sketch.update(i);
    }

    assertEquals(sketch.getCurrentPreambleLongs(false), lowQSPreLongs);
    assertEquals(sketch.getCurrentPreambleLongs(true), 2); //compact form
    assertEquals(sketch.getCurrentDataLongs(false), k*2);
    assertEquals(sketch.getCurrentDataLongs(true), k); //compact form
    assertEquals(sketch.getCurrentBytes(false), (k*2*8) + (lowQSPreLongs << 3));
    assertEquals(sketch.getCurrentBytes(true), (k*8) + (2*8)); //compact form  //FAILS HERE

    compPreLongs = Sketch.computeCompactPreLongs(sketch.getThetaLong(), sketch.isEmpty(),
        sketch.getRetainedEntries(true));
    assertEquals(compPreLongs, 2);

    for (int i=k; i<(2*k); i++)
     {
      sketch.update(i); //go estimation mode
    }
    int curCount = sketch.getRetainedEntries(true);

    assertEquals(sketch.getCurrentPreambleLongs(false), lowQSPreLongs);
    assertEquals(sketch.getCurrentPreambleLongs(true), 3); //compact form
    assertEquals(sketch.getCurrentDataLongs(false), k*2);
    assertEquals(sketch.getCurrentDataLongs(true), curCount); //compact form
    assertEquals(sketch.getCurrentBytes(false), (k*2*8) + (lowQSPreLongs << 3));
    assertEquals(sketch.getCurrentBytes(true), (curCount*8) + (3*8)); //compact form

    compPreLongs = Sketch.computeCompactPreLongs(sketch.getThetaLong(), sketch.isEmpty(),
        sketch.getRetainedEntries(true));
    assertEquals(compPreLongs, 3);

    for (int i=0; i<3; i++) {
      int maxCompBytes = Sketch.getMaxCompactSketchBytes(i);
      assertEquals(maxCompBytes, (Family.COMPACT.getMaxPreLongs() << 3) + (i*8));
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
    int bytes = sketch1.getCurrentBytes(true);
    WritableMemory v3mem = WritableMemory.wrap(new byte[bytes]);
    sketch1.compact(true, v3mem);

    Memory v1mem = ForwardCompatibilityTest.convertSerV3toSerV1(v3mem);
    Sketch csk2 = Sketch.wrap(v1mem);
    assertFalse(csk2.isDirect());
    assertEquals(uest1, csk2.getEstimate(), 0.0);

    Memory v2mem = ForwardCompatibilityTest.convertSerV3toSerV2(v3mem);
    csk2 = Sketch.wrap(v2mem);
    assertFalse(csk2.isDirect());
    assertEquals(uest1, csk2.getEstimate(), 0.0);
  }

  @Test
  public void checkIsSameResource() {
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*16) +24]);
    WritableMemory cmem = WritableMemory.wrap(new byte[8]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build(mem);
    assertTrue(sketch.isSameResource(mem));
    DirectCompactOrderedSketch dcos = (DirectCompactOrderedSketch) sketch.compact(true, cmem);
    assertTrue(dcos.isSameResource(cmem));
    //never create 2 sketches with the same memory, so don't do as I do :)
    DirectCompactUnorderedSketch dcs = (DirectCompactUnorderedSketch) sketch.compact(false, cmem);
    assertTrue(dcs.isSameResource(cmem));

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
