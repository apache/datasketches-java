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

import static org.apache.datasketches.HashOperations.minLgHashTableSize;
import static org.apache.datasketches.ResizeFactor.X4;
import static org.apache.datasketches.Util.REBUILD_THRESHOLD;
import static org.apache.datasketches.theta.Sketch.getMaxUpdateSketchBytes;
import static org.testng.Assert.assertEquals;
//import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.datasketches.Family;
import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class SetOperationTest {

  @Test
  public void checkBuilder() {
    final int k = 2048;
    final long seed = 1021;

    final UpdateSketch usk1 = UpdateSketch.builder().setSeed(seed).setNominalEntries(k).build();
    final UpdateSketch usk2 = UpdateSketch.builder().setSeed(seed).setNominalEntries(k).build();

    for (int i=0; i<k/2; i++) {
      usk1.update(i); //256
    }
    for (int i=k/2; i<k; i++) {
      usk2.update(i); //256 no overlap
    }

    final ResizeFactor rf = X4;
    //use default size
    final Union union = SetOperation.builder().setSeed(seed).setResizeFactor(rf).buildUnion();

    union.union(usk1);
    union.union(usk2);

    final double exactUnionAnswer = k;

    final CompactSketch comp1 = union.getResult(false, null); //ordered: false
    final double compEst = comp1.getEstimate();
    assertEquals(compEst, exactUnionAnswer, 0.0);
  }

  @Test
  public void checkBuilder2() {
    final SetOperationBuilder bldr = SetOperation.builder();

    final long seed = 12345L;
    bldr.setSeed(seed);
    assertEquals(bldr.getSeed(), seed);

    final float p = (float)0.5;
    bldr.setP(p);
    assertEquals(bldr.getP(), p);

    final ResizeFactor rf = ResizeFactor.X4;
    bldr.setResizeFactor(rf);
    assertEquals(bldr.getResizeFactor(), rf);

    final int lgK = 10;
    final int k = 1 << lgK;
    bldr.setNominalEntries(k);
    assertEquals(bldr.getLgNominalEntries(), lgK);

    final MemoryRequestServer mrs = new DefaultMemoryRequestServer();
    bldr.setMemoryRequestServer(mrs);
    assertEquals(bldr.getMemoryRequestServer(), mrs);

    println(bldr.toString());
  }

  @Test
  public void checkBuilderNonPowerOf2() {
    SetOperation.builder().setNominalEntries(1000).buildUnion();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBuilderBadFamily() {
    SetOperation.builder().build(Family.ALPHA);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBuilderIllegalPhi() {
    final float p = (float)1.5;
    SetOperation.builder().setP(p).buildUnion();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBuilderIllegalPlo() {
    final float p = 0;
    SetOperation.builder().setP(p).buildUnion();
  }

  @Test
  public void checkBuilderValidP() {
    final float p = (float).5;
    SetOperation.builder().setP(p).buildUnion();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBuilderAnotB_noMem() {
    final WritableMemory mem = WritableMemory.writableWrap(new byte[64]);
    SetOperation.builder().build(Family.A_NOT_B, mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBuilderBadSeedHashes() {
    final int k = 2048;
    final long seed = 1021;

    final UpdateSketch usk1 = UpdateSketch.builder().setSeed(seed).setNominalEntries(k).build();
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<k/2; i++) {
      usk1.update(i); //256
    }
    for (int i=k/2; i<k; i++) {
      usk2.update(i); //256 no overlap
    }

    final ResizeFactor rf = X4;

    final Union union = SetOperation.builder().setSeed(seed).setResizeFactor(rf).setNominalEntries(k).buildUnion();

    union.union(usk1);
    union.union(usk2); //throws seed exception here
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBuilderNomEntries() {
    final int k = 1 << 27;
    final SetOperationBuilder bldr = SetOperation.builder();
    bldr.setNominalEntries(k);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIllegalSetOpHeapify() {
    final int k = 64;
    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      usk1.update(i); //64
    }
    final byte[] byteArray = usk1.toByteArray();
    final Memory mem = Memory.wrap(byteArray);
    SetOperation.heapify(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIllegalSetOpWrap() {
    final int k = 64;
    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      usk1.update(i); //64
    }
    final byte[] byteArray = usk1.toByteArray();
    final Memory mem = Memory.wrap(byteArray);
    Sketches.wrapIntersection(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIllegalSetOpWrap2() {
    final int k = 64;
    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      usk1.update(i); //64
    }
    final WritableMemory wmem = WritableMemory.writableWrap(usk1.toByteArray());
    PreambleUtil.insertSerVer(wmem, 2); //corrupt
    final Memory mem = wmem;
    SetOperation.wrap(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIllegalSetOpWrap3() {
    final int k = 64;
    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      usk1.update(i); //64
    }
    final WritableMemory wmem = WritableMemory.writableWrap(usk1.toByteArray());
    SetOperation.wrap(wmem);
  }

  @Test
  public void checkBuildSetOps() {
    final SetOperationBuilder bldr = Sketches.setOperationBuilder();
    bldr.buildUnion();
    bldr.buildIntersection();
    bldr.buildANotB();
  }

  @Test
  public void checkComputeLgArrLongs() {
    assertEquals(minLgHashTableSize(30, REBUILD_THRESHOLD), 5);
    assertEquals(minLgHashTableSize(31, REBUILD_THRESHOLD), 6);
  }

  /**
   * The objective is to union 3 16K sketches into a union SetOperation and get the result.
   * All operations are to be performed within a single direct ByteBuffer as the backing store.
   * First we will make the union size large enough so that its answer will be exact (with this
   * specific example).
   * <p> Next, we recover the Union SetOp and the 3 sketches and the space for the result. Then
   * recompute the union using a Union of the same size as the input sketches, where the end result
   * will be an estimate.
   */
  @Test
  public void checkDirectUnionExample() {
    //The first task is to compute how much direct memory we need and set the heap large enough.
    //For the first trial, we will set the Union large enough for an exact result for THIS example.
    final int sketchNomEntries = 1 << 14; //16K
    int unionNomEntries = 1 << 15;  //32K
    final int[] heapLayout = getHeapLayout(sketchNomEntries, unionNomEntries);

    //This BB belongs to you and you always retain a link to it until you are completely
    // done and then let java garbage collect it.
    //I use a heap backing array, because for this example it is easier to peak into it and
    // see what is going on.
    final byte[] backingArr = new byte[heapLayout[5]];
    final ByteBuffer heapBuf = ByteBuffer.wrap(backingArr).order(ByteOrder.nativeOrder());

    // Attaches a WritableMemory object to the underlying memory of heapBuf.
    // heapMem will have a Read/Write view of the complete backing memory of heapBuf (direct or not).
    // Any R/W action from heapMem will be visible via heapBuf and visa versa.
    //
    // However, if you had created this WM object directly in raw, off-heap "native" memory
    // you would have the responsibility to close it when you are done.
    // But, since it was allocated via BB, it closes it for you.
    final WritableMemory heapMem = WritableMemory.writableWrap(heapBuf);

    double result = directUnionTrial1(heapMem, heapLayout, sketchNomEntries, unionNomEntries);
    println("1st est: "+result);
    final int expected = sketchNomEntries*2;
    assertEquals(result, expected, 0.0); //est must be exact.

    //For trial 2, we will use the same union space but use only part of it.
    unionNomEntries = 1 << 14; //16K
    result = directUnionTrial2(heapMem, heapLayout, sketchNomEntries, unionNomEntries);

    //intentionally loose bounds
    assertEquals(result, expected, expected*0.05);
    println("2nd est: "+result);
    println("Error %: "+(result/expected -1.0)*100);
  }

  @Test
  public void setOpsExample() {
    println("Set Operations Example:");
    final int k = 4096;
    final UpdateSketch skA = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    final UpdateSketch skB = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    final UpdateSketch skC = Sketches.updateSketchBuilder().setNominalEntries(k).build();

    for (int i=1;  i<=10; i++) { skA.update(i); }
    for (int i=1;  i<=20; i++) { skB.update(i); }
    for (int i=6;  i<=15; i++) { skC.update(i); } //overlapping set

    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.union(skA);
    union.union(skB);
    // ... continue to iterate on the input sketches to union

    final CompactSketch unionSk = union.getResult();   //the result union sketch
    println("A U B      : "+unionSk.getEstimate());   //the estimate of the union

    //Intersection is similar

    final Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.intersect(unionSk);
    inter.intersect(skC);
    // ... continue to iterate on the input sketches to intersect

    final CompactSketch interSk = inter.getResult();  //the result intersection sketch
    println("(A U B) ^ C: "+interSk.getEstimate());  //the estimate of the intersection

    //The AnotB operation is a little different as it is stateless:

    final AnotB aNotB = Sketches.setOperationBuilder().buildANotB();
    final CompactSketch not = aNotB.aNotB(skA, skC);

    println("A \\ C      : "+not.getEstimate()); //the estimate of the AnotB operation
  }

  @Test
  public void checkIsSameResource() {
    final int k = 16;
    final WritableMemory wmem = WritableMemory.writableWrap(new byte[k*16 + 32]);
    final Memory roCompactMem = Memory.wrap(new byte[8]);
    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion(wmem);
    assertTrue(union.isSameResource(wmem));
    assertFalse(union.isSameResource(roCompactMem));

    final Intersection inter = Sketches.setOperationBuilder().buildIntersection(wmem);
    assertTrue(inter.isSameResource(wmem));
    assertFalse(inter.isSameResource(roCompactMem));

    final AnotB aNotB = Sketches.setOperationBuilder().buildANotB();

    assertFalse(aNotB.isSameResource(roCompactMem));
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

  /**
   * Compute offsets for MyHeap for Union, sketch1, sketch2, sketch3, resultSketch, total layout.
   * @param sketchNomEntries the configured nominal entries of the sketch
   * @param unionNomEntries configured nominal entries of the union
   * @return array of offsets for Union, sketch1, sketch2, sketch3, resultSketch, total layout
   */
  private static int[] getHeapLayout(final int sketchNomEntries, final int unionNomEntries) {
    final int[] heapLayout = new int[6];
    final int unionBytes = SetOperation.getMaxUnionBytes(unionNomEntries);
    final int sketchBytes = getMaxUpdateSketchBytes(sketchNomEntries);
    final int resultBytes = Sketch.getMaxCompactSketchBytes(unionNomEntries);
    heapLayout[0] = 0;                             //offset for Union
    heapLayout[1] = unionBytes;                    //offset for sketch1
    heapLayout[2] = unionBytes + sketchBytes;      //offset for sketch2
    heapLayout[3] = unionBytes + 2*sketchBytes;    //offset for sketch3
    heapLayout[4] = unionBytes + 3*sketchBytes;    //offset for result
    heapLayout[5] = unionBytes + 3*sketchBytes + resultBytes;  //total
    return heapLayout;
  }

  private static double directUnionTrial1(
      final WritableMemory heapMem, final int[] heapLayout, final int sketchNomEntries, final int unionNomEntries) {

    final int offset = heapLayout[0];
    final int bytes = heapLayout[1] - offset;
    final WritableMemory unionMem = heapMem.writableRegion(offset, bytes);

    Union union = SetOperation.builder().setNominalEntries(unionNomEntries).buildUnion(unionMem);

    final WritableMemory sketch1mem = heapMem.writableRegion(heapLayout[1], heapLayout[2]-heapLayout[1]);
    final WritableMemory sketch2mem = heapMem.writableRegion(heapLayout[2], heapLayout[3]-heapLayout[2]);
    final WritableMemory sketch3mem = heapMem.writableRegion(heapLayout[3], heapLayout[4]-heapLayout[3]);
    final WritableMemory resultMem = heapMem.writableRegion(heapLayout[4], heapLayout[5]-heapLayout[4]);

    //Initialize the 3 sketches
    final UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(sketchNomEntries).build(sketch1mem);
    final UpdateSketch sk2 = UpdateSketch.builder().setNominalEntries(sketchNomEntries).build(sketch2mem);
    final UpdateSketch sk3 = UpdateSketch.builder().setNominalEntries(sketchNomEntries).build(sketch3mem);

    //This little trial has sk1 and sk2 distinct and sk2 overlap both.
    //Build the sketches.
    for (int i=0; i< sketchNomEntries; i++) {
      sk1.update(i);
      sk2.update(i + sketchNomEntries/2);
      sk3.update(i + sketchNomEntries);
    }

    //confirm that each of these 3 sketches is exact.
    assertEquals(sk1.getEstimate(), sketchNomEntries, 0.0);
    assertEquals(sk2.getEstimate(), sketchNomEntries, 0.0);
    assertEquals(sk3.getEstimate(), sketchNomEntries, 0.0);

    //Let's union the first 2 sketches
    union.union(sk1);
    union.union(sk2);

    //Let's recover the union and the 3rd sketch
    union = Sketches.wrapUnion(unionMem);
    union.union(Sketch.wrap(sketch3mem));

    final Sketch resSk = union.getResult(true, resultMem);
    final double est = resSk.getEstimate();

    return est;
  }

  private static double directUnionTrial2(
      final WritableMemory heapMem, final int[] heapLayout, final int sketchNomEntries, final int unionNomEntries) {

    final WritableMemory unionMem = heapMem.writableRegion(heapLayout[0], heapLayout[1]-heapLayout[0]);
    final WritableMemory sketch1mem = heapMem.writableRegion(heapLayout[1], heapLayout[2]-heapLayout[1]);
    final WritableMemory sketch2mem = heapMem.writableRegion(heapLayout[2], heapLayout[3]-heapLayout[2]);
    final WritableMemory sketch3mem = heapMem.writableRegion(heapLayout[3], heapLayout[4]-heapLayout[3]);
    final WritableMemory resultMem = heapMem.writableRegion(heapLayout[4], heapLayout[5]-heapLayout[4]);

    //Recover the 3 sketches
    final UpdateSketch sk1 = (UpdateSketch) Sketch.wrap(sketch1mem);
    final UpdateSketch sk2 = (UpdateSketch) Sketch.wrap(sketch2mem);
    final UpdateSketch sk3 = (UpdateSketch) Sketch.wrap(sketch3mem);

    //confirm that each of these 3 sketches is exact.
    assertEquals(sk1.getEstimate(), sketchNomEntries, 0.0);
    assertEquals(sk2.getEstimate(), sketchNomEntries, 0.0);
    assertEquals(sk3.getEstimate(), sketchNomEntries, 0.0);

    //Create a new union in the same space with a smaller size.
    unionMem.clear();
    final Union union = SetOperation.builder().setNominalEntries(unionNomEntries).buildUnion(unionMem);
    union.union(sk1);
    union.union(sk2);
    union.union(sk3);

    final Sketch resSk = union.getResult(true, resultMem);
    final double est = resSk.getEstimate();

    return est;
  }

}
