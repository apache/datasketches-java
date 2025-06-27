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

import static org.apache.datasketches.common.Util.DEFAULT_UPDATE_SEED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.Arena;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.tuple.Util;
import org.testng.annotations.Test;

@SuppressWarnings("resource")
public class HeapifyWrapSerVer1and2Test {
  private static final short defaultSeedHash = org.apache.datasketches.common.Util.computeSeedHash(DEFAULT_UPDATE_SEED);

  @Test
  public void checkHeapifyCompactSketchAssumedDefaultSeed() {
    final int k = 64;
    final long seed = DEFAULT_UPDATE_SEED;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    CompactSketch csk = usk.compact();
    Memory cskMem = Memory.wrap(csk.toByteArray());
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = Sketches.heapifyCompactSketch(cskMem);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion2 test
    Memory sv2cskMem = BackwardConversions.convertSerVer3toSerVer2(csk, seed);
    cskResult = Sketches.heapifyCompactSketch(sv2cskMem);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion1 test
    Memory sv1cskMem = BackwardConversions.convertSerVer3toSerVer1(csk);
    cskResult = Sketches.heapifyCompactSketch(sv1cskMem);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
  }

  @Test
  public void checkHeapifyCompactSketchAssumedDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    CompactSketch csk = usk.compact();
    Memory cskMem = Memory.wrap(csk.toByteArray());
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = Sketches.heapifyCompactSketch(cskMem);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion2 test
    Memory sv2cskMem = BackwardConversions.convertSerVer3toSerVer2(csk, seed);
    cskResult = Sketches.heapifyCompactSketch(sv2cskMem);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion1 test
    Memory sv1cskMem = BackwardConversions.convertSerVer3toSerVer1(csk);
    cskResult = Sketches.heapifyCompactSketch(sv1cskMem);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), defaultSeedHash);
  }

  @Test
  public void checkHeapifyCompactSketchGivenDefaultSeed() {
    final int k = 64;
    final long seed = DEFAULT_UPDATE_SEED;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    CompactSketch csk = usk.compact();
    Memory cskMem = Memory.wrap(csk.toByteArray());
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = Sketches.heapifyCompactSketch(cskMem, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion2 test
    Memory sv2cskMem = BackwardConversions.convertSerVer3toSerVer2(csk, seed);
    cskResult = Sketches.heapifyCompactSketch(sv2cskMem, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion1 test
    Memory sv1cskMem = BackwardConversions.convertSerVer3toSerVer1(csk);
    cskResult = Sketches.heapifyCompactSketch(sv1cskMem, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
  }

  @Test
  public void checkHeapifyCompactSketchGivenDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    CompactSketch csk = usk.compact();
    Memory cskMem = Memory.wrap(csk.toByteArray());
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = Sketches.heapifyCompactSketch(cskMem, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion2 test
    Memory sv2cskMem = BackwardConversions.convertSerVer3toSerVer2(csk, seed);
    cskResult = Sketches.heapifyCompactSketch(sv2cskMem, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion1 test
    Memory sv1cskMem = BackwardConversions.convertSerVer3toSerVer1(csk);
    cskResult = Sketches.heapifyCompactSketch(sv1cskMem, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
  }

  @Test
  public void checkHeapifySketchAssumedDefaultSeed() {
    final int k = 64;
    final long seed = DEFAULT_UPDATE_SEED;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    CompactSketch csk = usk.compact();
    Memory cskMem = Memory.wrap(csk.toByteArray());
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = (CompactSketch) Sketches.heapifySketch(cskMem);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion2 test
    Memory sv2cskMem = BackwardConversions.convertSerVer3toSerVer2(csk, seed);
    cskResult = (CompactSketch) Sketches.heapifySketch(sv2cskMem);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion1 test
    Memory sv1cskMem = BackwardConversions.convertSerVer3toSerVer1(csk);
    cskResult = (CompactSketch) Sketches.heapifySketch(sv1cskMem);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
  }

  @Test
  public void checkHeapifySketchAssumedDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    CompactSketch csk = usk.compact();
    Memory cskMem = Memory.wrap(csk.toByteArray());
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = (CompactSketch) Sketches.heapifySketch(cskMem);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion2 test
    Memory sv2cskMem = BackwardConversions.convertSerVer3toSerVer2(csk, seed);
    cskResult = (CompactSketch) Sketches.heapifySketch(sv2cskMem);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion1 test
    Memory sv1cskMem = BackwardConversions.convertSerVer3toSerVer1(csk);
    cskResult = (CompactSketch) Sketches.heapifySketch(sv1cskMem);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), defaultSeedHash);
  }

  @Test
  public void checkHeapifySketchGivenDefaultSeed() {
    final int k = 64;
    final long seed = DEFAULT_UPDATE_SEED;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    CompactSketch csk = usk.compact();
    Memory cskMem = Memory.wrap(csk.toByteArray());
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = (CompactSketch) Sketches.heapifySketch(cskMem, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion2 test
    Memory sv2cskMem = BackwardConversions.convertSerVer3toSerVer2(csk, seed);
    cskResult = (CompactSketch) Sketches.heapifySketch(sv2cskMem, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion1 test
    Memory sv1cskMem = BackwardConversions.convertSerVer3toSerVer1(csk);
    cskResult = (CompactSketch) Sketches.heapifySketch(sv1cskMem, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
  }

  @Test
  public void checkHeapifySketchGivenDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }

    CompactSketch csk = usk.compact();
    Memory cskMem = Memory.wrap(csk.toByteArray());
    CompactSketch cskResult;

    //SerialVersion3 test
    cskResult = (CompactSketch) Sketches.heapifySketch(cskMem, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion2 test
    Memory sv2cskMem = BackwardConversions.convertSerVer3toSerVer2(csk, seed);
    cskResult = (CompactSketch) Sketches.heapifySketch(sv2cskMem, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);

    //SerialVersion1 test
    Memory sv1cskMem = BackwardConversions.convertSerVer3toSerVer1(csk);
    cskResult = (CompactSketch) Sketches.heapifySketch(sv1cskMem, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
  }

  @Test
  public void checkWrapCompactSketchAssumedDefaultSeed() {
    final int k = 64;
    final long seed = DEFAULT_UPDATE_SEED;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    WritableMemory offHeap;
    CompactSketch csk = usk.compact();

    //SerialVersion3 test
    offHeap = putOffHeap(Memory.wrap(csk.toByteArray()), Arena.ofConfined());
    cskResult = Sketches.wrapCompactSketch(offHeap);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertTrue(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }


    //SerialVersion2 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer2(csk, seed), Arena.ofConfined());
    cskResult = Sketches.wrapCompactSketch(offHeap);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }

    //SerialVersion1 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer1(csk), Arena.ofConfined());
    cskResult = Sketches.wrapCompactSketch(offHeap);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }
  }

  @Test
  public void checkWrapCompactSketchAssumedDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    WritableMemory offHeap;
    CompactSketch csk = usk.compact();

    //SerialVersion3 test
    offHeap = putOffHeap(Memory.wrap(csk.toByteArray()), Arena.ofConfined());
    cskResult = Sketches.wrapCompactSketch(offHeap);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertTrue(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }

    //SerialVersion2 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer2(csk, seed), Arena.ofConfined());
    cskResult = Sketches.wrapCompactSketch(offHeap);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }

    //SerialVersion1 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer1(csk), Arena.ofConfined());
    cskResult = Sketches.wrapCompactSketch(offHeap);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), defaultSeedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }
  }

  @Test
  public void checkWrapCompactSketchGivenDefaultSeed() {
    final int k = 64;
    final long seed = DEFAULT_UPDATE_SEED;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    WritableMemory offHeap;
    CompactSketch csk = usk.compact();

    //SerialVersion3 test
    offHeap = putOffHeap(Memory.wrap(csk.toByteArray()), Arena.ofConfined());
    cskResult = Sketches.wrapCompactSketch(offHeap, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertTrue(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }

    //SerialVersion2 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer2(csk, seed), Arena.ofConfined());
    cskResult = Sketches.wrapCompactSketch(offHeap, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }

    //SerialVersion1 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer1(csk), Arena.ofConfined());
    cskResult = Sketches.wrapCompactSketch(offHeap, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }
  }

  @Test
  public void checkWrapCompactSketchGivenDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    WritableMemory offHeap;
    CompactSketch csk = usk.compact();

    //SerialVersion3 test
    offHeap = putOffHeap(Memory.wrap(csk.toByteArray()), Arena.ofConfined());
    cskResult = Sketches.wrapCompactSketch(offHeap, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertTrue(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }

    //SerialVersion2 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer2(csk, seed), Arena.ofConfined());
    cskResult = Sketches.wrapCompactSketch(offHeap, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }

    //SerialVersion1 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer1(csk), Arena.ofConfined());
    cskResult = Sketches.wrapCompactSketch(offHeap, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }
  }

  @Test
  public void checkWrapSketchAssumedDefaultSeed() {
    final int k = 64;
    final long seed = DEFAULT_UPDATE_SEED;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    WritableMemory offHeap;
    CompactSketch csk = usk.compact();

    //SerialVersion3 test
    offHeap = putOffHeap(Memory.wrap(csk.toByteArray()), Arena.ofConfined());
    cskResult = (CompactSketch) Sketches.wrapSketch(offHeap);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertTrue(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }

    //SerialVersion2 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer2(csk, seed), Arena.ofConfined());
    cskResult = (CompactSketch) Sketches.wrapSketch(offHeap);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }

    //SerialVersion1 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer1(csk), Arena.ofConfined());
    cskResult = (CompactSketch) Sketches.wrapSketch(offHeap);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }
  }

  @Test
  public void checkWrapSketchAssumedDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    WritableMemory offHeap;
    CompactSketch csk = usk.compact();

    //SerialVersion3 test
    offHeap = putOffHeap(Memory.wrap(csk.toByteArray()), Arena.ofConfined());
    cskResult = (CompactSketch) Sketches.wrapSketch(offHeap);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertTrue(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }

    //SerialVersion2 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer2(csk, seed), Arena.ofConfined());
    cskResult = (CompactSketch) Sketches.wrapSketch(offHeap);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }

    //SerialVersion1 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer1(csk), Arena.ofConfined());
    cskResult = (CompactSketch) Sketches.wrapSketch(offHeap);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), defaultSeedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }
  }

  @Test
  public void checkWrapSketchGivenDefaultSeed() {
    final int k = 64;
    final long seed = DEFAULT_UPDATE_SEED;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    WritableMemory offHeap;
    CompactSketch csk = usk.compact();

    //SerialVersion3 test
    offHeap = putOffHeap(Memory.wrap(csk.toByteArray()), Arena.ofConfined());
    cskResult = (CompactSketch) Sketches.wrapSketch(offHeap, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertTrue(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }

    //SerialVersion2 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer2(csk, seed), Arena.ofConfined());
    cskResult = (CompactSketch) Sketches.wrapSketch(offHeap, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }

    //SerialVersion1 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer1(csk), Arena.ofConfined());
    cskResult = (CompactSketch) Sketches.wrapSketch(offHeap, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }
  }

  @Test
  public void checkWrapSketchGivenDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = org.apache.datasketches.common.Util.computeSeedHash(seed);
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i = 0; i < k; i++) { usk.update(i); }
    CompactSketch cskResult;
    WritableMemory offHeap;
    CompactSketch csk = usk.compact();

    //SerialVersion3 test
    offHeap = putOffHeap(Memory.wrap(csk.toByteArray()), Arena.ofConfined());
    cskResult = (CompactSketch) Sketches.wrapSketch(offHeap, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertTrue(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }

    //SerialVersion2 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer2(csk, seed), Arena.ofConfined());
    cskResult = (CompactSketch) Sketches.wrapSketch(offHeap, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }

    //SerialVersion1 test
    offHeap = putOffHeap(BackwardConversions.convertSerVer3toSerVer1(csk), Arena.ofConfined());
    cskResult = (CompactSketch) Sketches.wrapSketch(offHeap, seed);
    assertEquals(cskResult.getEstimate(), usk.getEstimate());
    assertEquals(cskResult.getSeedHash(), seedHash);
    assertFalse(cskResult.isDirect());
    if (offHeap.isAlive()) { offHeap.getArena().close(); }
  }

  private static WritableMemory putOffHeap(Memory heapMem, Arena arena) {
    final long cap = heapMem.getCapacity();
    WritableMemory wmem = WritableMemory.allocateDirect(cap, arena);
    heapMem.copyTo(0, wmem, 0, cap);
    return wmem;
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

