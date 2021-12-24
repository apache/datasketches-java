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

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.testng.Assert.assertEquals;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.Util;
import org.testng.annotations.Test;

public class SketchTest2 {
  private static final short defaultSeedHash = Util.computeSeedHash(DEFAULT_UPDATE_SEED);

  @Test
  public void checkHeapifyCompactSketchAssumedDefaultSeed() {
    final int k = 64;
    final long seed = DEFAULT_UPDATE_SEED;
    final short seedHash = Util.computeSeedHash(seed);
    UpdateSketch sv3usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i=0; i<k; i++) { sv3usk.update(i); }

    CompactSketch sv3csk = sv3usk.compact();
    Memory sv3cskMem = Memory.wrap(sv3csk.toByteArray());
    CompactSketch sv3cskResult;

    //SV3 test
    sv3cskResult = Sketches.heapifyCompactSketch(sv3cskMem);
    assertEquals(sv3cskResult.getEstimate(), sv3usk.getEstimate());
    assertEquals(sv3cskResult.getSeedHash(), seedHash);

    //SV2 test
    Memory sv2cskMem = BackwardConversions.convertSerVer3toSerVer2(sv3csk, seed);
    sv3cskResult = Sketches.heapifyCompactSketch(sv2cskMem);
    assertEquals(sv3cskResult.getEstimate(), sv3usk.getEstimate());
    assertEquals(sv3cskResult.getSeedHash(), seedHash);

    //SV1 test
    Memory sv1cskMem = BackwardConversions.convertSerVer3toSerVer1(sv3csk);
    sv3cskResult = Sketches.heapifyCompactSketch(sv1cskMem);
    assertEquals(sv3cskResult.getEstimate(), sv3usk.getEstimate());
    assertEquals(sv3cskResult.getSeedHash(), seedHash);
  }

  @Test
  public void checkHeapifyCompactSketchAssumedDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = Util.computeSeedHash(seed);
    UpdateSketch sv3usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i=0; i<k; i++) { sv3usk.update(i); }

    CompactSketch sv3csk = sv3usk.compact();
    Memory sv3cskMem = Memory.wrap(sv3csk.toByteArray());
    CompactSketch sv3cskResult;

    //SV3 test
    sv3cskResult = Sketches.heapifyCompactSketch(sv3cskMem);
    assertEquals(sv3cskResult.getEstimate(), sv3usk.getEstimate());
    assertEquals(sv3cskResult.getSeedHash(), seedHash);

    //SV2 test
    Memory sv2cskMem = BackwardConversions.convertSerVer3toSerVer2(sv3csk, seed);
    sv3cskResult = Sketches.heapifyCompactSketch(sv2cskMem);
    assertEquals(sv3cskResult.getEstimate(), sv3usk.getEstimate());
    assertEquals(sv3cskResult.getSeedHash(), seedHash);

    //SV1 test
    Memory sv1cskMem = BackwardConversions.convertSerVer3toSerVer1(sv3csk);
    sv3cskResult = Sketches.heapifyCompactSketch(sv1cskMem);
    assertEquals(sv3cskResult.getEstimate(), sv3usk.getEstimate());
    assertEquals(sv3cskResult.getSeedHash(), defaultSeedHash);
  }


  @Test
  public void checkHeapifyCompactSketchGivenDefaultSeed() {
    final int k = 64;
    final long seed = DEFAULT_UPDATE_SEED;
    final short seedHash = Util.computeSeedHash(seed);
    UpdateSketch sv3usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i=0; i<k; i++) { sv3usk.update(i); }

    CompactSketch sv3csk = sv3usk.compact();
    Memory sv3cskMem = Memory.wrap(sv3csk.toByteArray());
    CompactSketch sv3cskResult;

    //SV3 test
    sv3cskResult = Sketches.heapifyCompactSketch(sv3cskMem, seed);
    assertEquals(sv3cskResult.getEstimate(), sv3usk.getEstimate());
    assertEquals(sv3cskResult.getSeedHash(), seedHash);

    //SV2 test
    Memory sv2cskMem = BackwardConversions.convertSerVer3toSerVer2(sv3csk, seed);
    sv3cskResult = Sketches.heapifyCompactSketch(sv2cskMem, seed);
    assertEquals(sv3cskResult.getEstimate(), sv3usk.getEstimate());
    assertEquals(sv3cskResult.getSeedHash(), seedHash);

    //SV1 test
    Memory sv1cskMem = BackwardConversions.convertSerVer3toSerVer1(sv3csk);
    sv3cskResult = Sketches.heapifyCompactSketch(sv1cskMem, seed);
    assertEquals(sv3cskResult.getEstimate(), sv3usk.getEstimate());
    assertEquals(sv3cskResult.getSeedHash(), seedHash);
  }

  @Test
  public void checkHeapifyCompactSketchGivenDifferentSeed() {
    final int k = 64;
    final long seed = 128L;
    final short seedHash = Util.computeSeedHash(seed);
    UpdateSketch sv3usk = UpdateSketch.builder().setNominalEntries(k).setSeed(seed).build();
    for (int i=0; i<k; i++) { sv3usk.update(i); }

    CompactSketch sv3csk = sv3usk.compact();
    Memory sv3cskMem = Memory.wrap(sv3csk.toByteArray());
    CompactSketch sv3cskResult;

    //SV3 test
    sv3cskResult = Sketches.heapifyCompactSketch(sv3cskMem, seed);
    assertEquals(sv3cskResult.getEstimate(), sv3usk.getEstimate());
    assertEquals(sv3cskResult.getSeedHash(), seedHash);

    //SV2 test
    Memory sv2cskMem = BackwardConversions.convertSerVer3toSerVer2(sv3csk, seed);
    sv3cskResult = Sketches.heapifyCompactSketch(sv2cskMem, seed);
    assertEquals(sv3cskResult.getEstimate(), sv3usk.getEstimate());
    assertEquals(sv3cskResult.getSeedHash(), seedHash);

    //SV1 test
    Memory sv1cskMem = BackwardConversions.convertSerVer3toSerVer1(sv3csk);
    sv3cskResult = Sketches.heapifyCompactSketch(sv1cskMem, seed);
    assertEquals(sv3cskResult.getEstimate(), sv3usk.getEstimate());
    assertEquals(sv3cskResult.getSeedHash(), seedHash);
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

