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

package org.apache.datasketches.quantiles2;

import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.quantiles2.HeapUpdateDoublesSketchTest.buildAndLoadQS;

import java.lang.foreign.MemorySegment;

import org.testng.annotations.Test;

public class DoublesUtilTest {

  @Test
  public void checkPrintMemData() {
    final int k = 16;
    final int n = 1000;
    final DoublesSketch qs = buildAndLoadQS(k,n);

    byte[] byteArr = qs.toByteArray(false);
    MemorySegment mem = MemorySegment.ofArray(byteArr);
    println(memToString(true, true, mem));

    byteArr = qs.toByteArray(true);
    mem = MemorySegment.ofArray(byteArr);
    println(memToString(true, true, mem));
  }

  @Test
  public void checkPrintMemData2() {
    final int k = PreambleUtil.DEFAULT_K;
    final int n = 0;
    final DoublesSketch qs = buildAndLoadQS(k,n);

    final byte[] byteArr = qs.toByteArray();
    final MemorySegment mem = MemorySegment.ofArray(byteArr);
    println(memToString(true, true, mem));
  }

  static String memToString(final boolean withLevels, final boolean withLevelsAndItems,
      final MemorySegment mem) {
    final DoublesSketch ds = DoublesSketch.heapify(mem);
    return ds.toString(withLevels, withLevelsAndItems);
  }

  @Test
  public void checkCopyToHeap() {
    final int k = 128;
    final int n = 400;

    // HeapUpdateDoublesSketch
    final HeapUpdateDoublesSketch huds = (HeapUpdateDoublesSketch) buildAndLoadQS(k, n);
    final HeapUpdateDoublesSketch target1 = DoublesUtil.copyToHeap(huds);
    DoublesSketchTest.testSketchEquality(huds, target1);

    // DirectUpdateDoublesSketch
    final MemorySegment mem1 = MemorySegment.ofArray(huds.toByteArray());
    final DirectUpdateDoublesSketch duds = (DirectUpdateDoublesSketch) UpdateDoublesSketch.wrap(mem1);
    final HeapUpdateDoublesSketch target2 = DoublesUtil.copyToHeap(duds);
    DoublesSketchTest.testSketchEquality(huds, duds);
    DoublesSketchTest.testSketchEquality(duds, target2);

    // HeapCompactDoublesSketch
    final CompactDoublesSketch hcds = huds.compact();
    final HeapUpdateDoublesSketch target3  = DoublesUtil.copyToHeap(hcds);
    DoublesSketchTest.testSketchEquality(huds, hcds);
    DoublesSketchTest.testSketchEquality(hcds, target3);

    // DirectCompactDoublesSketch
    final MemorySegment mem2 = MemorySegment.ofArray(hcds.toByteArray());
    final DirectCompactDoublesSketch dcds = (DirectCompactDoublesSketch) DoublesSketch.wrap(mem2);
    final HeapUpdateDoublesSketch target4 = DoublesUtil.copyToHeap(dcds);
    DoublesSketchTest.testSketchEquality(huds, dcds);
    DoublesSketchTest.testSketchEquality(dcds, target4);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    print(s + LS);
  }

  /**
   * @param s value to print
   */
  static void print(final String s) {
    //System.out.print(s); //disable here
  }

}
