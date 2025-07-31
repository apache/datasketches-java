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

package org.apache.datasketches.quantiles;

import static org.apache.datasketches.common.Util.clear;
import static org.apache.datasketches.quantiles.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.quantiles.PreambleUtil.extractFlags;
import static org.apache.datasketches.quantiles.PreambleUtil.extractK;
import static org.apache.datasketches.quantiles.PreambleUtil.extractMaxDouble;
import static org.apache.datasketches.quantiles.PreambleUtil.extractMinDouble;
import static org.apache.datasketches.quantiles.PreambleUtil.extractN;
import static org.apache.datasketches.quantiles.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.quantiles.PreambleUtil.extractSerVer;
import static org.apache.datasketches.quantiles.PreambleUtil.insertFamilyID;
import static org.apache.datasketches.quantiles.PreambleUtil.insertFlags;
import static org.apache.datasketches.quantiles.PreambleUtil.insertK;
import static org.apache.datasketches.quantiles.PreambleUtil.insertMaxDouble;
import static org.apache.datasketches.quantiles.PreambleUtil.insertMinDouble;
import static org.apache.datasketches.quantiles.PreambleUtil.insertN;
import static org.apache.datasketches.quantiles.PreambleUtil.insertPreLongs;
import static org.apache.datasketches.quantiles.PreambleUtil.insertSerVer;
import static org.testng.Assert.assertEquals;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.testng.annotations.Test;

public class PreambleUtilTest {

  @Test
  public void checkInsertsAndExtracts() {
    final int bytes = 32;
    try (Arena arena = Arena.ofConfined()) {
        final MemorySegment offHeapSeg = arena.allocate(bytes);
      final MemorySegment onHeapSeg = MemorySegment.ofArray(new byte[bytes]);

      clear(onHeapSeg);
      clear(offHeapSeg);

      //BYTES
      int v = 0XFF;
      int onH, offH;

      //PREAMBLE_LONGS_BYTE;
      insertPreLongs(onHeapSeg, v);
      onH = extractPreLongs(onHeapSeg);
      assertEquals(onH, v);

      insertPreLongs(offHeapSeg, v);
      offH = extractPreLongs(offHeapSeg);
      assertEquals(offH, v);
      clear(onHeapSeg);
      clear(offHeapSeg);

      //SER_VER_BYTE;
      insertSerVer(onHeapSeg, v);
      onH = extractSerVer(onHeapSeg);
      assertEquals(onH, v);

      insertSerVer(offHeapSeg, v);
      offH = extractSerVer(offHeapSeg);
      assertEquals(offH, v);
      clear(onHeapSeg);
      clear(offHeapSeg);

      //FAMILY_BYTE;
      insertFamilyID(onHeapSeg, v);
      onH = extractFamilyID(onHeapSeg);
      assertEquals(onH, v);

      insertFamilyID(offHeapSeg, v);
      offH = extractFamilyID(offHeapSeg);
      assertEquals(offH, v);
      clear(onHeapSeg);
      clear(offHeapSeg);

      //FLAGS_BYTE;
      insertFlags(onHeapSeg, v);
      onH = extractFlags(onHeapSeg);
      assertEquals(onH, v);

      insertFlags(offHeapSeg, v);
      offH = extractFlags(offHeapSeg);
      assertEquals(offH, v);
      clear(onHeapSeg);
      clear(offHeapSeg);

      //SHORTS
      v = 0XFFFF;

      //K_SHORT;
      insertK(onHeapSeg, v);
      onH = extractK(onHeapSeg);
      assertEquals(onH, v);

      insertK(offHeapSeg, v);
      offH = extractK(offHeapSeg);
      assertEquals(offH, v);
      clear(onHeapSeg);
      clear(offHeapSeg);

      //LONGS

      //N_LONG;
      long onHL, offHL;
      final long vL = 1L << 30;
      insertN(onHeapSeg, vL);
      onHL = extractN(onHeapSeg);
      assertEquals(onHL, vL);

      insertN(offHeapSeg, vL);
      offHL = extractN(offHeapSeg);
      assertEquals(offHL, vL);
      clear(onHeapSeg);
      clear(offHeapSeg);

      //DOUBLES

      //MIN_DOUBLE;
      double onHD, offHD;
      final double vD = 1L << 40;

      insertMinDouble(onHeapSeg, vD);
      onHD = extractMinDouble(onHeapSeg);
      assertEquals(onHD, vD);

      insertMinDouble(offHeapSeg, vD);
      offHD = extractMinDouble(offHeapSeg);
      assertEquals(offHD, vD);
      clear(onHeapSeg);
      clear(offHeapSeg);

      //MAX_DOUBLE;
      insertMaxDouble(onHeapSeg, vD);
      onHD = extractMaxDouble(onHeapSeg);
      assertEquals(onHD, vD);

      insertMaxDouble(offHeapSeg, vD);
      offHD = extractMaxDouble(offHeapSeg);
      assertEquals(offHD, vD);
      clear(onHeapSeg);
      clear(offHeapSeg);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkToString() {
    final int k = PreambleUtil.DEFAULT_K;
    final int n = 1000000;
    final UpdateDoublesSketch qs = DoublesSketch.builder().setK(k).build();
    for (int i=0; i<n; i++) {
      qs.update(i);
    }
    final byte[] byteArr = qs.toByteArray();
    DoublesSketch.toString(byteArr);
    println(DoublesSketch.toString(MemorySegment.ofArray(byteArr)));
  }

  @Test
  public void checkToStringEmpty() {
    final int k = PreambleUtil.DEFAULT_K;
    final DoublesSketch qs = DoublesSketch.builder().setK(k).build();
    final byte[] byteArr = qs.toByteArray();
    println(PreambleUtil.toString(byteArr, true));
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
