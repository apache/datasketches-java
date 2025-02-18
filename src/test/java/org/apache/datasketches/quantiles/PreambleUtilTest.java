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

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

public class PreambleUtilTest {

  @Test
  public void checkInsertsAndExtracts() {
    final int bytes = 32;
    try (Arena arena = Arena.ofConfined()) {
        WritableMemory offHeapMem = WritableMemory.allocateDirect(bytes, arena);
      final WritableMemory onHeapMem = WritableMemory.writableWrap(new byte[bytes]);

      onHeapMem.clear();
      offHeapMem.clear();

      //BYTES
      int v = 0XFF;
      int onH, offH;

      //PREAMBLE_LONGS_BYTE;
      insertPreLongs(onHeapMem, v);
      onH = extractPreLongs(onHeapMem);
      assertEquals(onH, v);

      insertPreLongs(offHeapMem, v);
      offH = extractPreLongs(offHeapMem);
      assertEquals(offH, v);
      onHeapMem.clear();
      offHeapMem.clear();

      //SER_VER_BYTE;
      insertSerVer(onHeapMem, v);
      onH = extractSerVer(onHeapMem);
      assertEquals(onH, v);

      insertSerVer(offHeapMem, v);
      offH = extractSerVer(offHeapMem);
      assertEquals(offH, v);
      onHeapMem.clear();
      offHeapMem.clear();

      //FAMILY_BYTE;
      insertFamilyID(onHeapMem, v);
      onH = extractFamilyID(onHeapMem);
      assertEquals(onH, v);

      insertFamilyID(offHeapMem, v);
      offH = extractFamilyID(offHeapMem);
      assertEquals(offH, v);
      onHeapMem.clear();
      offHeapMem.clear();

      //FLAGS_BYTE;
      insertFlags(onHeapMem, v);
      onH = extractFlags(onHeapMem);
      assertEquals(onH, v);

      insertFlags(offHeapMem, v);
      offH = extractFlags(offHeapMem);
      assertEquals(offH, v);
      onHeapMem.clear();
      offHeapMem.clear();

      //SHORTS
      v = 0XFFFF;

      //K_SHORT;
      insertK(onHeapMem, v);
      onH = extractK(onHeapMem);
      assertEquals(onH, v);

      insertK(offHeapMem, v);
      offH = extractK(offHeapMem);
      assertEquals(offH, v);
      onHeapMem.clear();
      offHeapMem.clear();

      //LONGS

      //N_LONG;
      long onHL, offHL, vL = 1L << 30;
      insertN(onHeapMem, vL);
      onHL = extractN(onHeapMem);
      assertEquals(onHL, vL);

      insertN(offHeapMem, vL);
      offHL = extractN(offHeapMem);
      assertEquals(offHL, vL);
      onHeapMem.clear();
      offHeapMem.clear();

      //DOUBLES

      //MIN_DOUBLE;
      double onHD, offHD, vD = 1L << 40;

      insertMinDouble(onHeapMem, vD);
      onHD = extractMinDouble(onHeapMem);
      assertEquals(onHD, vD);

      insertMinDouble(offHeapMem, vD);
      offHD = extractMinDouble(offHeapMem);
      assertEquals(offHD, vD);
      onHeapMem.clear();
      offHeapMem.clear();

      //MAX_DOUBLE;
      insertMaxDouble(onHeapMem, vD);
      onHD = extractMaxDouble(onHeapMem);
      assertEquals(onHD, vD);

      insertMaxDouble(offHeapMem, vD);
      offHD = extractMaxDouble(offHeapMem);
      assertEquals(offHD, vD);
      onHeapMem.clear();
      offHeapMem.clear();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkToString() {
    int k = PreambleUtil.DEFAULT_K;
    int n = 1000000;
    UpdateDoublesSketch qs = DoublesSketch.builder().setK(k).build();
    for (int i=0; i<n; i++) {
      qs.update(i);
    }
    byte[] byteArr = qs.toByteArray();
    DoublesSketch.toString(byteArr);
    println(DoublesSketch.toString(Memory.wrap(byteArr)));
  }

  @Test
  public void checkToStringEmpty() {
    int k = PreambleUtil.DEFAULT_K;
    DoublesSketch qs = DoublesSketch.builder().setK(k).build();
    byte[] byteArr = qs.toByteArray();
    println(PreambleUtil.toString(byteArr, true));
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
