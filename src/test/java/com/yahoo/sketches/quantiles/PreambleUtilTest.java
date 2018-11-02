/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractMaxDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractMinDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractN;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertK;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertMaxDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertMinDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertN;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSerVer;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;

public class PreambleUtilTest {

  @Test
  public void checkInsertsAndExtracts() {
    final int bytes = 32;
    try (WritableDirectHandle offHeapMemHandler = WritableMemory.allocateDirect(bytes)) {
      final WritableMemory offHeapMem = offHeapMemHandler.get();
      final WritableMemory onHeapMem = WritableMemory.wrap(new byte[bytes]);

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
