/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.quantiles.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.quantiles.PreambleUtil.K_SHORT;
import static com.yahoo.sketches.quantiles.PreambleUtil.SER_DE_ID_SHORT;
import static com.yahoo.sketches.quantiles.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerDeId;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertK;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSerDeId;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSerVer;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.AllocMemory;
import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;

public class PreambleUtilTest {

  @Test
  public void checkExtracts() {
    long v; int shift;
//    v = 0XFFL;    shift = PREAMBLE_LONGS_BYTE << 3;
//    assertEquals(extractPreLongs(v<<shift), (int) v);
//    assertEquals(extractPreLongs(~(v<<shift)), 0);
    
    v = 0XFFL;    shift = SER_VER_BYTE << 3;
    assertEquals(extractSerVer(v<<shift), (int) v);
    assertEquals(extractSerVer(~(v<<shift)), 0);
    
    v = 0XFFL;    shift = FAMILY_BYTE << 3;
    assertEquals(extractFamilyID(v<<shift), (int) v);
    assertEquals(extractFamilyID(~(v<<shift)), 0);
    
    v = 0XFFL; shift = FLAGS_BYTE << 3;
    assertEquals(extractFlags(v<<shift), (int) v);
    assertEquals(extractFlags(~(v<<shift)), 0);
    
    v = 0XFFFFL;   shift = K_SHORT << 3;
    assertEquals(extractK(v<<shift), (int) v);
    assertEquals(extractK(~(v<<shift)), 0);
    
    v = 0XFFFFL;   shift = SER_DE_ID_SHORT << 3;
    assertEquals(extractSerDeId(v<<shift), (byte) v);
    assertEquals(extractSerDeId(~(v<<shift)), 0);
  }
  
  @Test
  public void checkInsertExtractPreLongs() {
    Memory onHeapMem = new NativeMemory(new byte[128]);
    NativeMemory offHeapMem = new AllocMemory(128);
    offHeapMem.clear();
    long onHeapOffset = onHeapMem.getCumulativeOffset(0L);
    long offHeapOffset = offHeapMem.getCumulativeOffset(0L);
    int by = 0XFF;
    insertPreLongs(onHeapMem, false, onHeapOffset, by);
    int x = extractPreLongs(onHeapMem, false, onHeapOffset) & by;
    assertEquals(x, by);
    insertPreLongs(offHeapMem, true, offHeapOffset, by);
    x = extractPreLongs(offHeapMem, true, offHeapOffset) & by;
    assertEquals(x, by);
    offHeapMem.freeMemory();
  }
  
  @Test
  public void checkInserts() {
    long v; int shift;
//    v = 0XFFL; shift = PREAMBLE_LONGS_BYTE << 3;
//    assertEquals(insertPreLongs((int)v, ~(v<<shift)), -1L);
//    assertEquals(insertPreLongs((int)v, 0), v<<shift);
    
    v = 0XFFL; shift = SER_VER_BYTE << 3; 
    assertEquals(insertSerVer((int)v, ~(v<<shift)), -1L);
    assertEquals(insertSerVer((int)v, 0), v<<shift);
    
    v = 0XFFL; shift = FAMILY_BYTE << 3;
    assertEquals(insertFamilyID((int)v, ~(v<<shift)), -1L);
    assertEquals(insertFamilyID((int)v, 0), v<<shift);
    
    v = 0XFFL; shift = FLAGS_BYTE << 3;
    assertEquals(insertFlags((int)v, ~(v<<shift)), -1L);
    assertEquals(insertFlags((int)v, 0), v<<shift);
    
    v = 0XFFFFL; shift = K_SHORT << 3;
    assertEquals(insertK((int)v, ~(v<<shift)), -1L);
    assertEquals(insertK((int)v, 0), v<<shift);
    
    v = 0XFFFFL; shift = SER_DE_ID_SHORT << 3;
    assertEquals(insertSerDeId((byte)v, ~(v<<shift)), -1L);
    assertEquals(insertSerDeId((byte)v, 0), v<<shift);
  }
  
  @Test
  public void checkToString() {
    int k = DoublesSketch.DEFAULT_K;
    int n = 1000000;
    DoublesSketch qs = DoublesSketch.builder().build(k);
    for (int i=0; i<n; i++) qs.update(i);
    byte[] byteArr = qs.toByteArray();
    println(PreambleUtil.toString(byteArr));
  }
  
  @Test
  public void checkToStringEmpty() {
    int k = DoublesSketch.DEFAULT_K;
    DoublesSketch qs = DoublesSketch.builder().build(k);
    byte[] byteArr = qs.toByteArray();
    println(PreambleUtil.toString(byteArr));
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
