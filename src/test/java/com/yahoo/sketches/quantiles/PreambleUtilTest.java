/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.BUFFER_DOUBLES_ALLOC_INT;
import static com.yahoo.sketches.quantiles.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.quantiles.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.quantiles.PreambleUtil.K_SHORT;
import static com.yahoo.sketches.quantiles.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.quantiles.PreambleUtil.SEED_SHORT;
import static com.yahoo.sketches.quantiles.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractBufAlloc;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSeed;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertBufAlloc;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertK;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSeed;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSerVer;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class PreambleUtilTest {

  @Test
  public void checkExtracts() {
    long v; int shift;
    v = 0XFFL;    shift = PREAMBLE_LONGS_BYTE << 3;
    assertEquals(extractPreLongs(v<<shift), (int) v);
    assertEquals(extractPreLongs(~(v<<shift)), 0);
    
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
    
    v = 0XFFFFL;   shift = SEED_SHORT << 3;
    assertEquals(extractSeed(v<<shift), (int) v);
    assertEquals(extractSeed(~(v<<shift)), 0);
    
    v = 0XFFFFFFFFL; shift = BUFFER_DOUBLES_ALLOC_INT << 3;
    assertEquals(extractBufAlloc(v<<shift), (int) v);
    assertEquals(extractBufAlloc(~(v<<shift)), 0);
  }
  
  @Test
  public void checkInserts() {
    long v; int shift;
    v = 0XFFL; shift = PREAMBLE_LONGS_BYTE << 3;
    assertEquals(insertPreLongs((int)v, ~(v<<shift)), -1L);
    assertEquals(insertPreLongs((int)v, 0), v<<shift);
    
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
    
    v = 0XFFFFL; shift = SEED_SHORT << 3;
    assertEquals(insertSeed((int)v, ~(v<<shift)), -1L);
    assertEquals(insertSeed((int)v, 0), v<<shift);
    
    v = 0XFFFFFFFFL; shift = BUFFER_DOUBLES_ALLOC_INT << 3;
    assertEquals(insertBufAlloc((int)v, ~(v<<shift)), -1L);
    assertEquals(insertBufAlloc((int)v, 0), v<<shift);
  }
  
  @Test
  public void checkToString() {
    int k = 227;
    int n = 1000000;
    QuantilesSketch qs = QuantilesSketch.builder().build(k);
    for (int i=0; i<n; i++) qs.update(i);
    byte[] byteArr = qs.toByteArray();
    println(PreambleUtil.toString(byteArr));
  }
  
  @Test
  public void checkToStringEmpty() {
    int k = 227;
    QuantilesSketch qs = QuantilesSketch.builder().build(k);
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
