/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.SetOperation.getMaxUnionBytes;
import static com.yahoo.sketches.theta.PreambleUtil.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.PreambleUtil;
import com.yahoo.sketches.theta.UpdateSketch;

/** 
 * @author Lee Rhodes
 */
public class PreambleUtilTest {

  @Test
  public void checkToString() {
    int k = 4096;
    int u = 2*k;
    int bytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    byte[] byteArray = new byte[bytes];
    Memory mem = new NativeMemory(byteArray);

    UpdateSketch quick1 = UpdateSketch.builder().initMemory(mem).build(k);
    println(PreambleUtil.preambleToString(byteArray));
    
    Assert.assertTrue(quick1.isEmpty());

    for (int i = 0; i< u; i++) {
      quick1.update(i);
    }
    println("U: "+quick1.getEstimate());
    
    assertEquals(quick1.getEstimate(), u, .05*u);
    assertTrue(quick1.getRetainedEntries(false) > k);
    println(quick1.toString());
    println(PreambleUtil.preambleToString(mem));
    
    Memory uMem = new NativeMemory(new byte[getMaxUnionBytes(k)]);
    Union union = SetOperation.builder().initMemory(uMem).buildUnion(k);
    union.update(quick1);
    println(PreambleUtil.preambleToString(uMem));
    
  }
  
  @Test
  public void checkPreambleToStringExceptions() {
    byte[] byteArr = new byte[7];
    try { //check preLongs < 8 fails
      PreambleUtil.preambleToString(byteArr);
      fail("Did not throw SketchesArgumentException.");
    } catch (SketchesArgumentException e) {
      //expected
    }
    byteArr = new byte[8];
    byteArr[0] = (byte) 2; //needs min capacity of 16
    try { //check preLongs == 2 fails
      PreambleUtil.preambleToString(byteArr);
      fail("Did not throw SketchesArgumentException.");
    } catch (SketchesArgumentException e) {
      //expected
    }
  }
  
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSeedHashFromSeed() {
    //In the first 64K values 50541 produces a seedHash of 0, 
    Util.computeSeedHash(50541);
  }
  
  @Test
  public void checkPreLongs() {
    UpdateSketch sketch = UpdateSketch.builder().build(16);
    CompactSketch comp = sketch.compact(false, null);
    byte[] byteArr = comp.toByteArray();
    println(PreambleUtil.preambleToString(byteArr)); //PreLongs = 1
    
    sketch.update(1);
    comp = sketch.compact(false, null);
    byteArr = comp.toByteArray();
    println(PreambleUtil.preambleToString(byteArr)); //PreLongs = 2
    
    for (int i=2; i<=32; i++) sketch.update(i);
    comp = sketch.compact(false, null);
    byteArr = comp.toByteArray();
    println(PreambleUtil.preambleToString(byteArr)); //PreLongs = 3
  }
  
  @Test
  public void checkExtracts() {
    long v; int shift;
    v = 0X3FL;          shift = PREAMBLE_LONGS_BYTE << 3;
    assertEquals(extractPreLongs(v<<shift), (int) v);
    assertEquals(extractPreLongs(~(v<<shift)), 0);
    
    v = 3L;             shift = (PREAMBLE_LONGS_BYTE << 3) + 6;
    assertEquals(extractResizeFactor(v<<shift), (int) v);
    assertEquals(extractResizeFactor(~(v<<shift)), 0);
    
    v = 0XFFL;          shift = SER_VER_BYTE << 3;
    assertEquals(extractSerVer(v<<shift), (int) v);
    assertEquals(extractSerVer(~(v<<shift)), 0);
    
    v = 0XFFL;          shift = FAMILY_BYTE << 3;
    assertEquals(extractFamilyID(v<<shift), (int) v);
    assertEquals(extractFamilyID(~(v<<shift)), 0);
    
    v = 0XFFL;          shift = LG_NOM_LONGS_BYTE << 3;
    assertEquals(extractLgNomLongs(v<<shift), (int) v);
    assertEquals(extractLgNomLongs(~(v<<shift)), 0);
    
    v = 0XFFL;          shift = LG_ARR_LONGS_BYTE << 3;
    assertEquals(extractLgArrLongs(v<<shift), (int) v);
    assertEquals(extractLgArrLongs(~(v<<shift)), 0);
    
    v = 0XFFL;          shift = FLAGS_BYTE << 3;
    assertEquals(extractFlags(v<<shift), (int) v);
    assertEquals(extractFlags(~(v<<shift)), 0);
    
    v = 0XFFL;          shift = FLAGS_BYTE_V1 << 3;
    assertEquals(extractFlagsV1(v<<shift), (int) v);
    assertEquals(extractFlagsV1(~(v<<shift)), 0);
    
    v = 0XFFFFL;        shift = SEED_HASH_SHORT << 3;
    assertEquals(extractSeedHash(v<<shift), (int) v);
    assertEquals(extractSeedHash(~(v<<shift)), 0);
    
    v = 0XFFFFFFFFL;    shift = 0;
    assertEquals(extractCurCount(v<<shift), (int) v);
    assertEquals(extractCurCount(~(v<<shift)), 0);
    
    float f = (float) 1.0; shift = 32;
    long res = insertP(f, -1L);
    float fresult = extractP(res);
    assertEquals(fresult, f);
  }
  
  @Test
  public void checkInserts() {
    long v; int shift; 
    v = 0X3FL;          shift = PREAMBLE_LONGS_BYTE << 3;
    assertEquals(insertPreLongs((int)v, ~(v<<shift)), -1L);
    assertEquals(insertPreLongs((int)v, 0), v<<shift);
    
    v = 3L;             shift = (PREAMBLE_LONGS_BYTE << 3) + 6;
    assertEquals(insertResizeFactor((int)v, ~(v<<shift)), -1L);
    assertEquals(insertResizeFactor((int)v, 0), v<<shift);
    
    v = 0XFFL;          shift = SER_VER_BYTE << 3; 
    assertEquals(insertSerVer((int)v, ~(v<<shift)), -1L);
    assertEquals(insertSerVer((int)v, 0), v<<shift);
    
    v = 0XFFL;          shift = FAMILY_BYTE << 3;
    assertEquals(insertFamilyID((int)v, ~(v<<shift)), -1L);
    assertEquals(insertFamilyID((int)v, 0), v<<shift);
    
    v = 0XFFL;          shift = LG_NOM_LONGS_BYTE << 3;
    assertEquals(insertLgNomLongs((int)v, ~(v<<shift)), -1L);
    assertEquals(insertLgNomLongs((int)v, 0), v<<shift);
    
    v = 0XFFL;          shift = LG_ARR_LONGS_BYTE << 3;
    assertEquals(insertLgArrLongs((int)v, ~(v<<shift)), -1L);
    assertEquals(insertLgArrLongs((int)v, 0), v<<shift);
    
    v = 0XFFL;          shift = FLAGS_BYTE << 3;
    assertEquals(insertFlags((int)v, ~(v<<shift)), -1L);
    assertEquals(insertFlags((int)v, 0), v<<shift);
    
    v = 0XFFFFL;        shift = SEED_HASH_SHORT << 3;
    assertEquals(insertSeedHash((int)v, ~(v<<shift)), -1L);
    assertEquals(insertSeedHash((int)v, 0), v<<shift);
    
    v = 0XFFFFFFFFL;    shift = 0;
    assertEquals(insertCurCount((int)v, ~(v<<shift)), -1L);
    assertEquals(insertCurCount((int)v, 0), v<<shift);

    float f = (float) 1.0; shift = 32;
    long res = insertP(f, -1L);
    int hi = (int) (res >>> shift);
    int lo = (int) res;
    assertEquals(lo, -1);
    assertEquals(Float.intBitsToFloat(hi), f);
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
