/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.*;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.AllocMemory;
import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;

public class PreambleUtilTest {
  
  @Test
  public void checkInsertsAndExtracts() {
    int bytes = 32;
    Memory onHeapMem = new NativeMemory(new byte[bytes]);
    NativeMemory offHeapMem = new AllocMemory(bytes);
    Object onHeapArr = onHeapMem.array();
    Object offHeapArr = offHeapMem.array();
    long onHeapOffset = onHeapMem.getCumulativeOffset(0L);
    long offHeapOffset = offHeapMem.getCumulativeOffset(0L);
    
    onHeapMem.clear();
    offHeapMem.clear();
    
    //BYTES
    int v = 0XFF;
    int onH, offH;
    
    //PREAMBLE_LONGS_BYTE;
    insertPreLongs(onHeapArr, onHeapOffset, v);
    onH = extractPreLongs(onHeapArr, onHeapOffset);
    assertEquals(onH, v);
    
    insertPreLongs(offHeapArr, offHeapOffset, v);
    offH = extractPreLongs(offHeapArr, offHeapOffset);
    assertEquals(offH, v);
    onHeapMem.clear();
    offHeapMem.clear();
    
    //SER_VER_BYTE;
    insertSerVer(onHeapArr, onHeapOffset, v);
    onH = extractSerVer(onHeapArr, onHeapOffset);
    assertEquals(onH, v);
    
    insertSerVer(offHeapArr, offHeapOffset, v);
    offH = extractSerVer(offHeapArr, offHeapOffset);
    assertEquals(offH, v);
    onHeapMem.clear();
    offHeapMem.clear();
    
    //FAMILY_BYTE;
    insertFamilyID(onHeapArr, onHeapOffset, v);
    onH = extractFamilyID(onHeapArr, onHeapOffset);
    assertEquals(onH, v);
    
    insertFamilyID(offHeapArr, offHeapOffset, v);
    offH = extractFamilyID(offHeapArr, offHeapOffset);
    assertEquals(offH, v);
    onHeapMem.clear();
    offHeapMem.clear();
    
    //FLAGS_BYTE;
    insertFlags(onHeapArr, onHeapOffset, v);
    onH = extractFlags(onHeapArr, onHeapOffset);
    assertEquals(onH, v);
    
    insertFlags(offHeapArr, offHeapOffset, v);
    offH = extractFlags(offHeapArr, offHeapOffset);
    assertEquals(offH, v);
    onHeapMem.clear();
    offHeapMem.clear();
    
    //SHORTS
    v = 0XFFFF;
    
    //K_SHORT;
    insertK(onHeapArr, onHeapOffset, v);
    onH = extractK(onHeapArr, onHeapOffset);
    assertEquals(onH, v);
    
    insertK(offHeapArr, offHeapOffset, v);
    offH = extractK(offHeapArr, offHeapOffset);
    assertEquals(offH, v);
    onHeapMem.clear();
    offHeapMem.clear();
    
    //LONGS
    
    //N_LONG;
    long onHL, offHL, vL = 1L << 30;
    insertN(onHeapArr, onHeapOffset, vL);
    onHL = extractN(onHeapArr, onHeapOffset);
    assertEquals(onHL, vL);
    
    insertN(offHeapArr, offHeapOffset, vL);
    offHL = extractN(offHeapArr, offHeapOffset);
    assertEquals(offHL, vL);
    onHeapMem.clear();
    offHeapMem.clear();
    
    //DOUBLES
    
    //MIN_DOUBLE;
    double onHD, offHD, vD = 1L << 40;
    
    insertMinDouble(onHeapArr, onHeapOffset, vD);
    onHD = extractMinDouble(onHeapArr, onHeapOffset);
    assertEquals(onHD, vD);
    
    insertMinDouble(offHeapArr, offHeapOffset, vD);
    offHD = extractMinDouble(offHeapArr, offHeapOffset);
    assertEquals(offHD, vD);
    onHeapMem.clear();
    offHeapMem.clear();
    
    //MAX_DOUBLE;
    insertMaxDouble(onHeapArr, onHeapOffset, vD);
    onHD = extractMaxDouble(onHeapArr, onHeapOffset);
    assertEquals(onHD, vD);
    
    insertMaxDouble(offHeapArr, offHeapOffset, vD);
    offHD = extractMaxDouble(offHeapArr, offHeapOffset);
    assertEquals(offHD, vD);
    onHeapMem.clear();
    offHeapMem.clear();
    
    
    offHeapMem.freeMemory();
  }
  
  @Test
  public void checkToString() {
    int k = DoublesSketch.DEFAULT_K;
    int n = 1000000;
    DoublesSketch qs = DoublesSketch.builder().build(k);
    for (int i=0; i<n; i++) qs.update(i);
    byte[] byteArr = qs.toByteArray();
    println(PreambleUtil.toString(byteArr, true));
  }
  
  @Test
  public void checkToStringEmpty() {
    int k = DoublesSketch.DEFAULT_K;
    DoublesSketch qs = DoublesSketch.builder().build(k);
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
