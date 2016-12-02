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
    Object onHeapMemObj = onHeapMem.array();
    Object offHeapMemObj = offHeapMem.array();
    long onHeapCumOffset = onHeapMem.getCumulativeOffset(0L);
    long offHeapCumOffset = offHeapMem.getCumulativeOffset(0L);
    
    onHeapMem.clear();
    offHeapMem.clear();
    
    //BYTES
    int v = 0XFF;
    int onH, offH;
    
    //PREAMBLE_LONGS_BYTE;
    insertPreLongs(onHeapMemObj, onHeapCumOffset, v);
    onH = extractPreLongs(onHeapMemObj, onHeapCumOffset);
    assertEquals(onH, v);
    
    insertPreLongs(offHeapMemObj, offHeapCumOffset, v);
    offH = extractPreLongs(offHeapMemObj, offHeapCumOffset);
    assertEquals(offH, v);
    onHeapMem.clear();
    offHeapMem.clear();
    
    //SER_VER_BYTE;
    insertSerVer(onHeapMemObj, onHeapCumOffset, v);
    onH = extractSerVer(onHeapMemObj, onHeapCumOffset);
    assertEquals(onH, v);
    
    insertSerVer(offHeapMemObj, offHeapCumOffset, v);
    offH = extractSerVer(offHeapMemObj, offHeapCumOffset);
    assertEquals(offH, v);
    onHeapMem.clear();
    offHeapMem.clear();
    
    //FAMILY_BYTE;
    insertFamilyID(onHeapMemObj, onHeapCumOffset, v);
    onH = extractFamilyID(onHeapMemObj, onHeapCumOffset);
    assertEquals(onH, v);
    
    insertFamilyID(offHeapMemObj, offHeapCumOffset, v);
    offH = extractFamilyID(offHeapMemObj, offHeapCumOffset);
    assertEquals(offH, v);
    onHeapMem.clear();
    offHeapMem.clear();
    
    //FLAGS_BYTE;
    insertFlags(onHeapMemObj, onHeapCumOffset, v);
    onH = extractFlags(onHeapMemObj, onHeapCumOffset);
    assertEquals(onH, v);
    
    insertFlags(offHeapMemObj, offHeapCumOffset, v);
    offH = extractFlags(offHeapMemObj, offHeapCumOffset);
    assertEquals(offH, v);
    onHeapMem.clear();
    offHeapMem.clear();
    
    //SHORTS
    v = 0XFFFF;
    
    //K_SHORT;
    insertK(onHeapMemObj, onHeapCumOffset, v);
    onH = extractK(onHeapMemObj, onHeapCumOffset);
    assertEquals(onH, v);
    
    insertK(offHeapMemObj, offHeapCumOffset, v);
    offH = extractK(offHeapMemObj, offHeapCumOffset);
    assertEquals(offH, v);
    onHeapMem.clear();
    offHeapMem.clear();
    
    //LONGS
    
    //N_LONG;
    long onHL, offHL, vL = 1L << 30;
    insertN(onHeapMemObj, onHeapCumOffset, vL);
    onHL = extractN(onHeapMemObj, onHeapCumOffset);
    assertEquals(onHL, vL);
    
    insertN(offHeapMemObj, offHeapCumOffset, vL);
    offHL = extractN(offHeapMemObj, offHeapCumOffset);
    assertEquals(offHL, vL);
    onHeapMem.clear();
    offHeapMem.clear();
    
    //DOUBLES
    
    //MIN_DOUBLE;
    double onHD, offHD, vD = 1L << 40;
    
    insertMinDouble(onHeapMemObj, onHeapCumOffset, vD);
    onHD = extractMinDouble(onHeapMemObj, onHeapCumOffset);
    assertEquals(onHD, vD);
    
    insertMinDouble(offHeapMemObj, offHeapCumOffset, vD);
    offHD = extractMinDouble(offHeapMemObj, offHeapCumOffset);
    assertEquals(offHD, vD);
    onHeapMem.clear();
    offHeapMem.clear();
    
    //MAX_DOUBLE;
    insertMaxDouble(onHeapMemObj, onHeapCumOffset, vD);
    onHD = extractMaxDouble(onHeapMemObj, onHeapCumOffset);
    assertEquals(onHD, vD);
    
    insertMaxDouble(offHeapMemObj, offHeapCumOffset, vD);
    offHD = extractMaxDouble(offHeapMemObj, offHeapCumOffset);
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
