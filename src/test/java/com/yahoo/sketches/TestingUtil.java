/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import static com.yahoo.sketches.Util.zeroPad;

//import org.testng.annotations.Test;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;


/**
 * @author Lee Rhodes
 */
public class TestingUtil {
  public static final String LS = System.getProperty("line.separator");
  public static final char TAB = '\t';
  
  /**
   * Returns a string structured as an array of longs viewed as spaced hex bytes in Big-Endian order.
   * Each row (a long) is preceeded by a long index. 
   * @param arr the given byte array
   * @param comment an optional header comment
   * @return the above described string.
   */
  public static String byteArrToHexBytes(byte[] arr, String comment) {
    Memory mem = new NativeMemory(arr);
    return memToHexBytes(mem, comment);
  }
  
  /**
   * Returns a string structured as an array of longs viewed as spaced hex bytes in Big-Endian order.
   * Each row (a long) is preceeded by a long index. 
   * @param arr the given long array
   * @param comment an optional header comment
   * @return the above described string.
   */
  public static String longArrToHexBytes(long[] arr, String comment) {
    Memory mem = new NativeMemory(arr);
    return memToHexBytes(mem, comment);
  }
  
  /**
   * Returns a string structured as an array of longs viewed as spaced hex bytes in Big-Endian order.
   * Each row (a long) is preceeded by a long index.  Any odd bytes at the end are presented in
   * a long at the end.
   * @param mem the given Memory
   * @param comment an optional header comment
   * @return the above described string.
   */
  public static String memToHexBytes(Memory mem, String comment) {
    StringBuilder sb = new StringBuilder();
    if ((comment != null) && !comment.isEmpty() ) {
      sb.append(comment).append(LS);
    }
    int bytes = (int) mem.getCapacity();
    int longs = bytes >>> 3;
    int remBytes = bytes % 8;
    boolean hasRem = (remBytes > 0);
    int i;
    for (i=0; i<longs; i++) { //integral longs
      long v = mem.getLong(i << 3);
      sb.append(i).append(TAB).append(longToHexBytes(v)).append(LS);
    }
    if (hasRem) {
      int longBytes = longs << 3;
      long long0 = 0;
      for (int b=0; b<remBytes; b++) {
        int byteVal = mem.getByte(longBytes + b);
        long0 = insertByte(byteVal, b, long0);
      }
      sb.append(i).append(TAB).append(longToHexBytes(long0)).append(LS);
    }
    return sb.toString();
  }

  /**
   * Returns a string of spaced hex bytes in Big-Endian order.
   * @param v the given long
   * @return string of spaced hex bytes in Big-Endian order.
   */
  public static String longToHexBytes(long v) {
    long mask = 0XFFL;
    StringBuilder sb = new StringBuilder();
    for (int i = 8; i--> 0; ) {
      String s = Long.toHexString((v >>> i*8) & mask);
      sb.append(zeroPad(s, 2)).append(" ");
    }
    return sb.toString();
  }
  
  //@Test
  public static void test1() {
    int len = 27;
    byte[] byteArr = new byte[len];
    for (int i=0; i<len; i++) {
      byteArr[i] = (byte) i;
    }
    Memory mem = new NativeMemory(byteArr);
    println(memToHexBytes(mem, "Test"));
  }
  
  private static final long insertByte(final int byteVal, int byteIdx, long long0) {
    int shift = byteIdx << 3;
    long mask = 0XFFL;
    return ((byteVal & mask) << shift) | (~(mask << shift) & long0);
  }
  
  //@Test
  public void printlnTest() {
    println(this.getClass().getSimpleName());
  }
  
  /**
   * @param s value to print 
   */
  static void println(String s) {
    System.out.println(s); //disable here
  }
}
