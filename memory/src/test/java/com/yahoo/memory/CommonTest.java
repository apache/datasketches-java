/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.memory;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * @author Lee Rhodes
 */
public class CommonTest {
  
  public static void setGetTests(Memory mem) {    
    mem.putBoolean(0, true);
    assertEquals(mem.getBoolean(0), true);
    mem.putBoolean(0, false);
    assertEquals(mem.getBoolean(0), false);
    
    mem.putByte(0, (byte) -1);
    assertEquals(mem.getByte(0), (byte) -1);
    mem.putByte(0, (byte)0);
    assertEquals(mem.getByte(0), (byte)0);

    mem.putChar(0, 'A');
    assertEquals(mem.getChar(0), 'A');
    mem.putChar(0, 'Z');
    assertEquals(mem.getChar(0), 'Z');
    
    mem.putShort(0, Short.MAX_VALUE);
    assertEquals(mem.getShort(0), Short.MAX_VALUE);
    mem.putShort(0, Short.MIN_VALUE);
    assertEquals(mem.getShort(0), Short.MIN_VALUE);
    
    mem.putInt(0, Integer.MAX_VALUE);
    assertEquals(mem.getInt(0), Integer.MAX_VALUE);
    mem.putInt(0, Integer.MIN_VALUE);
    assertEquals(mem.getInt(0), Integer.MIN_VALUE);

    mem.putFloat(0, Float.MAX_VALUE);
    assertEquals(mem.getFloat(0), Float.MAX_VALUE);
    mem.putFloat(0, Float.MIN_VALUE);
    assertEquals(mem.getFloat(0), Float.MIN_VALUE);
    
    mem.putLong(0, Long.MAX_VALUE);
    assertEquals(mem.getLong(0), Long.MAX_VALUE);
    mem.putLong(0, Long.MIN_VALUE);
    assertEquals(mem.getLong(0), Long.MIN_VALUE);
    
    mem.putDouble(0, Double.MAX_VALUE);
    assertEquals(mem.getDouble(0), Double.MAX_VALUE);
    mem.putDouble(0, Double.MIN_VALUE);
    assertEquals(mem.getDouble(0), Double.MIN_VALUE);
  }
  
  public static void setGetArraysTests(Memory mem) {
    int accessCapacity = (int)mem.getCapacity();
    
    int words = 4;
    boolean[] srcArray1 = {true, false, true, false};
    boolean[] dstArray1 = new boolean[words];
    mem.fill(0, accessCapacity, (byte)127);
    mem.putBooleanArray(0, srcArray1, 0, words);
    mem.getBooleanArray(0, dstArray1, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray1[i], srcArray1[i]);
    }

    byte[] srcArray2 = { 1, -2, 3, -4 };
    byte[] dstArray2 = new byte[4];
    mem.putByteArray(0, srcArray2, 0, words);
    mem.getByteArray(0, dstArray2, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray2[i], srcArray2[i]);
    }
    
    char[] srcArray3 = { 'A', 'B', 'C', 'D' };
    char[] dstArray3 = new char[words];
    mem.putCharArray(0, srcArray3, 0, words);
    mem.getCharArray(0, dstArray3, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray3[i], srcArray3[i]);
    }
    
    double[] srcArray4 = { 1.0, -2.0, 3.0, -4.0 };
    double[] dstArray4 = new double[words];
    mem.putDoubleArray(0, srcArray4, 0, words);
    mem.getDoubleArray(0, dstArray4, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray4[i], srcArray4[i], 0.0);
    }
    
    float[] srcArray5 = { (float)1.0, (float)-2.0, (float)3.0, (float)-4.0 };
    float[] dstArray5 = new float[words];
    mem.putFloatArray(0, srcArray5, 0, words);
    mem.getFloatArray(0, dstArray5, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray5[i], srcArray5[i], 0.0);
    }
    
    int[] srcArray6 = { 1, -2, 3, -4 };
    int[] dstArray6 = new int[words];
    mem.putIntArray(0, srcArray6, 0, words);
    mem.getIntArray(0, dstArray6, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray6[i], srcArray6[i]);
    }
    
    long[] srcArray7 = { 1, -2, 3, -4 };
    long[] dstArray7 = new long[words];
    mem.putLongArray(0, srcArray7, 0, words);
    mem.getLongArray(0, dstArray7, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray7[i], srcArray7[i]);
    }
    
    short[] srcArray8 = { 1, -2, 3, -4 };
    short[] dstArray8 = new short[words];
    mem.putShortArray(0, srcArray8, 0, words);
    mem.getShortArray(0, dstArray8, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray8[i], srcArray8[i]);
    }
  }
  
  public static void setGetPartialArraysWithOffsetTests(Memory mem) {
    int words = 4;
    boolean[] srcArray1 = {true, false, true, false};
    boolean[] dstArray1 = new boolean[words];
    mem.putBooleanArray(0, srcArray1, 2, words/2);
    mem.getBooleanArray(0, dstArray1, 2, words/2);
    for (int i=2; i<words/2; i++) {
      assertEquals(dstArray1[i], srcArray1[i]);
    }

    byte[] srcArray2 = { 1, -2, 3, -4 };
    byte[] dstArray2 = new byte[words];
    mem.putByteArray(0, srcArray2, 2, words/2);
    mem.getByteArray(0, dstArray2, 2, words/2);
    for (int i=2; i<words/2; i++) {
      assertEquals(dstArray2[i], srcArray2[i]);
    }
    
    char[] srcArray3 = { 'A', 'B', 'C', 'D' };
    char[] dstArray3 = new char[words];
    mem.putCharArray(0, srcArray3, 2, words/2);
    mem.getCharArray(0, dstArray3, 2, words/2);
    for (int i=2; i<words/2; i++) {
      assertEquals(dstArray3[i], srcArray3[i]);
    }
    
    double[] srcArray4 = { 1.0, -2.0, 3.0, -4.0 };
    double[] dstArray4 = new double[words];
    mem.putDoubleArray(0, srcArray4, 2, words/2);
    mem.getDoubleArray(0, dstArray4, 2, words/2);
    for (int i=2; i<words/2; i++) {
      assertEquals(dstArray4[i], srcArray4[i], 0.0);
    }
    
    float[] srcArray5 = { (float)1.0, (float)-2.0, (float)3.0, (float)-4.0 };
    float[] dstArray5 = new float[words];
    mem.putFloatArray(0, srcArray5, 2, words/2);
    mem.getFloatArray(0, dstArray5, 2, words/2);
    for (int i=2; i<words/2; i++) {
      assertEquals(dstArray5[i], srcArray5[i], 0.0);
    }
    
    int[] srcArray6 = { 1, -2, 3, -4 };
    int[] dstArray6 = new int[words];
    mem.putIntArray(0, srcArray6, 2, words/2);
    mem.getIntArray(0, dstArray6, 2, words/2);
    for (int i=2; i<words/2; i++) {
      assertEquals(dstArray6[i], srcArray6[i]);
    }
    
    long[] srcArray7 = { 1, -2, 3, -4 };
    long[] dstArray7 = new long[words];
    mem.putLongArray(0, srcArray7, 2, words/2);
    mem.getLongArray(0, dstArray7, 2, words/2);
    for (int i=2; i<words/2; i++) {
      assertEquals(dstArray7[i], srcArray7[i]);
    }
    
    short[] srcArray8 = { 1, -2, 3, -4 };
    short[] dstArray8 = new short[words];
    mem.putShortArray(0, srcArray8, 2, words/2);
    mem.getShortArray(0, dstArray8, 2, words/2);
    for (int i=2; i<words/2; i++) {
      assertEquals(dstArray8[i], srcArray8[i]);
    }
  }
  
  public static void setClearIsBitsTests(Memory mem) {
  //single bits
    for (int i=0; i<8; i++) {
      byte bitMask = (byte)(1 << i);
      assertTrue(mem.isAnyBitsClear(0, bitMask));
      mem.setBits(0, bitMask);
      assertTrue(mem.isAnyBitsSet(0, bitMask));
      mem.clearBits(0, bitMask);
      assertTrue(mem.isAnyBitsClear(0, bitMask));
    }
    
    //multiple bits
    for (int i=0; i<7; i++) {
      byte bitMask1 = (byte)(1 << i);
      byte bitMask2 = (byte)(3 << i);
      assertTrue(mem.isAnyBitsClear(0, bitMask1));
      assertTrue(mem.isAnyBitsClear(0, bitMask2));
      mem.setBits(0, bitMask1); //set one bit
      assertTrue(mem.isAnyBitsSet(0, bitMask2)); 
      assertTrue(mem.isAnyBitsClear(0, bitMask2));
      assertFalse(mem.isAllBitsSet(0, bitMask2));
      assertFalse(mem.isAllBitsClear(0, bitMask2));
    }
  }
  
  public static void getAndAddSetTests(Memory mem) {
    mem.putInt(0, 500);
    mem.getAndAddInt(0, 1);
    assertEquals(mem.getInt(0), 501);
    
    mem.putLong(0, 500);
    mem.getAndAddLong(0, 1);
    assertEquals(mem.getLong(0), 501);
    
    mem.putInt(0, 500);
    int oldInt = mem.getAndSetInt(0, 501);
    int newInt = mem.getInt(0);
    assertEquals(oldInt, 500);
    assertEquals(newInt, 501);
    
    mem.putLong(0, 500);
    long oldLong = mem.getAndSetLong(0, 501);
    long newLong = mem.getLong(0);
    assertEquals(oldLong, 500);
    assertEquals(newLong, 501);
  }
  
  //enable println to visually check
  public static void setClearMemoryRegionsTests(Memory mem) { 
    int accessCapacity = (int)mem.getCapacity();
    
  //define regions
    int reg1Start = 0;
    int reg1Len = 28;
    int reg2Start = 28;
    int reg2Len = 32;
    
    //set region 1
    byte b1 = 5;
    mem.fill(reg1Start, reg1Len, b1);
    for (int i=reg1Start; i<reg1Len+reg1Start; i++) {
      assertEquals(mem.getByte(i), b1);
    }
    println(mem.toHexString("Region1 to 5", reg1Start, reg1Len));
    
    //set region 2
    byte b2 = 7;
    mem.fill(reg2Start, reg2Len, b2);
    for (int i=reg2Start; i<reg2Len+reg2Start; i++) {
      assertEquals(mem.getByte(i), b2);
    }
    println(mem.toHexString("Region2 to 7", reg2Start, reg2Len));
    
    //clear region 1
    byte zeroByte = 0;
    mem.clear(reg1Start, reg1Len);
    for (int i=reg1Start; i<reg1Len+reg1Start; i++) {
      assertEquals(mem.getByte(i), zeroByte);
    }
    println(mem.toHexString("Region1 cleared", reg1Start, reg1Len));
    
    //clear region 2
    mem.clear(reg2Start, reg2Len);
    for (int i=reg2Start; i<reg2Len+reg2Start; i++) {
      assertEquals(mem.getByte(i), zeroByte);
    }
    println(mem.toHexString("Region2 cleared", reg2Start, reg2Len));
    
    //set all to ones
    byte b4 = 127;
    mem.fill(b4);
    for (int i=0; i<accessCapacity; i++) {
      assertEquals(mem.getByte(i), b4);
    }
    println(mem.toHexString("Region1 + Region2 all ones", 0, accessCapacity));
    
    //clear all
    mem.clear();
    for (int i=0; i<accessCapacity; i++) {
      assertEquals(mem.getByte(i), zeroByte);
    }
    println(mem.toHexString("Region1 + Region2 cleared", 0, accessCapacity));  
  }

  //enable println to visually check  
  public static void toHexStringAllMemTests(Memory mem) { 
    int memCapacity = (int)mem.getCapacity();
    
    for (int i=0; i<memCapacity; i++) {
      mem.putByte(i, (byte)i);
    }
    
    println(mem.toHexString("Check toHexString(0, 48) to integers", 0, memCapacity));
    println(mem.toHexString("Check toHexString(8, 40)", 8, 40));
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
