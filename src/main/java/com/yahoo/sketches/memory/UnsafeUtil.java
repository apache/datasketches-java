/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.memory;

import sun.misc.Unsafe;

import java.lang.reflect.Constructor;

/**
 * Provides package private reference to the sun.misc.Unsafe class and its key static fields.
 * 
 * <p><b>NOTE:</b> Native/Direct memory acquired using Unsafe may have garbage in it. 
 * It is the responsibility of the using class to clear this memory, if required, 
 * and to call <i>freeMemory()</i> when done.
 * 
 * @author Lee Rhodes
 */

@SuppressWarnings("restriction")
final class UnsafeUtil {
  static final Unsafe unsafe;
  
  static final int ADDRESS_SIZE; //not an indicator of whether compressed references are used.
  
  //varies depending on compressed ref, 16 or 24 for 64-bit systems
  static final int ARRAY_BOOLEAN_BASE_OFFSET;
  static final int ARRAY_BYTE_BASE_OFFSET;
  static final int ARRAY_SHORT_BASE_OFFSET;
  static final int ARRAY_CHAR_BASE_OFFSET;
  static final int ARRAY_INT_BASE_OFFSET;
  static final int ARRAY_LONG_BASE_OFFSET;
  static final int ARRAY_FLOAT_BASE_OFFSET;
  static final int ARRAY_DOUBLE_BASE_OFFSET;
  static final int ARRAY_OBJECT_BASE_OFFSET;
  
//@formatter:off
  static final int ARRAY_BOOLEAN_INDEX_SCALE; // 1
  static final int ARRAY_BYTE_INDEX_SCALE;    // 1
  static final int ARRAY_SHORT_INDEX_SCALE;   // 2
  static final int ARRAY_CHAR_INDEX_SCALE;    // 2
  static final int ARRAY_INT_INDEX_SCALE;     // 4
  static final int ARRAY_LONG_INDEX_SCALE;    // 8
  static final int ARRAY_FLOAT_INDEX_SCALE;   // 4
  static final int ARRAY_DOUBLE_INDEX_SCALE;  // 8
  static final int ARRAY_OBJECT_INDEX_SCALE;  // varies, 4 or 8 depending on compressed ref
  
  //Used to convert "type" to bytes:  bytes = longs << LONG_SHIFT
  static final int BOOLEAN_SHIFT   = 0;
  static final int BYTE_SHIFT      = 0;
  static final int SHORT_SHIFT     = 1;
  static final int CHAR_SHIFT      = 1;
  static final int INT_SHIFT       = 2;
  static final int LONG_SHIFT      = 3;
  static final int FLOAT_SHIFT     = 2;
  static final int DOUBLE_SHIFT    = 3;
  
  public static final String LS = System.getProperty("line.separator");
  
//@formatter:on

  /** 
   * This number limits the number of bytes to copy per call to Unsafe's copyMemory method. 
   * A limit is imposed to allow for safepoint polling during a large copy.
   */
  static final long UNSAFE_COPY_THRESHOLD = 1L << 20; //2^20

  static {
    try {
      //should work across JVMs, e.g., with Android:
      Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
      unsafeConstructor.setAccessible(true);
      unsafe = unsafeConstructor.newInstance();

      // Alternative, but may not work across different JVMs.
//    Field field = Unsafe.class.getDeclaredField("theUnsafe");
//    field.setAccessible(true);
//    unsafe = (Unsafe) field.get(null);

      //4 on 32-bits systems, 8 on 64-bit systems.  Not an indicator of compressed ref (Oop)
      ADDRESS_SIZE = unsafe.addressSize(); 

      ARRAY_BOOLEAN_BASE_OFFSET = unsafe.arrayBaseOffset(boolean[].class);
      ARRAY_BYTE_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);
      ARRAY_SHORT_BASE_OFFSET = unsafe.arrayBaseOffset(short[].class);
      ARRAY_CHAR_BASE_OFFSET = unsafe.arrayBaseOffset(char[].class);
      ARRAY_INT_BASE_OFFSET = unsafe.arrayBaseOffset(int[].class);
      ARRAY_LONG_BASE_OFFSET = unsafe.arrayBaseOffset(long[].class);
      ARRAY_FLOAT_BASE_OFFSET = unsafe.arrayBaseOffset(float[].class);
      ARRAY_DOUBLE_BASE_OFFSET = unsafe.arrayBaseOffset(double[].class);
      ARRAY_OBJECT_BASE_OFFSET = unsafe.arrayBaseOffset(Object[].class);
      
      ARRAY_BOOLEAN_INDEX_SCALE = unsafe.arrayIndexScale(boolean[].class);
      ARRAY_BYTE_INDEX_SCALE = unsafe.arrayIndexScale(byte[].class);
      ARRAY_SHORT_INDEX_SCALE = unsafe.arrayIndexScale(short[].class);
      ARRAY_CHAR_INDEX_SCALE = unsafe.arrayIndexScale(char[].class);
      ARRAY_INT_INDEX_SCALE = unsafe.arrayIndexScale(int[].class);
      ARRAY_LONG_INDEX_SCALE = unsafe.arrayIndexScale(long[].class);
      ARRAY_FLOAT_INDEX_SCALE = unsafe.arrayIndexScale(float[].class);
      ARRAY_DOUBLE_INDEX_SCALE = unsafe.arrayIndexScale(double[].class);
      ARRAY_OBJECT_INDEX_SCALE = unsafe.arrayIndexScale(Object[].class);
      
    }
    catch (Exception e) {
      throw new RuntimeException("Unable to acquire Unsafe. ", e);
    }
  }

  private UnsafeUtil() {}

  /**
   * Perform bounds checking using java assert (if enabled) checking the requested offset and length 
   * against the allocated size. If any of the parameters are negative the assert will be thrown. 
   * @param reqOff the requested offset
   * @param reqLen the requested length
   * @param allocSize the allocated size. 
   */
  static void assertBounds(final long reqOff, final long reqLen, final long allocSize) {
    assert ((reqOff | reqLen | (reqOff + reqLen) | (allocSize - (reqOff + reqLen))) >= 0) : 
      "offset: "+ reqOff + ", reqLength: "+ reqLen+ ", size: "+allocSize;
  }

  /**
   * Return true if the two memory regions do not overlap
   * @param srcOff the start of the source region
   * @param dstOff the start of the destination region
   * @param length the length of both regions
   * @return true if the two memory regions do not overlap
   */
  static boolean checkOverlap(final long srcOff, final long dstOff, final long length) {
    long min = Math.min(srcOff, dstOff);
    long max = Math.max(srcOff, dstOff);
    return (min + length) <= max;
  }

}
