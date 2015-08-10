/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.memory;

import sun.misc.Unsafe;

import java.lang.reflect.Constructor;

/**
 * Provides package private reference to the sun.misc.Unsafe class and its key static fields.
 * The internal static initializer also detects whether the methods unique to the Unsafe class in
 * JDK8 are present; if not, methods that are compatible with JDK7 are substituted using an internal
 * interface.  In order for this to work, this library still needs to be compiled using jdk8 
 * but it should be done with both source and target versions of jdk7 specified in pom.xml. 
 * The resultant jar will work on jdk7 and jdk8.
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
  static final JDKCompatibility compatibilityMethods;
  static final int ADDRESS_BYTES;
  static final int BOOLEAN_ARRAY_BASE_OFFSET;
  static final int BYTE_ARRAY_BASE_OFFSET;
  static final int CHAR_ARRAY_BASE_OFFSET;
  static final int DOUBLE_ARRAY_BASE_OFFSET;
  static final int FLOAT_ARRAY_BASE_OFFSET;
  static final int INT_ARRAY_BASE_OFFSET;
  static final int LONG_ARRAY_BASE_OFFSET;
  static final int SHORT_ARRAY_BASE_OFFSET;
//@formatter:off
  static final int BOOLEAN_SIZE    = 1;
  static final int BYTE_SIZE       = 1;
  static final int CHAR_SIZE       = 2;
  static final int DOUBLE_SIZE     = 8;
  static final int FLOAT_SIZE      = 4;
  static final int INT_SIZE        = 4;
  static final int LONG_SIZE       = 8;
  static final int SHORT_SIZE      = 2;
  
  static final int BOOLEAN_SHIFT   = 0;
  static final int BYTE_SHIFT      = 0;
  static final int CHAR_SHIFT      = 1;
  static final int DOUBLE_SHIFT    = 3;
  static final int FLOAT_SHIFT     = 2;
  static final int INT_SHIFT       = 2;
  static final int LONG_SHIFT      = 3;
  static final int SHORT_SHIFT     = 1;
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
      
      ADDRESS_BYTES = unsafe.addressSize(); //4 on 32-bits systems, 8 on 64-bit systems
      
      //These are all 16 on 64-bit systems.
      BOOLEAN_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(boolean[].class);
      BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);
      CHAR_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(char[].class);
      DOUBLE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(double[].class);
      FLOAT_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(float[].class);
      INT_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(int[].class);
      LONG_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(long[].class);
      SHORT_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(short[].class);
      
      boolean onJDK8 = true;
      try {
        unsafe.getClass().getMethod("getAndSetInt", Object.class, long.class, int.class);
      } catch (NoSuchMethodException e) {
        // We must not be on jdk8
        onJDK8 = false;
      }
      
      if (onJDK8) {
        compatibilityMethods = new JDK8Compatible(unsafe);
      } else {
        compatibilityMethods = new JDK7Compatible(unsafe);
      }
      
    }
    catch (Exception e) {
      throw new RuntimeException("Unable to acquire Unsafe. ", e);
    }
  }
  
  private UnsafeUtil() {}
  
  /**
   * Perform bounds checking using java assert (if enabled) checking the offset and length 
   * against the allocated size and zero.
   * @param off the offset
   * @param len the required length
   * @param size the allocated size
   */
  static void assertBounds(final long off, final long len, final long size) {
    assert ((off | len | (off + len) | (size - (off + len))) >= 0) : 
      "offset: "+ off + ", length: "+ len + ", size: "+size;
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
  
  interface JDKCompatibility {
    int getAndAddInt(Object obj, long address, int increment);
    int getAndSetInt(Object obj, long address, int value);
    long getAndAddLong(Object obj, long address, long increment);
    long getAndSetLong(Object obj, long address, long value);
  }
  
  private static class JDK8Compatible implements JDKCompatibility {
    private final Unsafe myUnsafe;
    
    JDK8Compatible(Unsafe unsafe) {
      this.myUnsafe = unsafe;
    }
    
    @Override
    public int getAndAddInt(Object obj, long address, int increment) {
      return myUnsafe.getAndAddInt(obj, address, increment);
    }
    
    @Override
    public int getAndSetInt(Object obj, long address, int value) {
      return myUnsafe.getAndSetInt(obj, address, value);
    }
    
    @Override
    public long getAndAddLong(Object obj, long address, long increment) {
      return myUnsafe.getAndAddLong(obj, address, increment);
    }
    
    @Override
    public long getAndSetLong(Object obj, long address, long value) {
      return myUnsafe.getAndSetLong(obj, address, value);
    }
  }
  
  private static class JDK7Compatible implements JDKCompatibility {
    private final Unsafe myUnsafe;
    
    JDK7Compatible(Unsafe unsafe) {
      this.myUnsafe = unsafe;
    }
    
    @Override
    public int getAndAddInt(Object obj, long address, int increment) {
      int retVal;
      do {
        retVal = myUnsafe.getIntVolatile(obj, address);
      } while(!myUnsafe.compareAndSwapInt(obj, address, retVal, retVal + increment));
      
      return retVal;
    }
    
    @Override
    public int getAndSetInt(Object obj, long address, int value) {
      int retVal;
      do {
        retVal = myUnsafe.getIntVolatile(obj, address);
      } while(!myUnsafe.compareAndSwapInt(obj, address, retVal, value));
      
      return retVal;
    }
    
    @Override
    public long getAndAddLong(Object obj, long address, long increment) {
      long retVal;
      do {
        retVal = myUnsafe.getLongVolatile(obj, address);
      } while(!myUnsafe.compareAndSwapLong(obj, address, retVal, retVal + increment));
      
      return retVal;
    }
    
    @Override
    public long getAndSetLong(Object obj, long address, long value) {
      long retVal;
      do {
        retVal = myUnsafe.getLongVolatile(obj, address);
      } while(!myUnsafe.compareAndSwapLong(obj, address, retVal, value));
      
      return retVal;
    }
  }
}