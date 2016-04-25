/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.memory;

import java.lang.reflect.Constructor;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class UnsafeUtilTest {

  @Test
  public void checkJDK7Compatible() throws Exception {
    sun.misc.Unsafe myUnsafe = UnsafeUtil.unsafe;
    Class<?> innerClass = Class.forName("com.yahoo.sketches.memory.UnsafeUtil$JDK7Compatible");
    Constructor<?> ctor = innerClass.getDeclaredConstructor(myUnsafe.getClass());
    ctor.setAccessible(true);
    UnsafeUtil.JDKCompatibility jdkCompat = (UnsafeUtil.JDKCompatibility) ctor.newInstance(myUnsafe);
    byte[] byteArr = new byte[8];
    NativeMemory mem = new NativeMemory(byteArr);
    long orig, stor;

    mem.putInt(0, 1);
    orig = jdkCompat.getAndAddInt(byteArr, 16, 5); //java array offset = 16
    stor = mem.getInt(0);
    println("Orig: " + orig);
    println("Stor: " + stor);
    assertEquals(orig, 1);
    assertEquals(stor, 6);

    orig = jdkCompat.getAndSetInt(byteArr, 16, 5);
    stor = mem.getInt(0);
    println("Orig: " + orig);
    println("Stor: " + stor);
    assertEquals(orig, 6);
    assertEquals(stor, 5);
    
    mem.putLong(0, 1L);
    orig = jdkCompat.getAndAddLong(byteArr, 16, 5);
    stor = mem.getInt(0);
    println("Orig: " + orig);
    println("Stor: " + stor);
    assertEquals(orig, 1);
    assertEquals(stor, 6);

    orig = jdkCompat.getAndSetLong(byteArr, 16, 5);
    stor = mem.getInt(0);
    println("Orig: " + orig);
    println("Stor: " + stor);
    assertEquals(orig, 6);
    assertEquals(stor, 5);
  }
  
  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.err.println(s); //disable here
  }
  
}
