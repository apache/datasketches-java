/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import com.yahoo.sketches.memory.Memory;

/**
 * The API for Union operations
 * 
 * @author Lee Rhodes
 */
public interface Union {

  /**
   * Union the given sketch with the internal state.
   * This method can be repeatedly called.
   * If the given sketch is null it is interpreted as an empty sketch.
   * 
   * @param sketchIn The incoming sketch.
   */  
  void update(Sketch sketchIn);
  
  /**
   * Union the given sketch with the internal state.
   * This method can be repeatedly called. This method only works with Memory sketch objects
   * that are in Compact and Ordered form.
   * If the given sketch is null it is interpreted as an empty sketch.
   * @param mem Memory image of sketch to be merged
   */
  void update(Memory mem);
  
  /**
   * Present this union with a long.
   * 
   * @param datum The given long datum.
   */
  void update(long datum);
  
  /**
   * Present this union with the given double (or float) datum. 
   * The double will be converted to a long using Double.doubleToLongBits(datum), 
   * which normalizes all NaN values to a single NaN representation. 
   * Plus and minus zero will be normalized to plus zero. 
   * The special floating-point values NaN and +/- Infinity are treated as distinct.
   * 
   * @param datum The given double datum.
   */
  void update(double datum);
  
  /**
   * Present this union with the given String. 
   * The string is converted to a byte array using UTF8 encoding. 
   * If the string is null or empty no update attempt is made and the method returns.
   * 
   * @param datum The given String.
   */
  void update(String datum);
  
  /**
   * Present this union with the given byte array. 
   * If the byte array is null or empty no update attempt is made and the method returns.
   * 
   * @param data The given byte array.
   */
  void update(byte[] data);
  
  /**
   * Present this union with the given integer array. 
   * If the integer array is null or empty no update attempt is made and the method returns.
   * 
   * @param data The given int array.
   */
  void update(int[] data);
  
  /**
   * Present this union with the given long array. 
   * If the long array is null or empty no update attempt is made and the method returns.
   * 
   * @param data The given long array.
   */
  public void update(long[] data);
  
  /**
   * Gets the result of this operation as a CompactSketch of the chosen form
   * @param dstOrdered 
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>
   * 
   * @param dstMem 
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   * 
   * @return the result of this operation as a CompactSketch of the chosen form
   */
  CompactSketch getResult(boolean dstOrdered, Memory dstMem);
  
  /**
   * Gets the result of this operation as an ordered CompactSketch on the Java heap
   * @return the result of this operation as an ordered CompactSketch on the Java heap
   */
  CompactSketch getResult();
  /**
   * Returns a byte array image of this Union object
   * @return a byte array image of this Union object
   */
  byte[] toByteArray();
  
  /**
   * Rebuilds the hash table to remove dirty values or to reduce the size
   * to nominal entries.
   * @return this union
   */
  public Union rebuild();
  
  /**
   * Resets this Union. The seed remains intact, otherwise reverts back to its virgin state.
   */
  void reset();
}
