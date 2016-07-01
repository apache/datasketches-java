/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import com.yahoo.sketches.memory.Memory;

/**
 * Base class for serializing and deserializing custom types.
 * @param <T> Type of item
 */
public abstract class ArrayOfItemsSerDe<T> {

  /**
   * Serialize an array of items to byte array.
   * The size of the array doesn't need to be serialized.
   * This method is called by the sketch serialization process.
   * 
   * @param items array of items to be serialized
   * @return serialized representation of the given array of items
   */
  public abstract byte[] serializeToByteArray(T[] items);

  /**
   * Deserialize an array of items from a given Memory object.
   * This method is called by the sketch deserialization process.
   * 
   * @param mem Memory containing a serialized array of items
   * @param numItems number of items in the serialized array
   * @return deserialized array of items
   */
  public abstract T[] deserializeFromMemory(Memory mem, int numItems);

  /**
   * Provides a simple mechanism to check compatibility between SerDe implementations.
   * 
   * You will need to override this in the following cases:
   * <ul><li>If you want to rename the class or change its package hierarchy and keep the ID the 
   * same, which enables compatible deserialization of binary images that were serialized with a
   * different class name or package hierarchy.</li>
   * <li>If you wish to change the binary layout of the serialization and don't want to change the 
   * class name or package hierarchy, you will need to change the returned code.</li>
   * </ul>
   * @return a unique identifier of this SerDe
   */
  public short getId() {
    /*
     * Note that the hashCode() of a String is strictly a function of the content of the String
     * and will be the same across different JVMs. This is not the case for Object.hashCode(), 
     * which generally computes the hash code from the native internal address of the object and
     * will be DIFFERENT when computed on different JVMs. So if you override this method, make 
     * sure it will be repeatable across JVMs.
     */
    return (short) getClass().getName().hashCode();
  }
}
