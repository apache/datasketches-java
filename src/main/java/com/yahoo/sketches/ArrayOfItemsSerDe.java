/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import com.yahoo.memory.Memory;

/**
 * Base class for serializing and deserializing custom types.
 * @param <T> Type of item
 *
 * @author Alexander Saydakov
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

}
