package com.yahoo.sketches;

import com.yahoo.sketches.memory.Memory;

/**
 * Interface for serializing and deserializing custom types for use in FrequentItemsSketch
 * @param <T> Type of item
 */
public interface ArrayOfItemsSerDe<T> {

  /**
   * This is to serialize an array of items to byte array.
   * The size of the array doesn't need to be serialized.
   * It will be provided to deserialize method.
   * @param items array of items to be serialized
   * @return serialized representation of the given array of items
   */
  byte[] serializeToByteArray(T[] items);

  /**
   * This is to deserialize an array of items from a given Memory object
   * @param mem memory containing a serialized array of items
   * @param numItems number of items in the serialized array
   * @return deserialized array of items
   */
  T[] deserializeFromMemory(Memory mem, int numItems);

  /**
   * This is a unique identifier of a particular ArrayOfItemsSerDe.
   * It will be used by FrequentItemsSketch to check compatibility of serialized data.
   * Be careful not to use the same type for incompatible representations.
   * @return ArrayOfItemsSerDe type
   */
  byte getType();
}
