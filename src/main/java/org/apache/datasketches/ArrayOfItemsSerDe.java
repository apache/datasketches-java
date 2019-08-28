/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches;

import org.apache.datasketches.memory.Memory;

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
