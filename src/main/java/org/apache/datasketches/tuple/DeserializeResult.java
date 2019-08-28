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

package org.apache.datasketches.tuple;

/**
 * Returns an object and its size in bytes as a result of a deserialize operation
 * @param <T> Type of object
 */
public class DeserializeResult<T> {
  private final T object;
  private final int size;

  /**
   * Creates an instance.
   * @param object Deserialized object.
   * @param size Deserialized size in bytes.
   */
  public DeserializeResult(final T object, final int size) {
    this.object = object;
    this.size = size;
  }

  /**
   * @return Deserialized object
   */
  public T getObject() {
    return object;
  }

  /**
   * @return Size in bytes occupied by the object in the serialized form
   */
  public int getSize() {
    return size;
  }
}
