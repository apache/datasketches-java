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

package org.apache.datasketches.tuple2;

import java.lang.foreign.MemorySegment;

/**
 * Interface for deserializing user-defined Summary
 * @param <S> type of Summary
 */
public interface SummaryDeserializer<S extends Summary> {

  /**
   * This is to create an instance of a Summary given a serialized representation.
   * The user may assume that the start of the given MemorySegment is the correct place to start
   * deserializing. However, the user must be able to determine the number of bytes required to
   * deserialize the summary as the capacity of the given MemorySegment may
   * include multiple such summaries and may be much larger than required for a single summary.
   * @param seg MemorySegment object with serialized representation of a Summary
   * @return DeserializedResult object, which contains a Summary object and number of bytes read
   * from the MemorySegment
   */
  public DeserializeResult<S> heapifySummary(MemorySegment seg);

}
