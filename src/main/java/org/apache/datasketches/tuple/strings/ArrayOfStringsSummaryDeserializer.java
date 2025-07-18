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

package org.apache.datasketches.tuple.strings;

import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.tuple.DeserializeResult;
import org.apache.datasketches.tuple.SummaryDeserializer;

/**
 * Implements SummaryDeserializer&lt;ArrayOfStringsSummary&gt;
 * @author Lee Rhodes
 */
public class ArrayOfStringsSummaryDeserializer implements SummaryDeserializer<ArrayOfStringsSummary> {

  @Override
  public DeserializeResult<ArrayOfStringsSummary> heapifySummary(final MemorySegment seg) {
    return ArrayOfStringsSummaryDeserializer.fromMemorySegment(seg);
  }

  /**
   * Also used in test.
   * @param seg the given MemorySegment
   * @return the DeserializeResult
   */
  static DeserializeResult<ArrayOfStringsSummary> fromMemorySegment(final MemorySegment seg) {
    final ArrayOfStringsSummary nsum = new ArrayOfStringsSummary(seg);
    final int totBytes = seg.get(JAVA_INT_UNALIGNED, 0);
    return new DeserializeResult<>(nsum, totBytes);
  }

}
