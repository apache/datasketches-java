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

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.DeserializeResult;
import org.apache.datasketches.tuple.SummaryDeserializer;

/**
 * @author Lee Rhodes
 */
public class ArrayOfStringsSummaryDeserializer implements SummaryDeserializer<ArrayOfStringsSummary> {

  @Override
  public DeserializeResult<ArrayOfStringsSummary> heapifySummary(final Memory mem) {
    return ArrayOfStringsSummaryDeserializer.fromMemory(mem);
  }

  /**
   * Also used in test.
   * @param mem the given memory
   * @return the DeserializeResult
   */
  static DeserializeResult<ArrayOfStringsSummary> fromMemory(final Memory mem) {
    final ArrayOfStringsSummary nsum = new ArrayOfStringsSummary(mem);
    final int totBytes = mem.getInt(0);
    return new DeserializeResult<>(nsum, totBytes);
  }

}
