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

package org.apache.datasketches.quantiles;

import static org.apache.datasketches.quantiles.HeapUpdateDoublesSketchTest.buildAndLoadQS;

import java.util.Comparator;

import org.testng.annotations.Test;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class DeprecatedAndMiscTest {

  @SuppressWarnings({ "deprecation", "unused" })
  @Test
  public void checkDeprecatedRankError() {
    DoublesSketch ds = buildAndLoadQS(64, 64);
    double err = ds.getNormalizedRankError();
    err = DoublesSketch.getNormalizedRankError(64);
    DoublesUnion du1 = DoublesUnionBuilder.heapify(ds);

    Memory mem = Memory.wrap(ds.toByteArray());
    DoublesUnion du2 = DoublesUnionBuilder.heapify(mem);

    DoublesUnion du3 = DoublesUnionBuilder.wrap(mem);

    WritableMemory wmem = WritableMemory.wrap(ds.toByteArray());
    DoublesUnion du4 = DoublesUnionBuilder.wrap(wmem);

    ItemsSketch<String> is = ItemsSketch.getInstance(64, Comparator.naturalOrder());
    err = is.getNormalizedRankError();
    err = ItemsSketch.getNormalizedRankError(64);
  }

}
