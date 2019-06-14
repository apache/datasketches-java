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

package com.yahoo.sketches.sampling;
/*
import static com.yahoo.sketches.sampling.PreambleUtil.RESERVOIR_SIZE_INT;
import static com.yahoo.sketches.sampling.PreambleUtil.RESERVOIR_SIZE_SHORT;
import static com.yahoo.sketches.sampling.PreambleUtil.SERDE_ID_SHORT;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfLongsSerDe;
*/

/**
 * Most parts of the class are tested in the sketch and union tests. This class handles only the
 * edge cases not easily tested elsewhere.
 */
public class VersionConverterTest {
/*
  @Test
  public void checkReadOnlyMemory() {
    final int k = 32768;
    final short encK = ReservoirSize.computeSize(k);
    final ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();

    final ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.newInstance(k);

    // get a new byte[], manually revert to v1, then reconstruct
    final byte[] sketchBytes = ris.toByteArray(serDe);
    final Memory sketchMem = new NativeMemory(sketchBytes);
    revertToV1(sketchMem, encK);

    final Memory readOnlyMem = sketchMem.asReadOnlyMemory();

    // read-only input should generate a new Memory
    Memory convertedSketch = VersionConverter.convertSketch1to2(readOnlyMem);
    assertNotEquals(readOnlyMem, convertedSketch);

    // same process on writable Memory should be in-place
    convertedSketch = VersionConverter.convertSketch1to2(sketchMem);
    assertEquals(sketchMem, convertedSketch);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void checkDeprecatedPreambleMethods() {
    final byte[] headerBytes = new byte[SERDE_ID_SHORT + Short.BYTES];
    final Memory headerMem = new NativeMemory(headerBytes);
    final ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();

    final Object memObj = headerMem.array(); // may be null
    final long memAddr = headerMem.getCumulativeOffset(0L);

    // SerDe ID
    PreambleUtil.insertSerDeId(memObj, memAddr, serDe.getId());
    assertEquals(serDe.getId(), PreambleUtil.extractSerDeId(memObj, memAddr));
  }

  private static Memory revertToV1(final Memory mem, final short encodedK) {
    mem.putByte(SER_VER_BYTE, (byte) 1);
    mem.putInt(RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    mem.putShort(RESERVOIR_SIZE_SHORT, encodedK);

    return mem;
  }
  */
}
