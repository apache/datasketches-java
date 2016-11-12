package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.sampling.PreambleUtil.RESERVOIR_SIZE_INT;
import static com.yahoo.sketches.sampling.PreambleUtil.RESERVOIR_SIZE_SHORT;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfLongsSerDe;

/**
 * Most parts of the class are tested in the sketch and union tests. This class handles only the
 * edge cases not easily tested elsewhere.
 */
public class VersionConverterTest {
  @Test
  public void checkReadOnlyMemory() {
    int k = 32768;
    short encK = ReservoirSize.computeSize(k);
    ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();

    ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.getInstance(k);

    // get a new byte[], manually revert to v1, then reconstruct
    byte[] sketchBytes = ris.toByteArray(serDe);
    Memory sketchMem = new NativeMemory(sketchBytes);
    revertToV1(sketchMem, encK);

    Memory readOnlyMem = sketchMem.asReadOnlyMemory();

    // read-only input should generate a new Memory
    Memory convertedSketch = VersionConverter.convertSketch1to2(readOnlyMem);
    assertNotEquals(readOnlyMem, convertedSketch);

    // same process on writable Memory should be in-place
    convertedSketch = VersionConverter.convertSketch1to2(sketchMem);
    assertEquals(sketchMem, convertedSketch);
  }

  private Memory revertToV1(Memory mem, short encodedK) {
    mem.putByte(SER_VER_BYTE, (byte) 1);
    mem.putInt(RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    mem.putShort(RESERVOIR_SIZE_SHORT, encodedK);

    return mem;
  }
}
