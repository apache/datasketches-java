package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.sampling.PreambleUtil.RESERVOIR_SIZE_INT;
import static com.yahoo.sketches.sampling.PreambleUtil.RESERVOIR_SIZE_SHORT;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;

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
    byte[] sketchBytesOrig = ris.toByteArray(serDe);

    // get a new byte[], manually revert to v1, then reconstruct
    byte[] sketchBytes = ris.toByteArray(serDe);
    Memory sketchMem = new NativeMemory(sketchBytes);

    sketchMem.putByte(SER_VER_BYTE, (byte) 1);
    sketchMem.putInt(RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    sketchMem.putShort(RESERVOIR_SIZE_SHORT, encK);

    Memory readOnlyMem = sketchMem.asReadOnlyMemory();
    ReservoirItemsSketch<Long> rebuilt = ReservoirItemsSketch.getInstance(readOnlyMem, serDe);
    byte[] rebuiltBytes = rebuilt.toByteArray(serDe);

    assertEquals(sketchBytesOrig.length, rebuiltBytes.length);
    for (int i = 0; i < sketchBytesOrig.length; ++i) {
      assertEquals(sketchBytesOrig[i], rebuiltBytes[i]);
    }

  }
}
