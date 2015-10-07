package com.yahoo.sketches.hll;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRegion;
import com.yahoo.sketches.memory.NativeMemory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PreambleTest
{
  @Test
  public void testSerDe() {
    Preamble preamble = new Preamble.Builder()
        .setLogConfigK((byte) 10)
        .setSeed(Short.MIN_VALUE)
        .setFlags((byte) 12).build();

    byte[] bytes = new byte[10];
    int initOffset = 1;
    int newOffset = preamble.intoByteArray(bytes, initOffset);
    Assert.assertEquals(newOffset, initOffset + Preamble.PERAMBLE_SIZE_BYTES);

    Memory mem = new MemoryRegion(new NativeMemory(bytes), 1, bytes.length - 1);
    Preamble serdePreamble = Preamble.fromMemory(mem);

    Assert.assertEquals(serdePreamble, preamble);
  }

  @Test
  public void testMaxAuxSize() {
    Preamble preamble10 = new Preamble.Builder()
        .setLogConfigK((byte) 10).build();

    final int intByteSize = 4;
    Assert.assertEquals(preamble10.getMaxAuxSize(), 16 * intByteSize);

    Preamble preamble13 = new Preamble.Builder()
        .setLogConfigK((byte) 13).build();
    Assert.assertEquals(preamble13.getMaxAuxSize(), 32 * intByteSize);

    Preamble preamble16 = new Preamble.Builder()
        .setLogConfigK((byte) 16).build();
    Assert.assertEquals(preamble16.getMaxAuxSize(), 256 * intByteSize);

    Preamble preamble26 = new Preamble.Builder()
        .setLogConfigK((byte) 21).build();
    Assert.assertEquals(preamble26.getMaxAuxSize(), 8192 * intByteSize);

  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSharedPreambleTooLarge() {
    Preamble.createSharedPreamble(256);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSharedPreambleTooLarge2() {
    Preamble.createSharedPreamble(50);
  }

  @Test
  public void testHashCodeAndEquals() {
    Preamble preamble = Preamble.createSharedPreamble(13);
    Assert.assertEquals(preamble.hashCode(), Preamble.createSharedPreamble(13).hashCode());
    Assert.assertEquals(preamble, preamble);

    Assert.assertTrue(preamble.equals(Preamble.createSharedPreamble(13)));
    Assert.assertFalse(preamble.equals(null));
  }
}
