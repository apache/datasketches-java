package com.yahoo.sketches.hll;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.hash.MurmurHash3;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRegion;
import com.yahoo.sketches.memory.NativeMemory;

public class Preamble {
  public static final byte PREAMBLE_LONGS = 1;
  public static final byte PREAMBLE_VERSION = 8;
  public static final byte HLL_PREAMBLE_FAMILY_ID = (byte) Family.HLL.getID();

  public static final int[] AUX_SIZE = new int[] {
      1, 4, 4, 4, 4, 4, 4, 8, 8, 8,
      16, 16, 32, 32, 64, 128, 256, 512, 1024, 2048,
      4096, 8192, 16384, 32768, 65536, 131072, 262144
  };

  private byte preambleSize;
  private byte version;
  private byte familyId;
  private byte logConfigK;
  private byte flags;
  private short seedHash;

  private Preamble(byte preambleSize, byte version, byte familyId, byte logConfigK, byte flags, short seedHash)
  {
    this.preambleSize = preambleSize;
    this.version = version;
    this.familyId = familyId;
    this.logConfigK = logConfigK;
    this.flags = flags;
    this.seedHash = seedHash;

    if (logConfigK < Interpolation.INTERPOLATION_MIN_LOG_K || logConfigK > Interpolation.INTERPOLATION_MAX_LOG_K) {
      throw new IllegalArgumentException(
          String.format(
              "logConfigK[%s] is out of bounds, should be between [%s] and [%s]",
              logConfigK, Interpolation.INTERPOLATION_MIN_LOG_K, Interpolation.INTERPOLATION_MAX_LOG_K
          )
      );
    }
  }

  public static Preamble fromMemory(Memory memory) {
    Builder builder = new Builder()
        .setPreambleSize(memory.getByte(0))
        .setVersion(memory.getByte(1))
        .setFamilyId(memory.getByte(2))
        .setLogConfigK(memory.getByte(3))
            // Invert the ++ in order to skip over the unused byte.  A bunch of bits are wasted
            // instead of packing the preamble so that the semantics of the various parts of the
            // preamble can be aligned across different sketches.
        .setFlags(memory.getByte(5));

    short seedHash = memory.getShort(6);
    return builder.setSeedHash(seedHash).build();
  }

  /**
   * Computes and checks the 16-bit seed hash from the given long seed.
   * The seed hash may not be zero in order to maintain compatibility with older serialized
   * versions that did not have this concept.
   *
   * @param seed the given seed.
   *
   * @return the seed hash.
   */
  private static short computeSeedHash(long seed) {
    long[] seedArr = {seed};
    short seedHash = (short) ((MurmurHash3.hash(seedArr, 0L)[0]) & 0xFFFFL);
    if (seedHash == 0) {
      throw new IllegalArgumentException(
          "The given seed: " + seed + " produced a seedHash of zero. " +
          "You must choose a different seed."
      );
    }
    return seedHash;
  }


  public static Preamble fromLogK(int logK) {
    if (logK > 255) {
      throw new IllegalArgumentException("logK is greater than a byte, make it smaller");
    }

    byte flags = new PreambleFlags.Builder()
        .setBigEndian(false)
        .setReadOnly(true)
        .setEmpty(true)
        .setSharedPreambleMode(true)
        .setSparseMode(true)
        .setUnionMode(true)
        .setEightBytePadding(false)
        .build();

    short seedHash = computeSeedHash(Util.DEFAULT_UPDATE_SEED);
    return new Builder()
        .setLogConfigK((byte) logK)
        .setFlags(flags)
        .setSeedHash(seedHash)
        .build();
  }

  public byte[] toByteArray() {
    byte[] retVal = new byte[getPreambleSize() << 3];
    intoByteArray(retVal, 0);
    return retVal;
  }

  public int intoByteArray(byte[] bytes, int offset) {
    if (bytes.length - offset < 8) {
      throw new IllegalArgumentException("bytes too small");
    }

    Memory mem = new MemoryRegion(new NativeMemory(bytes), offset, 8);
    mem.putByte(0, getPreambleSize());
    mem.putByte(1, getVersion());
    mem.putByte(2, getFamilyId());
    mem.putByte(3, getLogConfigK());
    mem.putByte(5, getFlags());
    mem.putShort(6, getSeedHash());
    return offset + 8;
  }

  public byte getPreambleSize() {
    return preambleSize;
  }

  public byte getVersion() {
    return version;
  }

  public byte getFamilyId() {
    return familyId;
  }

  public byte getLogConfigK() {
    return logConfigK;
  }

  public int getConfigK() {
    return 1 << logConfigK;
  }

  public int getMaxAuxSize() {
    return AUX_SIZE[logConfigK] << 2;
  }

  public byte getFlags() {
    return flags;
  }

  public short getSeedHash() {
    return seedHash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Preamble preamble = (Preamble) o;

    return familyId == preamble.familyId &&
           flags == preamble.flags &&
           logConfigK == preamble.logConfigK &&
           preambleSize == preamble.preambleSize &&
           seedHash == preamble.seedHash &&
           version == preamble.version;
  }

  @SuppressWarnings("cast")
  @Override
  public int hashCode() {
    int result = (int) preambleSize;
    result = 31 * result + (int) version;
    result = 31 * result + (int) familyId;
    result = 31 * result + (int) logConfigK;
    result = 31 * result + (int) flags;
    result = 31 * result + (int) seedHash;
    return result;
  }

  public static class Builder {
    private byte preambleSize = Preamble.PREAMBLE_LONGS;
    private byte version = Preamble.PREAMBLE_VERSION;
    private byte familyId = Preamble.HLL_PREAMBLE_FAMILY_ID;
    private byte logConfigK = (byte) Integer.numberOfTrailingZeros(Util.DEFAULT_NOMINAL_ENTRIES);
    private byte flags; //TODO needs defaults
    private short seedHash = computeSeedHash(Util.DEFAULT_UPDATE_SEED);

    public Builder setPreambleSize(byte preambleSize) {
      this.preambleSize = preambleSize;
      return this;
    }

    public Builder setVersion(byte version) {
      this.version = version;
      return this;
    }

    public Builder setFamilyId(byte familyId) {
      this.familyId = familyId;
      return this;
    }

    public Builder setLogConfigK(byte logConfigK) {
      this.logConfigK = logConfigK;
      return this;
    }

    public Builder setFlags(byte flags) {
      this.flags = flags;
      return this;
    }

    public Builder setSeed(long seed) {
      return setSeedHash(computeSeedHash(seed));
    }

    public Builder setSeedHash(short seedHash) {
      this.seedHash = seedHash;
      return this;
    }

    public Preamble build() {
      return new Preamble(preambleSize, version, familyId, logConfigK, flags, seedHash);
    }

  }

}