/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.hash.MurmurHash3;

/**
 * @author Kevin Lang
 */
public final class Preamble {
  static final byte PREAMBLE_LONGS = 1;
  static final byte PREAMBLE_VERSION = 8;
  static final byte HLL_PREAMBLE_FAMILY_ID = (byte) Family.HLL.getID();

  static final int[] AUX_SIZE = new int[] {
      1, 4, 4, 4, 4, 4, 4, 8, 8, 8,
      16, 16, 32, 32, 64, 128, 256, 512, 1024, 2048,
      4096, 8192, 16384, 32768, 65536, 131072, 262144
  };

  private byte preambleLongs;
  private byte version;
  private byte familyId;
  private byte logConfigK;
  private byte flags;
  private short seedHash;

  private Preamble(
      byte preambleLongs, byte version, byte familyId, byte logConfigK, byte flags, short seedHash) {
    this.preambleLongs = preambleLongs;
    this.version = version;
    this.familyId = familyId;
    this.logConfigK = logConfigK;
    this.flags = flags;
    this.seedHash = seedHash;

    if ((logConfigK < Interpolation.INTERPOLATION_MIN_LOG_K) 
        || (logConfigK > Interpolation.INTERPOLATION_MAX_LOG_K)) {
      throw new SketchesArgumentException(
          String.format(
              "logConfigK[%s] is out of bounds, should be between [%s] and [%s]",
              logConfigK, Interpolation.INTERPOLATION_MIN_LOG_K, Interpolation.INTERPOLATION_MAX_LOG_K
          )
      );
    }
  }
  
  /**
   * Instantiates a new Preamble from the given Memory
   * @param memory the given Memory
   * @return a new Preamble from the given Memory
   */
  public static Preamble fromMemory(Memory memory) {
    Builder builder = new Builder()
        .setPreambleLongs(memory.getByte(0))
        .setVersion(memory.getByte(1))
        .setFamilyId(memory.getByte(2))
        .setLogConfigK(memory.getByte(3))
        // Invert the ++ in order to skip over the unused byte.  Some bits are wasted
        // instead of packing the preamble so that the semantics of the various parts of the
        // preamble can be aligned across different types of sketches.
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
      throw new SketchesArgumentException(
          "The given seed: " + seed + " produced a seedHash of zero. " 
              + "You must choose a different seed."
      );
    }
    return seedHash;
  }

  /**
   * Instantiates a new Preamble with the parameter log_base2 of K.
   * @param logK log_base2 of the desired K
   * @return a new Preamble with the parameter log_base2 of K.
   */
  public static Preamble fromLogK(int logK) {
    if (logK > 255) {
      throw new SketchesArgumentException("logK is greater than a byte, make it smaller");
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
  
  /**
   * Serializes this Preamble to a byte array.
   * @return this Preamble as a byte array
   */
  public byte[] toByteArray() {
    byte[] retVal = new byte[getPreambleLongs() << 3];
    intoByteArray(retVal, 0);
    return retVal;
  }

  int intoByteArray(byte[] bytes, int offset) {
    if ((bytes.length - offset) < 8) {
      throw new SketchesArgumentException("bytes too small");
    }

    Memory mem = new MemoryRegion(new NativeMemory(bytes), offset, 8);
    mem.putByte(0, getPreambleLongs());
    mem.putByte(1, getVersion());
    mem.putByte(2, getFamilyId());
    mem.putByte(3, getLogConfigK());
    mem.putByte(5, getFlags());
    mem.putShort(6, getSeedHash());
    return offset + 8;
  }

  /** 
   * Gets the size of the Preamble in longs
   * @return the size of the Preamble in longs
   */
  public byte getPreambleLongs() {
    return preambleLongs;
  }

  /**
   * Gets the serialization version of this Preamble
   * @return the serialization version of this Preamble
   */
  public byte getVersion() {
    return version;
  }

  /**
   * Gets the Family ID
   * @return the Family ID
   */
  public byte getFamilyId() {
    return familyId;
  }

  /**
   * Gets the log_base2 of the configured K
   * @return the log_base2 of the configured K
   */
  public byte getLogConfigK() {
    return logConfigK;
  }
  
  /**
   * Gets the configured K
   * @return the configured K
   */
  public int getConfigK() {
    return 1 << logConfigK;
  }
  
  /**
   * Gets the maximum auxiliary size
   * @return the maximum auxiliary size
   */
  public int getMaxAuxSize() {
    return AUX_SIZE[logConfigK] << 2;
  }

  /**
   * Gets the flags byte
   * @return the flags byte
   */
  public byte getFlags() {
    return flags;
  }

  /**
   * Gets the seed hash
   * @return the seed hash
   */
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

    return familyId == preamble.familyId 
       && flags == preamble.flags 
       && logConfigK == preamble.logConfigK 
       && preambleLongs == preamble.preambleLongs 
       && seedHash == preamble.seedHash 
       && version == preamble.version;
  }

  @SuppressWarnings("cast")
  @Override
  public int hashCode() {
    int result = (int) preambleLongs;
    result = 31 * result + (int) version;
    result = 31 * result + (int) familyId;
    result = 31 * result + (int) logConfigK;
    result = 31 * result + (int) flags;
    result = 31 * result + (int) seedHash;
    return result;
  }

  /**
   * Builder for the Preamble
   */
  public static class Builder {
    private byte preambleLongs = Preamble.PREAMBLE_LONGS;
    private byte version = Preamble.PREAMBLE_VERSION;
    private byte familyId = Preamble.HLL_PREAMBLE_FAMILY_ID;
    private byte logConfigK = (byte) Integer.numberOfTrailingZeros(Util.DEFAULT_NOMINAL_ENTRIES);
    private byte flags; //needs defaults?
    private short seedHash = computeSeedHash(Util.DEFAULT_UPDATE_SEED);
    
    /**
     * Sets the preamble longs byte
     * @param preambleLongs the size of the preamble in longs
     * @return this Builder
     */
    public Builder setPreambleLongs(byte preambleLongs) {
      this.preambleLongs = preambleLongs;
      return this;
    }

    /**
     * Sets the serialization version of this Preamble
     * @param version the serialization version
     * @return this Builder
     */
    public Builder setVersion(byte version) {
      this.version = version;
      return this;
    }
    
    /**
     * Sets the Family ID for this Preamble
     * @param familyId the Family ID
     * @return this Builder
     */
    public Builder setFamilyId(byte familyId) {
      this.familyId = familyId;
      return this;
    }

    /**
     * Sets the value of k by using the log_base2 of K
     * @param logConfigK the log_base2 of K
     * @return this Builder
     */
    public Builder setLogConfigK(byte logConfigK) {
      this.logConfigK = logConfigK;
      return this;
    }

    /**
     * Sets the flags byte for this Preamble
     * @param flags the flags byte
     * @return this Builder
     */
    public Builder setFlags(byte flags) {
      this.flags = flags;
      return this;
    }
    
    /**
     * Sets the seed hash from the given seed
     * @param seed the given seed
     * @return this Builder
     */
    public Builder setSeed(long seed) {
      return setSeedHash(computeSeedHash(seed));
    }
    
    /**
     * Sets the seed hash directly from the given seed hash
     * @param seedHash the given seed hash
     * @return this Builder
     */
    public Builder setSeedHash(short seedHash) {
      this.seedHash = seedHash;
      return this;
    }
    
    /**
     * Build this Preamble
     * @return a new Preamble
     */
    public Preamble build() {
      return new Preamble(preambleLongs, version, familyId, logConfigK, flags, seedHash);
    }

  } //End Builder

}
