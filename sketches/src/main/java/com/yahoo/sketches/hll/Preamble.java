/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.zeroPad;

import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * 8-byte preamble with similar structure to other sketches.
 */
public final class Preamble {
  static final byte PREAMBLE_LONGS = 1;
  static final byte PREAMBLE_VERSION = 8;
  static final byte HLL_PREAMBLE_FAMILY_ID = (byte) Family.HLL.getID();
  static final String LS = System.getProperty("line.separator");

  //max # of ints required for Aux-exceptions array given lg(k) (0 to 26), valid is only 7 to 21.
  static final int[] AUX_SIZE = new int[] {
      1, 4, 4, 4, 4, 4, 4, 8, 8, 8,
      16, 16, 32, 32, 64, 128, 256, 512, 1024, 2048,
      4096, 8192, 16384, 32768, 65536, 131072, 262144
  };

  private byte preambleLongs; //byte 0
  private byte version;       //byte 1
  private byte familyId;      //byte 2
  private byte logConfigK;    //byte 3
  //                          //byte 4 unused
  private byte flags;         //byte 5
  private short seedHash;     //byte 6-7

  private Preamble(final byte preambleLongs, final byte version, final byte familyId,
      final byte logConfigK, final byte flags, final short seedHash) {
    this.preambleLongs = preambleLongs;
    this.version = version;
    this.familyId = familyId;
    this.logConfigK = logConfigK;
    this.flags = flags;
    this.seedHash = seedHash;
  }

  /**
   * Instantiates a new Preamble from the given Memory
   * @param memory the given Memory
   * @return a new Preamble from the given Memory
   */
  public static Preamble fromMemory(final Memory memory) {
    final Builder builder = new Builder()
        .setPreambleLongs(memory.getByte(0))
        .setVersion(memory.getByte(1))
        .setFamilyId(memory.getByte(2))
        .setLogConfigK(memory.getByte(3))
        // Byte 4 is unused. This allows the preamble to have the same format as other sketches.
        .setFlags(memory.getByte(5));

    final short seedHash = memory.getShort(6);
    return builder.setSeedHash(seedHash).build();
  }

  /**
   * Instantiates a new Preamble with the parameter log_base2 of K.
   * @param logK log_base2 of the desired K.  Must be between 7 and 21, inclusive.
   * @return a new Preamble with the parameter log_base2 of K.
   */
  public static Preamble fromLogK(final int logK) {
    checkLogK(logK);

    final byte flags = 0x0;

    final short seedHash = Util.computeSeedHash(Util.DEFAULT_UPDATE_SEED);
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
    final byte[] retVal = new byte[getPreambleLongs() << 3];
    intoByteArray(retVal, 0);
    return retVal;
  }

  int intoByteArray(final byte[] bytes, final int offset) {
    if ((bytes.length - offset) < 8) {
      throw new SketchesArgumentException("Allocated space must be >= 8 bytes: "
          + (bytes.length - offset));
    }

    final Memory mem = new MemoryRegion(new NativeMemory(bytes), offset, 8);
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

  public boolean isHip() {
    return (flags & 0x1) == 1;
  }

  /**
   * Gets the seed hash
   * @return the seed hash
   */
  public short getSeedHash() {
    return seedHash;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if ((o == null) || (this.getClass() != o.getClass())) {
      return false;
    }

    final Preamble preamble = (Preamble) o;

    return (familyId == preamble.familyId)
       && (flags == preamble.flags)
       && (logConfigK == preamble.logConfigK)
       && (preambleLongs == preamble.preambleLongs)
       && (seedHash == preamble.seedHash)
       && (version == preamble.version);
  }

  @Override
  public int hashCode() {
    int result = preambleLongs;
    result = (31 * result) + version;
    result = (31 * result) + familyId;
    result = (31 * result) + logConfigK;
    result = (31 * result) + flags;
    result = (31 * result) + seedHash;
    return result;
  }

  @Override
  public String toString() {
    final String thisSimpleName = this.getClass().getSimpleName();
    final String flagsStr = zeroPad(Integer.toBinaryString(flags), 8) + ", " + (flags);
    final StringBuilder sb = new StringBuilder();
    sb.append(LS);
    sb.append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append("   Preamble longs          : ").append(preambleLongs).append(LS);
    sb.append("   Preamble version        : ").append(version).append(LS);
    sb.append("   Family ID               : ").append(familyId).append(LS);
    sb.append("   LogK                    : ").append(logConfigK).append(LS);
    sb.append("   Flag bits, value        : ").append(flagsStr).append(LS);
    sb.append("   SeedHash                : ").append(Integer.toHexString(seedHash)).append(LS);
    sb.append(LS);
    return sb.toString();
  }

  private static final void checkLogK(final int logK) {
    final int min = Interpolation.INTERPOLATION_MIN_LOG_K;
    final int max = Interpolation.INTERPOLATION_MAX_LOG_K;
    if ((logK < min) || (logK > max)) {
      throw new SketchesArgumentException( String.format(
        "logConfigK[%s] is out of bounds, should be between [%s] and [%s]", logK, min, max));
    }
  }

  /**
   * Builder for the Preamble
   */
  public static class Builder {
    private byte preambleLongs = Preamble.PREAMBLE_LONGS;
    private byte version = Preamble.PREAMBLE_VERSION;
    private byte familyId = Preamble.HLL_PREAMBLE_FAMILY_ID; //derived from sketches.Family
    private byte logConfigK = (byte) Integer.numberOfTrailingZeros(Util.DEFAULT_NOMINAL_ENTRIES);
    private byte flags; //needs defaults?
    private short seedHash = Util.computeSeedHash(Util.DEFAULT_UPDATE_SEED);

    /**
     * Sets the preamble longs byte
     * @param preambleLongs the size of the preamble in longs
     * @return this Builder
     */
    public Builder setPreambleLongs(final byte preambleLongs) {
      this.preambleLongs = preambleLongs;
      return this;
    }

    /**
     * Sets the serialization version of this Preamble
     * @param version the serialization version
     * @return this Builder
     */
    public Builder setVersion(final byte version) {
      this.version = version;
      return this;
    }

    /**
     * Sets the Family ID for this Preamble
     * @param familyId the Family ID
     * @return this Builder
     */
    public Builder setFamilyId(final byte familyId) {
      this.familyId = familyId;
      return this;
    }

    /**
     * Sets the value of k by using the log_base2 of K
     * @param logConfigK the log_base2 of K. Must be between 7 and 21, inclusive.
     * @return this Builder
     */
    public Builder setLogConfigK(final byte logConfigK) {
      checkLogK(logConfigK);
      this.logConfigK = logConfigK;
      return this;
    }

    /**
     * Sets the flags byte for this Preamble
     * @param flags the flags byte
     * @return this Builder
     */
    public Builder setFlags(final byte flags) {
      this.flags = flags;
      return this;
    }

    /**
     * Sets the seed hash from the given seed
     * @param seed the given seed
     * @return this Builder
     */
    public Builder setSeed(final long seed) {
      return setSeedHash(Util.computeSeedHash(seed));
    }

    /**
     * Sets the seed hash directly from the given seed hash
     * @param seedHash the given seed hash
     * @return this Builder
     */
    public Builder setSeedHash(final short seedHash) {
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
