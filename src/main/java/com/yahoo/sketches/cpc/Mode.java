/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import java.util.HashMap;
import java.util.Map;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * Defines the 9 modes of CPC, which includes the 5 Major Modes plus 4 due to MERGED or HIP.
 *
 * @see PreambleUtil
 * @author Lee Rhodes
 * @author Kevin Lang
 */
enum Mode {
  EMPTY((byte) 0, "EMPTY"),                 //0000

  /**
   * Consists of a Hash Set of collected coupons as pairs packed into integers as
   * row (26 bits), column (6 bits).
   * It is serialized by compressing the Hash Set into a bit-stream.
   * The Preamble includes the HIP registers, KxP and HIP Accum.
   */
  SPARSE_HIP((byte) 2, "SPARSE_HIP"),       //0010

  /**
   * Consists of a Hash Set of collected coupons as pairs packed into integers as
   * row (26 bits), column (6 bits).
   * It is serialized by compressing the Hash Set into a bit-stream.
   * The Preamble does not include the HIP registers.
   */
  SPARSE_MERGED((byte) 3, "SPARSE_MERGED"),   //0011

  /**
   * When live in memory this consists of (1) a <i>k</i> unsigned byte array of length <i>k</i>
   * (2) plus a Hash Set of collected surprising coupons as pairs packed into integers as
   * row (26 bits), column (6 bits).
   * It is serialized by first converting these structures into the SPARSE_HIP Mode.
   */
  HYBRID_HIP((byte) 4, "HYBRID_HIP"),       //0100

  /**
   * When live in memory this consists of (1) a <i>k</i> unsigned byte array of length <i>k</i>
   * (2) plus a Hash Set of collected surprising coupons as pairs packed into integers as
   * row (26 bits), column (6 bits).
   * It is serialized by first converting these structures into the SPARSE_MERGED Mode.
   */
  HYBRID_MERGED((byte) 5, "HYBRID_MERGED"),   //0101

  /**
   * When live in memory this consists of (1) a <i>k</i> unsigned byte array of length <i>k</i>
   * (2) plus a Hash Set of collected surprising coupons as pairs packed into integers as
   * row (26 bits), column (6 bits).
   * It is serialized by compressing both of the above structues into bit-streams.
   * The Preamble includes the HIP registers, KxP and HIP Accum.
   */
  PINNED_HIP((byte) 6, "PINNED_HIP"),       //0110

  /**
   * When live in memory this consists of (1) a <i>k</i> unsigned byte array of length <i>k</i>
   * (2) plus a Hash Set of collected surprising coupons as pairs packed into integers as
   * row (26 bits), column (6 bits).
   * It is serialized by compressing both of the above structues into bit-streams.
   * The Preamble does not include the HIP registers.
   */
  PINNED_MERGED((byte) 7, "PINNED_MERGED"),   //0111

  /**
   * When live in memory this consists of (1) a <i>k</i> unsigned byte array of length <i>k</i>
   * (2) plus a Hash Set of collected surprising coupons as pairs packed into integers as
   * row (26 bits), column (6 bits).
   * It is serialized by compressing both of the above structues into bit-streams.
   * The Preamble includes the HIP registers, KxP and HIP Accum.
   */
  SLIDING_HIP((byte) 8, "SLIDING_HIP"),     //1000

  /**
   * When live in memory this consists of (1) a <i>k</i> unsigned byte array of length <i>k</i>
   * (2) plus a Hash Set of collected surprising coupons as pairs packed into integers as
   * row (26 bits), column (6 bits).
   * It is serialized by compressing both of the above structues into bit-streams.
   * The Preamble does not include the HIP registers.
   */
  SLIDING_MERGED((byte) 9, "SLIDING_MERGED"); //1001

  private static final Map<Byte, Mode> lookupID = new HashMap<>();
  private static final Map<String, Mode> lookupModeName = new HashMap<>();
  private byte id_;
  private String modeName_;

  static {
    for (Mode m : values()) {
      lookupID.put(m.getID(), m);
      lookupModeName.put(m.getModeName().toUpperCase(), m);
    }
  }

  private Mode(final byte id, final String modeName) {
    id_ = id;
    modeName_ = modeName.toUpperCase();
  }

  /**
   * Returns the byte ID for this Mode
   * @return the byte ID for this Mode
   */
  public byte getID() {
    return id_;
  }

  /**
  *
  * @param id the given id, a value &lt; 128.
  */
  public void checkModeID(final byte id) {
    if (id != id_) {
      throw new SketchesArgumentException(
          "Possible Corruption: This Mode " + toString()
            + " does not match the ID of the given Mode: " + idToMode(id).toString());
    }
  }

  /**
   * Returns the name for this Mode
   * @return the name for this Mode
   */
  public String getModeName() {
    return modeName_;
  }

  @Override
  public String toString() {
    return modeName_;
  }

  /**
   * Returns the Mode given the ID
   * @param id the given ID
   * @return the Mode given the ID
   */
  public static Mode idToMode(final byte id) {
    final Mode m = lookupID.get(id);
    if (m == null) {
      throw new SketchesArgumentException("Possible Corruption: Illegal Mode ID: " + id);
    }
    return m;
  }

  /**
   * Returns the Mode given the Mode name
   * @param modeName the Mode name
   * @return the Mode given the Mode name
   */
  public static Mode stringToMode(final String modeName) {
    final Mode m = lookupModeName.get(modeName.toUpperCase());
    if (m == null) {
      throw new SketchesArgumentException("Possible Corruption: Illegal Mode Name: " + modeName);
    }
    return m;
  }

}
