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

package org.apache.datasketches.theta;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.apache.datasketches.common.Family.idToFamily;
import static org.apache.datasketches.common.Util.LONG_MAX_VALUE_AS_DOUBLE;
import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.ceilingPowerOf2;
import static org.apache.datasketches.common.Util.zeroPad;
import static org.apache.datasketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.thetacommon.HashOperations.count;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.thetacommon.BinomialBoundsN;
import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * The top-level class for all theta sketches. This class is never constructed directly.
 * Use the UpdateSketch.builder() methods to create UpdateSketches.
 *
 * @author Lee Rhodes
 */
public abstract class Sketch implements MemorySegmentStatus {

  Sketch() {}

  //public static factory constructor-type methods

  /**
   * Heapify takes the sketch image in MemorySegment and instantiates an on-heap Sketch.
   *
   * <p>The resulting sketch will not retain any link to the source MemorySegment.</p>
   *
   * <p>For Update Sketches this method checks if the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a></p>
   * was used to create the source MemorySegment image.
   *
   * <p>For Compact Sketches this method assumes that the sketch image was created with the
   * correct hash seed, so it is not checked. SerialVersion 1 sketches (pre-open-source) cannot be checked.</p>
   *
   * @param srcSeg an image of a Sketch.
   *
   * @return a Sketch on the heap.
   */
  public static Sketch heapify(final MemorySegment srcSeg) {
    final byte familyID = srcSeg.get(JAVA_BYTE, FAMILY_BYTE);
    final Family family = idToFamily(familyID);
    if (family == Family.COMPACT) {
      return CompactSketch.heapify(srcSeg);
    }
    return heapifyUpdateFromMemorySegment(srcSeg, Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify takes the sketch image in MemorySegment and instantiates an on-heap Sketch.
   *
   * <p>The resulting sketch will not retain any link to the source MemorySegment.</p>
   *
   * <p>For Update Sketches this method checks if the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a></p>
   * was used to create the source MemorySegment image.
   *
   * <p>For Compact Sketches this method assumes that the sketch image was created with the
   * correct hash seed, so it is not checked. SerialVersion 1 sketches (pre-open-source) cannot be checked.</p>
   *
   * @param srcSeg an image of a Sketch that was created using the given expectedSeed.
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   *  <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * Compact sketches store a 16-bit hash of the seed, but not the seed itself.
   * @return a Sketch on the heap.
   */
  public static Sketch heapify(final MemorySegment srcSeg, final long expectedSeed) {
    final byte familyID = srcSeg.get(JAVA_BYTE, FAMILY_BYTE);
    final Family family = idToFamily(familyID);
    if (family == Family.COMPACT) {
      return CompactSketch.heapify(srcSeg, expectedSeed);
    }
    return heapifyUpdateFromMemorySegment(srcSeg, expectedSeed);
  }

  /**
   * Wrap takes the sketch image in the given MemorySegment and refers to it directly.
   * There is no data copying onto the java heap.
   * The wrap operation enables fast read-only merging and access to all the public read-only API.
   *
   * <p>Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
   * been explicitly stored as direct sketches can be wrapped.
   * Wrapping earlier serial version sketches will result in a on-heap CompactSketch
   * where all data will be copied to the heap. These early versions were never designed to "wrap".</p>
   *
   * <p>Wrapping any subclass of this class that is empty or contains only a single item will
   * result in on-heap equivalent forms of empty and single item sketch respectively.
   * This is actually faster and consumes less overall space.</p>
   *
   * <p>For Update Sketches this method checks if the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a></p>
   * was used to create the source MemorySegment image.
   *
   * <p>For Compact Sketches this method assumes that the sketch image was created with the
   * correct hash seed, so it is not checked.  SerialVersion 1 (pre-open-source) sketches cannot be checked.</p>
   *
   * @param srcSeg a MemorySegment with an image of a Sketch.
   * @return a read-only Sketch backed by the given MemorySegment
   */
  public static Sketch wrap(final MemorySegment srcSeg) {
    final int  preLongs = srcSeg.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE) & 0X3F;
    final int serVer = srcSeg.get(JAVA_BYTE, SER_VER_BYTE) & 0XFF;
    final int familyID = srcSeg.get(JAVA_BYTE, FAMILY_BYTE) & 0XFF;
    final Family family = Family.idToFamily(familyID);
    if (family == Family.QUICKSELECT) {
      if (serVer == 3 && preLongs == 3) {
        return DirectQuickSelectSketchR.readOnlyWrap(srcSeg, Util.DEFAULT_UPDATE_SEED);
      } else {
        throw new SketchesArgumentException(
            "Corrupted: " + family + " family image: must have SerVer = 3 and preLongs = 3");
      }
    }
    if (family == Family.COMPACT) {
      return CompactSketch.wrap(srcSeg);
    }
    throw new SketchesArgumentException(
        "Cannot wrap family: " + family + " as a Sketch");
  }

  /**
   * Wrap takes the sketch image in the given MemorySegment and refers to it directly.
   * There is no data copying onto the java heap.
   * The wrap operation enables fast read-only merging and access to all the public read-only API.
   *
   * <p>Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
   * been explicitly stored as direct sketches can be wrapped.
   * Wrapping earlier serial version sketches will result in a on-heap CompactSketch
   * where all data will be copied to the heap. These early versions were never designed to "wrap".</p>
   *
   * <p>Wrapping any subclass of this class that is empty or contains only a single item will
   * result in on-heap equivalent forms of empty and single item sketch respectively.
   * This is actually faster and consumes less overall space.</p>
   *
   * <p>For Update Sketches this method checks if the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a></p>
   * was used to create the source MemorySegment image.
   *
   * <p>For Compact Sketches this method assumes that the sketch image was created with the
   * correct hash seed, so it is not checked.  SerialVersion 1 (pre-open-source) sketches cannot be checked.</p>
   *
   * @param srcSeg a MemorySegment with an image of a Sketch.
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a read-only Sketch backed by the given MemorySegment.
   */
  public static Sketch wrap(final MemorySegment srcSeg, final long expectedSeed) {
    final int  preLongs = srcSeg.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE) & 0X3F;
    final int serVer = srcSeg.get(JAVA_BYTE, SER_VER_BYTE) & 0XFF;
    final int familyID = srcSeg.get(JAVA_BYTE, FAMILY_BYTE) & 0XFF;
    final Family family = Family.idToFamily(familyID);
    if (family == Family.QUICKSELECT) {
      if (serVer == 3 && preLongs == 3) {
        return DirectQuickSelectSketchR.readOnlyWrap(srcSeg, expectedSeed);
      } else {
        throw new SketchesArgumentException(
            "Corrupted: " + family + " family image: must have SerVer = 3 and preLongs = 3");
      }
    }
    if (family == Family.COMPACT) {
      return CompactSketch.wrap(srcSeg, expectedSeed);
    }
    throw new SketchesArgumentException(
        "Cannot wrap family: " + family + " as a Sketch");
  }

  //Sketch interface

  /**
   * Converts this sketch to a ordered CompactSketch.
   *
   * <p>If <i>this.isCompact() == true</i> this method returns <i>this</i>,
   * otherwise, this method is equivalent to
   * {@link #compact(boolean, MemorySegment) compact(true, null)}.
   *
   * <p>A CompactSketch is always immutable.</p>
   *
   * @return this sketch as an ordered CompactSketch.
   */
  public CompactSketch compact() {
    return isCompact() ? (CompactSketch)this : compact(true, null);
  }

  /**
   * Convert this sketch to a <i>CompactSketch</i>.
   *
   * <p>If this sketch is a type of <i>UpdateSketch</i>, the compacting process converts the hash table
   * of the <i>UpdateSketch</i> to a simple list of the valid hash values.
   * Any hash values of zero or equal-to or greater than theta will be discarded.
   * The number of valid values remaining in the <i>CompactSketch</i> depends on a number of factors,
   * but may be larger or smaller than <i>Nominal Entries</i> (or <i>k</i>).
   * It will never exceed 2<i>k</i>.
   * If it is critical to always limit the size to no more than <i>k</i>,
   * then <i>rebuild()</i> should be called on the <i>UpdateSketch</i> prior to calling this method.</p>
   *
   * <p>A <i>CompactSketch</i> is always immutable.</p>
   *
   * <p>A new <i>CompactSketch</i> object is created:</p>
   * <ul><li>if <i>dstSeg!= null</i></li>
   * <li>if <i>dstSeg == null</i> and <i>this.hasMemorySegment() == true</i></li>
   * <li>if <i>dstSeg == null</i> and <i>this</i> has more than 1 item and <i>this.isOrdered() == false</i>
   * and <i>dstOrdered == true</i>.</li>
   *</ul>
   *
   * <p>Otherwise, this operation returns <i>this</i>.</p>
   *
   * @param dstOrdered assumed true if this sketch is empty or has only one value
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>
   *
   * @param dstSeg
   * <a href="{@docRoot}/resources/dictionary.html#dstSeg">See Destination MemorySegment</a>.
   *
   * @return this sketch as a <i>CompactSketch</i>.
   */
  public abstract CompactSketch compact(final boolean dstOrdered, final MemorySegment dstSeg);

  /**
   * Returns the number of storage bytes required for this Sketch if its current state were
   * compacted. It this sketch is already in the compact form this is equivalent to
   * calling {@link #getCurrentBytes()}.
   * @return number of compact bytes
   */
  public abstract int getCompactBytes();

  /**
   * Gets the number of hash values less than the given theta expressed as a long.
   * @param thetaLong the given theta as a long between zero and <i>Long.MAX_VALUE</i>.
   * @return the number of hash values less than the given thetaLong.
   */
  public int getCountLessThanThetaLong(final long thetaLong) {
    return count(getCache(), thetaLong);
  }

  /**
   * Returns the number of storage bytes required for this sketch in its current state.
   *
   * @return the number of storage bytes required for this sketch
   */
  public abstract int getCurrentBytes();

  /**
   * Gets the unique count estimate.
   * @return the sketch's best estimate of the cardinality of the input stream.
   */
  public abstract double getEstimate();

  /**
   * Returns the Family that this sketch belongs to
   * @return the Family that this sketch belongs to
   */
  public abstract Family getFamily();

  /**
   * Gets the approximate lower error bound given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the lower bound.
   */
  public double getLowerBound(final int numStdDev) {
    return isEstimationMode()
        ? lowerBound(getRetainedEntries(true), getThetaLong(), numStdDev, isEmpty())
        : getRetainedEntries(true);
  }

  /**
   * Returns the maximum number of storage bytes required for a CompactSketch with the given
   * number of actual entries.
   * @param numberOfEntries the actual number of retained entries stored in the sketch.
   * @return the maximum number of storage bytes required for a CompactSketch with the given number
   * of retained entries.
   */
  public static int getMaxCompactSketchBytes(final int numberOfEntries) {
    if (numberOfEntries == 0) { return 8; }
    if (numberOfEntries == 1) { return 16; }
    return (numberOfEntries << 3) + 24;
  }

  /**
   * Returns the maximum number of storage bytes required for a CompactSketch given the configured
   * log_base2 of the number of nominal entries, which is a power of 2.
   * @param lgNomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   * @return the maximum number of storage bytes required for a CompactSketch with the given
   * lgNomEntries.
   */
  public static int getCompactSketchMaxBytes(final int lgNomEntries) {
    return (int)((2 << lgNomEntries) * ThetaUtil.REBUILD_THRESHOLD
        + Family.QUICKSELECT.getMaxPreLongs()) * Long.BYTES;
  }

  /**
   * Returns the maximum number of storage bytes required for an UpdateSketch with the given
   * number of nominal entries (power of 2).
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   * This will become the ceiling power of 2 if it is not.
   * @return the maximum number of storage bytes required for a UpdateSketch with the given
   * nomEntries
   */
  public static int getMaxUpdateSketchBytes(final int nomEntries) {
    final int nomEnt = ceilingPowerOf2(nomEntries);
    return (nomEnt << 4) + (Family.QUICKSELECT.getMaxPreLongs() << 3);
  }

  /**
   * Returns the number of valid entries that have been retained by the sketch.
   * @return the number of valid retained entries
   */
  public int getRetainedEntries() {
    return getRetainedEntries(true);
  }

  /**
   * Returns the number of entries that have been retained by the sketch.
   * @param valid if true, returns the number of valid entries, which are less than theta and used
   * for estimation.
   * Otherwise, return the number of all entries, valid or not, that are currently in the internal
   * sketch cache.
   * @return the number of retained entries
   */
  public abstract int getRetainedEntries(boolean valid);

  /**
   * Returns the serialization version from the given MemorySegment
   * @param seg the sketch MemorySegment
   * @return the serialization version from the MemorySegment
   */
  public static int getSerializationVersion(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, SER_VER_BYTE);
  }

  /**
   * Gets the value of theta as a double with a value between zero and one
   * @return the value of theta as a double
   */
  public double getTheta() {
    return getThetaLong() / LONG_MAX_VALUE_AS_DOUBLE;
  }

  /**
   * Gets the value of theta as a long
   * @return the value of theta as a long
   */
  public abstract long getThetaLong();

  /**
   * Gets the approximate upper error bound given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the upper bound.
   */
  public double getUpperBound(final int numStdDev) {
    return isEstimationMode()
        ? upperBound(getRetainedEntries(true), getThetaLong(), numStdDev, isEmpty())
        : getRetainedEntries(true);
  }

  /**
   * Returns true if this sketch is in compact form.
   * @return true if this sketch is in compact form.
   */
  public abstract boolean isCompact();

  /**
   * <a href="{@docRoot}/resources/dictionary.html#empty">See Empty</a>
   * @return true if empty.
   */
  public abstract boolean isEmpty();

  /**
   * Returns true if the sketch is Estimation Mode (as opposed to Exact Mode).
   * This is true if theta &lt; 1.0 AND isEmpty() is false.
   * @return true if the sketch is in estimation mode.
   */
  public boolean isEstimationMode() {
    return estMode(getThetaLong(), isEmpty());
  }

  /**
   * Returns true if internal cache is ordered
   * @return true if internal cache is ordered
   */
  public abstract boolean isOrdered();

  /**
   * Returns a HashIterator that can be used to iterate over the retained hash values of the
   * Theta sketch.
   * @return a HashIterator that can be used to iterate over the retained hash values of the
   * Theta sketch.
   */
  public abstract HashIterator iterator();

  /**
   * Serialize this sketch to a byte array form.
   * @return byte array of this sketch
   */
  public abstract byte[] toByteArray();

  /**
   * Returns a human readable summary of the sketch.  This method is equivalent to the parameterized
   * call:<br>
   * <i>Sketch.toString(sketch, true, false, 8, true);</i>
   * @return summary
   */
  @Override
  public String toString() {
    return toString(true, false, 8, true);
  }

  /**
   * Gets a human readable listing of contents and summary of the given sketch.
   * This can be a very long string.  If this sketch is in a "dirty" state there
   * may be values in the dataDetail view that are &ge; theta.
   *
   * @param sketchSummary If true the sketch summary will be output at the end.
   * @param dataDetail If true, includes all valid hash values in the sketch.
   * @param width The number of columns of hash values. Default is 8.
   * @param hexMode If true, hashes will be output in hex.
   * @return The result string, which can be very long.
   */
  public String toString(final boolean sketchSummary, final boolean dataDetail, final int width,
      final boolean hexMode) {
    final StringBuilder sb = new StringBuilder();

    int nomLongs = 0;
    int arrLongs = 0;
    float p = 0;
    int rf = 0;
    final boolean updateSketch = this instanceof UpdateSketch;

    final long thetaLong = getThetaLong();
    final int curCount = this.getRetainedEntries(true);

    if (updateSketch) {
      final UpdateSketch uis = (UpdateSketch)this;
      nomLongs = 1 << uis.getLgNomLongs();
      arrLongs = 1 << uis.getLgArrLongs();
      p = uis.getP();
      rf = uis.getResizeFactor().getValue();
    }

    if (dataDetail) {
      final int w = width > 0 ? width : 8; // default is 8 wide
      if (curCount > 0) {
        sb.append("### SKETCH DATA DETAIL");
        final HashIterator it = iterator();
        int j = 0;
        while (it.next()) {
          final long h = it.get();
          if (j % w == 0) {
            sb.append(LS).append(String.format("   %6d", j + 1));
          }
          if (hexMode) {
            sb.append(" " + zeroPad(Long.toHexString(h), 16) + ",");
          }
          else {
            sb.append(String.format(" %20d,", h));
          }
          j++ ;
        }
        sb.append(LS).append("### END DATA DETAIL").append(LS + LS);
      }
    }

    if (sketchSummary) {
      final double thetaDbl = thetaLong / LONG_MAX_VALUE_AS_DOUBLE;
      final String thetaHex = zeroPad(Long.toHexString(thetaLong), 16);
      final String thisSimpleName = this.getClass().getSimpleName();
      final int seedHash = Short.toUnsignedInt(getSeedHash());

      sb.append(LS);
      sb.append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
      if (updateSketch) {
        sb.append("   Nominal Entries (k)     : ").append(nomLongs).append(LS);
      }
      sb.append("   Estimate                : ").append(getEstimate()).append(LS);
      sb.append("   Upper Bound, 95% conf   : ").append(getUpperBound(2)).append(LS);
      sb.append("   Lower Bound, 95% conf   : ").append(getLowerBound(2)).append(LS);
      if (updateSketch) {
        sb.append("   p                       : ").append(p).append(LS);
      }
      sb.append("   Theta (double)          : ").append(thetaDbl).append(LS);
      sb.append("   Theta (long)            : ").append(thetaLong).append(LS);
      sb.append("   Theta (long) hex        : ").append(thetaHex).append(LS);
      sb.append("   EstMode?                : ").append(isEstimationMode()).append(LS);
      sb.append("   Empty?                  : ").append(isEmpty()).append(LS);
      sb.append("   Ordered?                : ").append(isOrdered()).append(LS);
      if (updateSketch) {
        sb.append("   Resize Factor           : ").append(rf).append(LS);
        sb.append("   Array Size Entries      : ").append(arrLongs).append(LS);
      }
      sb.append("   Retained Entries        : ").append(curCount).append(LS);
      sb.append("   Seed Hash               : ").append(Integer.toHexString(seedHash))
        .append(" | ").append(seedHash).append(LS);
      sb.append("### END SKETCH SUMMARY").append(LS);

    }
    return sb.toString();
  }

  /**
   * Returns a human readable string of the preamble of a byte array image of a Theta Sketch.
   * @param byteArr the given byte array
   * @return a human readable string of the preamble of a byte array image of a Theta Sketch.
   */
  public static String toString(final byte[] byteArr) {
    return PreambleUtil.preambleToString(byteArr);
  }

  /**
   * Returns a human readable string of the preamble of a MemorySegment image of a Theta Sketch.
   * @param seg the given MemorySegment object
   * @return a human readable string of the preamble of a MemorySegment image of a Theta Sketch.
   */
  public static String toString(final MemorySegment seg) {
    return PreambleUtil.preambleToString(seg);
  }

  //Restricted methods

  /**
   * Gets the internal cache array. For on-heap sketches this will return a reference to the actual
   * cache array. For MemorySegment-based sketches this returns a copy.
   * @return the internal cache array.
   */
  abstract long[] getCache();

  /**
   * Gets preamble longs if stored in compact form. If this sketch is already in compact form,
   * this is identical to the call {@link #getCurrentPreambleLongs()}.
   * @return preamble longs if stored in compact form.
   */
  abstract int getCompactPreambleLongs();

  /**
   * Gets the number of data longs if stored in current state.
   * @return the number of data longs if stored in current state.
   */
  abstract int getCurrentDataLongs();

  /**
   * Returns preamble longs if stored in current state.
   * @return number of preamble longs if stored.
   */
  abstract int getCurrentPreambleLongs();

  /**
   * Returns the backing MemorySegment object if it exists, otherwise null.
   * This is overridden where relevant.
   * @return the backing MemorySegment object if it exists, otherwise null.
   */
  MemorySegment getMemorySegment() { return null; }

  /**
   * Gets the 16-bit seed hash
   * @return the seed hash
   */
  abstract short getSeedHash();

  /**
   * Returns true if given Family id is one of the theta sketches
   * @param id the given Family id
   * @return true if given Family id is one of the theta sketches
   */
  static final boolean isValidSketchID(final int id) {
    return id == Family.ALPHA.getID()
        || id == Family.QUICKSELECT.getID()
        || id == Family.COMPACT.getID();
  }

  /**
   * Checks Ordered and Compact flags for integrity between sketch and a MemorySegment
   * @param sketch the given sketch
   */
  static final void checkSketchAndMemorySegmentFlags(final Sketch sketch) {
    final MemorySegment seg = sketch.getMemorySegment();
    if (seg == null) { return; }
    final int flags = PreambleUtil.extractFlags(seg);
    if ((flags & COMPACT_FLAG_MASK) > 0 ^ sketch.isCompact()) {
      throw new SketchesArgumentException("Possible corruption: "
          + "MemorySegment Compact Flag inconsistent with Sketch");
    }
    if ((flags & ORDERED_FLAG_MASK) > 0 ^ sketch.isOrdered()) {
      throw new SketchesArgumentException("Possible corruption: "
          + "MemorySegment Ordered Flag inconsistent with Sketch");
    }
  }

  static final double estimate(final long thetaLong, final int curCount) {
    return curCount * (LONG_MAX_VALUE_AS_DOUBLE / thetaLong);
  }

  static final double lowerBound(final int curCount, final long thetaLong, final int numStdDev,
      final boolean empty) {
    final double theta = thetaLong / LONG_MAX_VALUE_AS_DOUBLE;
    return BinomialBoundsN.getLowerBound(curCount, theta, numStdDev, empty);
  }

  static final double upperBound(final int curCount, final long thetaLong, final int numStdDev,
      final boolean empty) {
    final double theta = thetaLong / LONG_MAX_VALUE_AS_DOUBLE;
    return BinomialBoundsN.getUpperBound(curCount, theta, numStdDev, empty);
  }

  private static final boolean estMode(final long thetaLong, final boolean empty) {
    return thetaLong < Long.MAX_VALUE && !empty;
  }

  /**
   * Instantiates a Heap Update Sketch from MemorySegment. Only SerVer3. SerVer 1 & 2 already handled.
   * @param srcSeg the source MemorySegment
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a Sketch
   */
  private static final Sketch heapifyUpdateFromMemorySegment(final MemorySegment srcSeg, final long expectedSeed) {
    final long cap = srcSeg.byteSize();
    if (cap < 8) {
      throw new SketchesArgumentException(
          "Corrupted: valid sketch must be at least 8 bytes.");
    }
    final byte familyID = srcSeg.get(JAVA_BYTE, FAMILY_BYTE);
    final Family family = idToFamily(familyID);

    if (family == Family.ALPHA) {
      final int flags = PreambleUtil.extractFlags(srcSeg);
      final boolean compactFlag = (flags & COMPACT_FLAG_MASK) != 0;
      if (compactFlag) {
        throw new SketchesArgumentException(
            "Corrupted: ALPHA family image: cannot be compact");
      }
      return HeapAlphaSketch.heapifyInstance(srcSeg, expectedSeed);
    }
    if (family == Family.QUICKSELECT) {
      return HeapQuickSelectSketch.heapifyInstance(srcSeg, expectedSeed);
    }
    throw new SketchesArgumentException(
        "Sketch cannot heapify family: " + family + " as a Sketch");
  }

}
