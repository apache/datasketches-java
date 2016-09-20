package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.sampling.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER;
import static com.yahoo.sketches.sampling.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.sampling.PreambleUtil.extractFlags;
import static com.yahoo.sketches.sampling.PreambleUtil.extractItemsSeenCount;
import static com.yahoo.sketches.sampling.PreambleUtil.extractReservoirSize;
import static com.yahoo.sketches.sampling.PreambleUtil.extractResizeFactor;
import static com.yahoo.sketches.sampling.PreambleUtil.extractSerDeId;
import static com.yahoo.sketches.sampling.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.sampling.PreambleUtil.getAndCheckPreLongs;

import java.lang.reflect.Array;
import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;
import com.yahoo.sketches.Util;


/**
 * Created by jmalkin on 8/22/16.
 *
 * @param <T> The type of object held in the reservoir.
 *
 * @author jmalkin
 * @author langk
 */
public class ReservoirItemsSketch<T> {

    /**
     * The smallest sampling array allocated: 16
     */
    private static final int MIN_LG_ARR_LONGS = 4;

    /**
     * Using 48 bits to capture number of items seen, so sketch cannot process more after this many items
     * capacity
     */
    private static final long MAX_ITEMS_SEEN = (1 << 48) - 1;

    /**
     * Default sampling size multiple when reallocating storage: 8
     */
    private static final ResizeFactor DEFAULT_RESIZE_FACTOR = ResizeFactor.X8;

    private int reservoirSize_;      // max size of sampling
    private short encodedResSize_;     // compact encoding of reservoir size
    private int currItemsAlloc_;     // currently allocated array size
    private long itemsSeen_;         // number of items presented to sketch
    private ResizeFactor rf_;        // resize factor
    private Object[] data_;          // stored sampled data

    /**
     * Construct a mergeable sampling sample sketch with up to k samples using the default resize factor (8).
     *
     * @param k Maximum size of sampling. Allocated size may be smaller until sampling fills. Unlike many sketches
     *          in this package, this value does <em>not</em> need to be a power of 2.
     * @param <T> The type of object held in the reservoir.
     * @return A ReservoirLongsSketch initialized with maximum size k and the default resize factor.
     * */
    public static <T> ReservoirItemsSketch<T> getInstance(final int k) {
        return new ReservoirItemsSketch<T>(k, DEFAULT_RESIZE_FACTOR);
    }

    /**
     * Construct a mergeable sampling sample sketch with up to k samples using the default resize factor (8).
     *
     * @param k Maximum size of sampling. Allocated size may be smaller until sampling fills. Unlike many sketches
     *          in this package, this value does <em>not</em> need to be a power of 2.
     * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
     * @param <T> The type of object held in the reservoir.
     * @return A ReservoirLongsSketch initialized with maximum size k and resize factor rf.
     * */
    public static <T> ReservoirItemsSketch<T> getInstance(final int k, ResizeFactor rf) {
        return new ReservoirItemsSketch<T>(k, rf);
    }

    /**
     * Thin wrapper around private constructor
     * @param data Reservoir data as long[]
     * @param itemsSeen Number of items presented to the sketch so far
     * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
     * @param encodedResSize Compact encoding of reservoir size
     * @return New sketch built with the provided inputs
     */
    static <T> ReservoirItemsSketch<T> getInstance(final T[] data, final long itemsSeen,
                                            final ResizeFactor rf, final short encodedResSize) {
        return new ReservoirItemsSketch(data, itemsSeen, rf, encodedResSize);
    }


    private ReservoirItemsSketch(final int k, final ResizeFactor rf) {
        // required due to a theorem about lightness during merging
        if (k < 2) {
            throw new SketchesArgumentException("k must be at least 2");
        }

        encodedResSize_ = ReservoirSize.computeSize(k);
        reservoirSize_ = ReservoirSize.decodeValue(encodedResSize_);
        rf_ = rf;

        itemsSeen_ = 0;

        int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(reservoirSize_), "ReservoirLongsSketch");
        int initialSize = SamplingUtil.startingSubMultiple(reservoirSize_, ceilingLgK, MIN_LG_ARR_LONGS);

        currItemsAlloc_ = SamplingUtil.getAdjustedSize(reservoirSize_, initialSize);
        data_ = new Object[currItemsAlloc_];
    }

    /**
     * Creates a fully-populated sketch. Used internally to avoid extraneous array allocation when deserializing.
     * Uses size of data array to as initial array allocation.
     * @param data Reservoir data as an <tt>Object[]</tt>
     * @param itemsSeen Number of items presented to the sketch so far
     * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
     * @param encodedResSize Compact encoding of reservoir size
     */
    private ReservoirItemsSketch(final Object[] data, final long itemsSeen,
                                 final ResizeFactor rf, final short encodedResSize) {
        int reservoirSize = ReservoirSize.decodeValue(encodedResSize);

        if (data == null) {
            throw new SketchesArgumentException("Instantiating sketch with null reservoir");
        }
        if (reservoirSize < 2) {
            throw new SketchesArgumentException("Cannot instantiate sketch with reservoir size less than 2");
        }
        if (reservoirSize < data.length) {
            throw new SketchesArgumentException("Instantiating sketch with max size less than array length: "
                    + reservoirSize + " max size, array of length " + data.length);
        }
        if ((itemsSeen >= reservoirSize && data.length < reservoirSize)
                || (itemsSeen < reservoirSize && data.length < itemsSeen)) {
            throw new SketchesArgumentException("Instantiating sketch with too few samples. Items seen: "
                    + itemsSeen + ", max reservoir size: " + reservoirSize + ", data array length: " + data.length);
        }

        // TODO: compute target current allocation to validate?
        encodedResSize_ = encodedResSize;
        reservoirSize_ = reservoirSize;
        currItemsAlloc_ = data.length;
        itemsSeen_ = itemsSeen;
        rf_ = rf;
        data_ = data;
    }

    /**
     * Fast constructor for full-specified sketch with no encoded/decoding size and no validation. Used with copy().
     * @param reservoirSize Maximum reservoir capacity
     * @param encodedResSize Maximum reservoir capacity encoded into fixed-point format
     * @param currItemsAlloc Current array size (assumed equal to data.length)
     * @param itemsSeen Total items seen by this sketch
     * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
     * @param data Data array backing the reservoir, will <em>not</em> be copied
     */
    private ReservoirItemsSketch(final int reservoirSize, final short encodedResSize, final int currItemsAlloc,
                                 final long itemsSeen, final ResizeFactor rf, final Object[] data) {
        this.reservoirSize_ = reservoirSize;
        this.encodedResSize_ = encodedResSize;
        this.currItemsAlloc_ = currItemsAlloc;
        this.itemsSeen_ = itemsSeen;
        this.rf_ = rf;
        this.data_ = data;
    }


    /**
     * Returns the sketch's value of <i>k</i>, the maximum number of samples stored in the reservoir. The current
     * number of items in the sketch may be lower.
     *
     * @return k, the maximum number of samples in the reservoir
     */
    public int getK() {
        return reservoirSize_;
    }

    /**
     * Returns the number of items processed from the input stream
     *
     * @return n, the number of stream items the sketch has seen
     */
    public long getN() {
        return itemsSeen_;
    }

    /**
     * Returns the current number of items in the reservoir, which may be smaller than the reservoir capacity.
     * @return the number of items currently in the reservoir
     */
    public int getNumSamples() {
        return (int) Math.min(reservoirSize_, itemsSeen_);
    }

    /**
     * Returns a copy of the items in the reservoir, or null if empty. The returned array length may be smaller than
     * the reservoir capacity.
     *
     * <p>In order to allocate an array of generic type T, uses the class of the first item in the array. This method
     * method may throw an <tt>ArrayAssignmentException</tt> if the reservoir stores instances of a polymorphic base
     * class.</p>
     *
     * @return A copy of the reservoir array
     */
    public T[] getSamples() {
        if (itemsSeen_ == 0) {
            return null;
        }

        return getSamples(data_[0].getClass());
    }

    /**
     * Returns a copy of the items in the reservoir as members of Class <em>clazz</em>, or null if empty. The returned
     * array length may be smaller than the reservoir capacity.
     *
     * <p>This method allocates an array of class <em>clazz</em>, which must either match or extend T. This method
     * should be used when objects in the array are all instances of T but are not necessarily instances of the
     * base class.</p>
     *
     * @param clazz A class to which the items are cast before returning
     * @return A copy of the reservoir array
     */
    @SuppressWarnings("unchecked")
    public T[] getSamples(Class<?> clazz) {
        if (itemsSeen_ == 0) {
            return null;
        }

        int numSamples = (int) Math.min(reservoirSize_, itemsSeen_);
        T[] dst = (T[]) Array.newInstance(clazz, numSamples);
        System.arraycopy(data_, 0, dst, 0, numSamples);
        return dst;
    }


    /**
     * Returns a sketch instance of this class from the given srcMem,
     * which must be a Memory representation of this sketch class.
     *
     * @param <T> The type of item this sketch contains
     * @param srcMem a Memory representation of a sketch of this class.
     * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
     * @param serDe An instance of ArrayOfItemsSerDe
     * @return a sketch instance of this class
     */
    public static <T> ReservoirItemsSketch<T> getInstance(final Memory srcMem, ArrayOfItemsSerDe<T> serDe) {
        final int numPreLongs = getAndCheckPreLongs(srcMem);
        final long pre0 = srcMem.getLong(0);
        final ResizeFactor rf = ResizeFactor.getRF(extractResizeFactor(pre0));
        final int serVer = extractSerVer(pre0);
        final int familyId = extractFamilyID(pre0);
        final boolean isEmpty = (extractFlags(pre0) & EMPTY_FLAG_MASK) != 0;

        final short encodedResSize = extractReservoirSize(pre0);
        final int reservoirSize  = ReservoirSize.decodeValue(encodedResSize);

        // Check values
        final boolean preLongsEqMin = (numPreLongs == Family.RESERVOIR.getMinPreLongs());
        final boolean preLongsEqMax = (numPreLongs == Family.RESERVOIR.getMaxPreLongs());

        if (!preLongsEqMin & !preLongsEqMax) {
            throw new SketchesArgumentException(
                    "Possible corruption: Non-empty sketch with only " + Family.RESERVOIR.getMinPreLongs()
                    + "preLongs");
        }
        if (serVer != SER_VER) {
            throw new SketchesArgumentException(
                    "Possible Corruption: Ser Ver must be " + SER_VER + ": " + serVer);
        }
        final int reqFamilyId = Family.RESERVOIR.getID();
        if (familyId != reqFamilyId) {
            throw new SketchesArgumentException(
                    "Possible Corruption: FamilyID must be " + reqFamilyId + ": " + familyId);
        }
        final short serDeId = extractSerDeId(pre0);
        if (serDe.getId() != serDeId) {
            throw new SketchesArgumentException(
                    "Possible Corruption: SerDeID must be " + serDeId + ": " + serDe.getId());
        }

        if (isEmpty) {
            return new ReservoirItemsSketch<T>(reservoirSize, rf);
        }

        //get rest of preamble
        final long pre1 = srcMem.getLong(8);
        final long itemsSeen = extractItemsSeenCount(pre1);

        int preLongBytes = numPreLongs << 3;
        int allocatedSize = reservoirSize; // default to full reservoir
        if (itemsSeen < reservoirSize) {
            // under-full so determine size to allocate, using ceilingLog2(totalSeen) as minimum
            // casts to int are safe since under-full
            int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(reservoirSize), "getInstance");
            int minLgSize = Util.toLog2(Util.ceilingPowerOf2((int) itemsSeen), "getInstance");
            int initialLgSize = SamplingUtil.startingSubMultiple(reservoirSize, ceilingLgK,
                    Math.min(minLgSize, MIN_LG_ARR_LONGS));

            allocatedSize = SamplingUtil.getAdjustedSize(reservoirSize, 1 << initialLgSize);
        }
        Object[] data = serDe.deserializeFromMemory(
                new MemoryRegion(srcMem, preLongBytes, srcMem.getCapacity() - preLongBytes), allocatedSize);

        return new ReservoirItemsSketch<T>(data, itemsSeen, rf, encodedResSize);
    }


    /**
     * Returns a byte array representation of this sketch. May fail for polymorphic item types.
     * @param serDe An instance of ArrayOfItemsSerDe
     * @return a byte array representation of this sketch
     */
    public byte[] toByteArray(final ArrayOfItemsSerDe<T> serDe) {
        if (itemsSeen_ == 0) {
            // null class is ok since empty -- no need to call serDe
            return toByteArray(serDe, null);
        } else {
            return toByteArray(serDe, data_[0].getClass());
        }
    }

    /**
     * Returns a byte array representation of this sketch. Copies contents into an array of the specified class for
     * serialization to allow for polymorphic types.
     * @param serDe An instance of ArrayOfItemsSerDe
     * @param clazz The class represented by type &lt;T&gt;
     * @return a byte array representation of this sketch
     */
    public byte[] toByteArray(final ArrayOfItemsSerDe<T> serDe, final Class<?> clazz) {
        final int preLongs, outBytes;
        final boolean empty = itemsSeen_ == 0;
        byte[] bytes = null; // for serialized data from serDe

        if (empty) {
            preLongs = 1;
            outBytes = 8;
        } else {
            preLongs = Family.RESERVOIR.getMaxPreLongs();
            bytes = serDe.serializeToByteArray(getSamples(clazz));
            outBytes = (preLongs << 3) + bytes.length;
        }
        final byte[] outArr = new byte[outBytes];
        final Memory mem = new NativeMemory(outArr);

        // build first preLong
        long pre0 = 0L;
        pre0 = PreambleUtil.insertPreLongs(preLongs, pre0);                  // Byte 0
        pre0 = PreambleUtil.insertResizeFactor(rf_.lg(), pre0);
        pre0 = PreambleUtil.insertSerVer(SER_VER, pre0);                     // Byte 1
        pre0 = PreambleUtil.insertFamilyID(Family.RESERVOIR.getID(), pre0);  // Byte 2
        pre0 = (empty) ? PreambleUtil.insertFlags(EMPTY_FLAG_MASK, pre0) : PreambleUtil.insertFlags(0, pre0); // Byte 3
        pre0 = PreambleUtil.insertReservoirSize(encodedResSize_, pre0);      // Bytes 4-5
        pre0 = PreambleUtil.insertSerDeId(serDe.getId(), pre0);              // Bytes 6-7

        if (empty) {
            mem.putLong(0, pre0);
        } else {
            // second preLong, only if non-empty
            long pre1 = 0L;
            pre1 = PreambleUtil.insertItemsSeenCount(itemsSeen_, pre1);

            final long[] preArr = new long[preLongs];
            preArr[0] = pre0;
            preArr[1] = pre1;
            mem.putLongArray(0, preArr, 0, preLongs);
            final int preBytes = preLongs << 3;
            mem.putByteArray(preBytes, bytes, 0, bytes.length);
        }

        return outArr;
    }

    double getImplicitSampleWeight() {
        if (itemsSeen_ < reservoirSize_) {
            return 1.0;
        } else {
            return (1.0 * itemsSeen_ / reservoirSize_);
        }
    }


    /**
     * Randomly decide whether or not to include an item in the sample set.
     *
     * @param item a unit-weight (equivalently, unweighted) item of the set being sampled from
     */
    public void update(T item) {
        if (itemsSeen_ == MAX_ITEMS_SEEN) {
            throw new SketchesStateException("Sketch has exceeded capacity for total items seen: " + MAX_ITEMS_SEEN);
        }

        if (itemsSeen_ < reservoirSize_) { // code for initial phase where we take the first reservoirSize_ items
            if (itemsSeen_ >= currItemsAlloc_) {
                growReservoir();
            }
            assert itemsSeen_ < currItemsAlloc_;
            // we'll randomize replacement positions, so in-order should be valid for now
            data_[(int) itemsSeen_] = item; // since less than reservoir size, cast is safe
            ++itemsSeen_;
        } else { // code for steady state where we sample randomly
            ++itemsSeen_;
            // prob(keep_item) < k / n = reservoirSize_ / itemsSeen_
            // so multiply to get: keep if rand * itemsSeen_ < reservoirSize_
            if (SamplingUtil.rand.nextDouble() * itemsSeen_ < reservoirSize_) {
                int newSlot = SamplingUtil.rand.nextInt(reservoirSize_);
                data_[newSlot] = item;
            }
        }
    }

    /**
     * Increases allocated sampling size by (adjusted) ResizeFactor and copies data from old sampling.
     */
    private void growReservoir() {
        int newSize = SamplingUtil.getAdjustedSize(reservoirSize_, currItemsAlloc_ * rf_
                .getValue());
        Object[] buffer = java.util.Arrays.copyOf(data_, newSize);

        currItemsAlloc_ = newSize;
        data_ = buffer;
    }

    /**
     * Useful during union operations to avoid copying the data array around if only updating a few points.
     * @param pos The position from which to retrieve the element
     * @return The value in the reservoir at position <tt>pos</tt>
     */
    T getValueAtPosition(int pos) {
        if (itemsSeen_ == 0) {
            throw new SketchesArgumentException("Requested element from empty reservoir.");
        }
        else if (pos < 0 || pos >= getNumSamples()) {
            throw new SketchesArgumentException("Requested position must be between 0 and " + getNumSamples() + ", "
                    + "inclusive. Received: " + pos);
        }

        return (T) data_[pos];
    }

    /**
     * Useful during union operation to force-insert a value into the union gadget. Does <em>NOT</em> increment count
     * of items seen.
     * @param value The entry to store in the reservoir
     * @param pos The position at which to store the entry
     */
    void insertValueAtPosition(T value, int pos) {
        if (pos < 0 || pos >= getNumSamples()) {
            throw new SketchesArgumentException("Insert position must be between 0 and " + getNumSamples() + ", "
                    + "inclusive. Received: " + pos);
        }

        data_[pos] = value;
    }

    /**
     * Used during union operations to update count of items seen. Does <em>NOT</em> check sign, but will throw an
     * exception if the final result exceeds the maximum possible items seen value.
     * @param inc The value added
     */
    void forceIncrementItemsSeen(final long inc) {
        itemsSeen_ += inc;

        if (itemsSeen_ > MAX_ITEMS_SEEN) {
            throw new SketchesStateException("Sketch has exceeded capacity for total items seen. Limit: "
                    + MAX_ITEMS_SEEN + ", found: " + itemsSeen_);
        }
    }

    /**
     * Used during union operations to ensure we do not overwrite an existing reservoir. Creates a
     * <en>shallow</en> copy of the reservoir.
     * @return A copy of the current sketch
     */
    ReservoirItemsSketch<T> copy() {
        Object[] dataCopy = Arrays.copyOf(data_, currItemsAlloc_);
        return new ReservoirItemsSketch<>(reservoirSize_, encodedResSize_, currItemsAlloc_, itemsSeen_, rf_, dataCopy);
    }
}
