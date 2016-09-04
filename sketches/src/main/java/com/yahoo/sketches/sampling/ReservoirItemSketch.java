package com.yahoo.sketches.sampling;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;
import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;

import java.lang.reflect.Array;
import java.util.Random;

import static com.yahoo.sketches.sampling.PreambleUtil.*;

/**
 * Created by jmalkin on 8/22/16.
 *
 * @author jmalkin
 * @author langk
 */
public class ReservoirItemSketch<T> {

    /**
     * The smallest sampling array allocated: 16
     */
    private static final int MIN_LG_ARR_LONGS = 4;

    /**
     * Default sampling size multiple when reallocating storage: 8
     */
    private static final ResizeFactor DEFAULT_RESIZE_FACTOR = ResizeFactor.X8;

    static Random rand = new Random();

    int reservoirSize_;      // max size of sampling
    int encodedResSize_;     // compact encoding of reservoir size
    int currItemsAlloc_;     // currently allocated array size
    long itemsSeen_;         // number of items presented to sketch
    ResizeFactor rf_;        // resize factor
    Object[] data_;          // stored sampled data

    /**
     * Construct a mergeable sampling sample sketch with up to k samples using the default resize factor (8).
     *
     * @param k Maximum size of sampling. Allocated size may be smaller until sampling fills. Unlike many sketches
     *          in this package, this value does <em>not</em> need to be a power of 2.
     */
    public static ReservoirItemSketch getInstance(final int k) {
        return new ReservoirItemSketch(k, DEFAULT_RESIZE_FACTOR);
    }

    /**
     * Construct a mergeable sampling sample sketch with up to k samples using the default resize factor (8).
     *
     * @param k Maximum size of sampling. Allocated size may be smaller until sampling fills. Unlike many sketches
     *          in this package, this value does <em>not</em> need to be a power of 2.
     * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
     */
    public static ReservoirItemSketch getInstance(final int k, ResizeFactor rf) {
        return new ReservoirItemSketch(k, rf);
    }

    private ReservoirItemSketch(final int k, final ResizeFactor rf) {
        // required due to a theorem about lightness during merging
        if (k < 2) {
            throw new IllegalArgumentException("k must be at least 2");
        }

        encodedResSize_ = ReservoirSize.computeSize(k);
        reservoirSize_ = ReservoirSize.decodeValue(encodedResSize_);
        rf_ = rf;

        itemsSeen_ = 0;

        int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(reservoirSize_), "ReservoirLongSketch");
        int initialSize = startingSubMultiple(reservoirSize_, ceilingLgK, MIN_LG_ARR_LONGS);

        currItemsAlloc_ = getAdjustedSize(reservoirSize_, initialSize);
        data_ = new Object[currItemsAlloc_];
    }

    /**
     * Creates a fully-populated sketch. Used internally to avoid extraneous array allocation when deserializing.
     * @param data Reservoir data as an <tt>Object[]</tt>
     * @param itemsSeen Number of items presented to the sketch so far
     * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
     * @param encodedResSize Compact encoding of reservoir size
     */
    private ReservoirItemSketch(final Object[] data, final long itemsSeen,
                                final ResizeFactor rf, final int encodedResSize) {
        int reservoirSize = ReservoirSize.decodeValue(encodedResSize);

        if (data == null) {
            throw new SketchesArgumentException("Instantiating sketch with null reservoir");
        }
        if (reservoirSize < data.length) {
            throw new SketchesArgumentException("Instantiating sketch with max size less than array length: "
                    + reservoirSize + " max size, array of length " + data.length);
        }
        if ((itemsSeen > reservoirSize && data.length < reservoirSize)
                || (itemsSeen < reservoirSize && data.length < itemsSeen)) {
            throw new SketchesArgumentException("Instantiating sketch with under-full reservoir. Items seen: " +
                    itemsSeen + ", max reservoir size: " + reservoirSize + ", data array length: " + data.length);
        }
        /*
        if (currItemsAlloc != data.length) {
            throw new SketchesArgumentException("Instantiating sketch with current allocation " + currItemsAlloc +
                    " but data arrany length " + data.length);
        }
        */

        // TODO: compute target current allocation to validate?
        encodedResSize_ = encodedResSize;
        reservoirSize_ = reservoirSize;
        currItemsAlloc_ = data.length;
        itemsSeen_ = itemsSeen;
        rf_ = rf;
        data_ = data;
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
        return (int) Math.min((long) reservoirSize_, itemsSeen_);
    }

    void setRandomSeed(long seed) {
        rand.setSeed(seed);
    }

    /**
     * Returns a copy of the items in the reservoir, or null if empty. The returned array length may be smaller than
     * the reservoir capacity.
     *
     * In order to allocate an array of generic type T, uses the class of the first item in the array. This method
     * may throw a <tt>ClassCastException</tt> if the reservoir stores instances of a polymorphci base class.
     *
     * @return A copy of the reservoir array
     */
    public T[] getSamples() {
        if (itemsSeen_ == 0) {
            return null;
        }

        return getSamples(data_[0].getClass());
    }

    @SuppressWarnings("unchecked")
    public T[] getSamples(Class clazz) {
         if (itemsSeen_ == 0) {
             return null;
         }

        int numSamples = (int) Math.min((long) reservoirSize_, itemsSeen_);
        T[] dst = (T[]) Array.newInstance(clazz, numSamples);
        System.arraycopy(data_, 0, dst, 0, numSamples);
        return dst;
    }


    /**
     * Returns a sketch instance of this class from the given srcMem,
     * which must be a Memory representation of this sketch class.
     *
     * @param srcMem a Memory representation of a sketch of this class.
     * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
     * @return a sketch instance of this class
     */
    public static <T> ReservoirItemSketch getInstance(final Memory srcMem, ArrayOfItemsSerDe<T> serDe) {
        final int numPreLongs = getAndCheckPreLongs(srcMem);
        final long pre0 = srcMem.getLong(0);
        final ResizeFactor rf = ResizeFactor.getRF(extractResizeFactor(pre0));
        final int serVer = extractSerVer(pre0);
        final int familyId = extractFamilyID(pre0);
        final boolean isEmpty = (extractFlags(pre0) & EMPTY_FLAG_MASK) != 0;

        final int encodedResSize = extractReservoirSize(pre0);
        final int reservoirSize  = ReservoirSize.decodeValue(encodedResSize);

        // Check values
        final boolean preLongsEqMin = (numPreLongs == Family.RESERVOIR.getMinPreLongs());
        final boolean preLongsEqMax = (numPreLongs == Family.RESERVOIR.getMaxPreLongs());

        if (!preLongsEqMin & !preLongsEqMax) {
            throw new SketchesArgumentException(
                    "Possible corruption: Non-empty sketch with only " + Family.RESERVOIR.getMinPreLongs() +
                            "preLongs");
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
        final int serDeId = extractSerDeId(pre0);
        if (serDe.getId() != serDeId) {
            throw new SketchesArgumentException(
                    "Possible Corruption: SerDeID must be " + serDeId + ": " + serDe.getId());
        }

        if (isEmpty) {
            return new ReservoirItemSketch<T>(reservoirSize, rf);
        }

        //get rest of preamble
        final long pre1 = srcMem.getLong(8);
        long itemsSeen = extractItemsSeenCount(pre1);

        int preLongBytes = numPreLongs << 3;
        int allocatedSize = reservoirSize; // default to full reservoir
        if (itemsSeen < reservoirSize) {
            // under-full so determine size to allocate, using ceilingLog2(totalSeen) as minimum
            // casts to int are safe since under-full
            int ceilingLgK = Util.toLog2(Util.ceilingPowerOf2(reservoirSize), "getInstance");
            int minLgSize = Util.toLog2(Util.ceilingPowerOf2((int) itemsSeen), "getInstance");
            int initialSize = startingSubMultiple(reservoirSize, ceilingLgK,
                    Math.min(minLgSize, MIN_LG_ARR_LONGS));

            allocatedSize = getAdjustedSize(reservoirSize, initialSize);
        }
        Object[] data = serDe.deserializeFromMemory(
                new MemoryRegion(srcMem, preLongBytes, srcMem.getCapacity() - preLongBytes), allocatedSize);

        return new ReservoirItemSketch<T>(data, itemsSeen, rf, encodedResSize);
    }


    /**
     * Returns a byte array representation of this sketch. May fail for polymorphic item tyes.
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

    public byte[] toByteArray(final ArrayOfItemsSerDe<T> serDe, final Class clazz) {
        final int preLongs, outBytes;
        final boolean empty = itemsSeen_ == 0;
        //final int numItems = (int) Math.min(reservoirSize_, itemsSeen_);
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
            return ((double) itemsSeen_ / (double) reservoirSize_);
        }
    }


    /**
     * Randomly decide whether or not to include an item in the sample set.
     *
     * @param item a unit-weight (equivalently, unweighted) item of the set being sampled from
     */
    public void update (T item) {
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
            if (rand.nextDouble() * itemsSeen_ < reservoirSize_) {
                int newSlot = rand.nextInt(reservoirSize_);
                data_[newSlot] = item;
            }
        }
    }

    /**
     * Increases allocated sampling size by (adjusted) ResizeFactor and copies data from old sampling.
     */
    private void growReservoir() {
        int newSize = getAdjustedSize(reservoirSize_, currItemsAlloc_ * rf_.getValue());
        Object[] buffer = java.util.Arrays.copyOf(data_, newSize);

        currItemsAlloc_ = newSize;
        data_ = buffer;
    }

    /**
     * Checks if target sampling allocation is more than 50% of max sampling size. If so, returns max sampling size,
     * otherwise passes through the target size.
     *
     * @param maxSize Maximum allowed reservoir size, as from getK()
     * @param resizeTarget Next size based on a pure ResizeFactor scaling
     * @return (reservoirSize_ &lt; 2*resizeTarget ? reservoirSize_ : resizeTarget)
     */
    private static int getAdjustedSize(final int maxSize, final int resizeTarget) {
        if (maxSize - (resizeTarget << 1) < 0L) {
            return maxSize;
        }
        return resizeTarget;
    }

    static int startingSubMultiple(int lgTarget, int lgRf, int lgMin) {
        return (lgTarget <= lgMin) ? lgMin : (lgRf == 0) ? lgTarget : (lgTarget - lgMin) % lgRf + lgMin;
    }

    public static void main(String[] args) {

        ReservoirItemSketch<Long> rs = ReservoirItemSketch.<Long>getInstance(5, ResizeFactor.X8);
        rs.setRandomSeed(11L);

        for (long i = 0; i < 3; ++i) {
            rs.update(i);
        }

        /*
        ReservoirItemSketch<Long> rs = ReservoirItemSketch.<Long>getInstance(5);
        rs.setRandomSeed(11L);

        rs.update(1L);
        rs.update(2);
        rs.update(3.0);
        */
        for (Long l : rs.getSamples()) {
            System.out.println(l);
        }

        ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();


        System.out.println("Samples after 3:");
        for (long l : rs.getSamples()) {
        //for (Object i : rs.getSamples()) {
        //    Long l = (Long) i;
            System.out.println(l);
        }
        System.out.println("\n\n");

        for (long i = 3; i < 20; ++i) {
            rs.update(i);
        }

        System.out.println("Samples after 20:");
        for (long l : rs.getSamples()) {
            System.out.println(l);
        }
        System.out.println("\n\n");

        byte[] ser1 = rs.toByteArray(serDe);
        Memory mem = new NativeMemory(ser1);

        //ReservoirItemSketch<Long> rs2 = ReservoirItemSketch.<Long>getInstance(mem, serDe);
        ReservoirLongSketch rs2 = ReservoirLongSketch.getInstance(mem);
        byte[] ser2 = rs2.toByteArray();

        System.out.println("Reconstructed samples:");
        for (long l : rs2.getSamples()) {
            System.out.println(l);
        }



        System.out.println("Preamble:");
        System.out.println(preambleToString(ser1));

        System.out.println("Comparing byte[] versions:");
        System.out.println("\tSizes " + (ser1.length == ser2.length ? "match" : "differ"));

        System.out.println("Ser 1:");
        System.out.print(printBytesAsLongs(ser1) + Util.LS);

        System.out.println("Ser 2:");
        System.out.print(printBytesAsLongs(ser2) + Util.LS);

        for (int i = 0; i < ser1.length; ++i) {
            if (ser1[i] != ser2[i]) {
                System.out.println("Arrays differ at byte " + i);
                return;
            }
        }
        System.out.println("Arrays match!");
    }

    static String printBytesAsLongs(byte[] byteArr) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < byteArr.length; i += 8) {
            for (int j = i + 7; j >= i; --j) {
                String str = Integer.toHexString(byteArr[j] & 0XFF);
                sb.append(Util.zeroPad(str, 2));
            }
            sb.append(Util.LS);

        }

        return sb.toString();
    }
}
