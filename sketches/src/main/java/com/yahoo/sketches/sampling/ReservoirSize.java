package com.yahoo.sketches.sampling;

import com.yahoo.sketches.Util;
import com.yahoo.sketches.theta.Intersection;

/**
 * This class provides a compact representation of reservoir size by encoding it into a fixed-point 16-bit value.
 * <p>The value itself is a fractional power of 2, with 5 bits of exponent and 11 bits of mantissa. The exponent
 * allows a choice of anywhere from 0-31, and there are 2048 possible reservoir size values within each octave.
 * Because reservoir size must be an integer, this means that some sizes below 2048 may have multiple valid encodings
 * .</p>
 *
 * <p>Reservoir sizes can be specified exactly to 4096, and may be off by up to 0.03% thereafter. The value returned
 * is always at least as large as the requested size, but may be larger.
 * </p>
 *
 * <p>NOTE: Numerical instability may cause an off-by-one error on reservoir size, causing a slight increase in
 * storage over the optimal value.</p>
 *
 * @author jmalkin
 */
public class ReservoirSize {
    /**
     * Number of bins per power of two.
     */
    private static final int BINS_PER_OCTAVE = 2048;

    /**
     * Precomputed inverse values for efficiency
     */
    private static final double INV_BINS_PER_OCTAVE = 1.0 / BINS_PER_OCTAVE;
    private static final double INV_LN_2 = 1.0 / Math.log(2.0);

    /**
     * Bit masks for encoding/decoding
     */
    private static final int EXPONENT_MASK  = 0X1F;
    private static final int EXPONENT_SHIFT = 11;
    private static final int INDEX_MASK     = 0X07FF;

    /**
     * Given target reservoir size k, computes the smallest representable reservoir size that can hold k entries and
     * returns it in a 16-bit fixed-point format.
     * @param k target reservoir size
     * @return reservoir size as 16-bit encoded value
     */
    public static int computeSize(final int k) {
        // find exponent as power of 2
        int p = Util.toLog2(Util.floorPowerOf2(k), "computeSize: p");

        // mantissa is scalar in range [1,2); can reconstruct k as m * 2^p
        double m = Math.pow(2.0, Math.log(k) * INV_LN_2 - p) - (1.0 / (BINS_PER_OCTAVE << 12));

        // Convert to index offset: ceil(m * BPO) - BPO
        // Additional multiple by BPO/BPO to deal with numerical precision issues
        // Should always be in range 0-(BINS_PER_OCTAVE-1)
        int i = (int) Math.ceil(m * BINS_PER_OCTAVE) - BINS_PER_OCTAVE;

        return ((p & EXPONENT_MASK) << EXPONENT_SHIFT) | (i & INDEX_MASK) & 0XFFFF;
    }

    /**
     * Decodes the 16-bit reservoir size value into an int.
     * @param value Encoded 16-bit value
     * @return int represented by value
     */
    public static int decodeValue(final int value) {
        int p = (value >> EXPONENT_SHIFT) & EXPONENT_MASK;
        int i = value & INDEX_MASK;

        return (int) ((1 << p) * (i * INV_BINS_PER_OCTAVE + 1.0));
    }

    public static void main(String[] args) {
        //int[] vals = {64, 65, 127, 128, 500, 1000, 4096, 4097, 5000, 10000, 50000, 100000, 100001, 1000000, 2500000};
        int[] vals = {4, 5, 6, 16, 17};

        for (int i = 0; i < vals.length; ++i) {
            int k = vals[i];
            System.out.println("k = " + k);

            int s = computeSize(k);
            System.out.println("Encoded value: " + Util.zeroPad(Integer.toBinaryString(s), 16));

            int kk = decodeValue(s);
            System.out.println("Decoded value: " + kk);

            System.out.println("Error: " + (kk - k) + Util.LS);

        }

    }

}
