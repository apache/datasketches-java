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

package org.apache.datasketches.filters.quotientfilter;

/*
Copyright � 1999 CERN - European Organization for Nuclear Research.
Permission to use, copy, modify, distribute and sell this software and its documentation for any purpose
is hereby granted without fee, provided that the above copyright notice appear in all copies and
that both that copyright notice and this permission notice appear in supporting documentation.
CERN makes no representations about the suitability of this software for any purpose.
It is provided "as is" without expressed or implied warranty.
*/

/**
 * Implements quick non polymorphic non bounds checking low level bitvector operations.
 * Includes some operations that interpret sub-bitstrings as long integers.
 * <p>
 * <b>WARNING: Methods of this class do not check preconditions.</b>
 * Provided with invalid parameters these method may return (or set) invalid values without throwing any exception.
 * <b>You should only use this class when performance is critical and you are absolutely sure that indexes are within bounds.</b>
 * <p>
 * A bitvector is modelled as a long array, i.e. long[] bits holds bits of a bitvector.
 * Each long value holds 64 bits.
 * The i-th bit is stored in bits[i/64] at
 * bit position i % 64 (where bit position 0 refers to the least
 * significant bit and 63 refers to the most significant bit).
 *
 * @author wolfgang.hoschek@cern.ch
 * @version 1.0, 09/24/99
 * @see     java.util.BitSet
 */
//package bitmap_implementations;

public class QuickBitVector extends Object {
    protected final static int ADDRESS_BITS_PER_UNIT = 6; // 64=2^6
    protected final static int BITS_PER_UNIT = 64; // = 1 << ADDRESS_BITS_PER_UNIT
    protected final static int BIT_INDEX_MASK = 63; // = BITS_PER_UNIT - 1;

    private static final long[] pows = precomputePows(); //precompute bitmasks for speed
    /**
     * Makes this class non instantiable, but still inheritable.
     */
    protected QuickBitVector() {
    }
    /**
     * Returns a bit mask with bits in the specified range set to 1, all the rest set to 0.
     * In other words, returns a bit mask having 0,1,2,3,...,64 bits set.
     * If to-from+1==0 then returns zero (0L).
     * Precondition (not checked): to-from+1 &ge; 0 AND to-from+1 &le; 64.
     *
     * @param from index of start bit (inclusive)
     * @param to index of end bit (inclusive).
     * @return the bit mask having all bits between from and to set to 1.
     */
    public static final long bitMaskWithBitsSetFromTo(long from, long to) {
        return pows[(int)(to-from+1)] << from;

        // This turned out to be slower:
        // 0xffffffffffffffffL == ~0L == -1L == all 64 bits set.
        // int width;
        // return (width=to-from+1) == 0 ? 0L : (0xffffffffffffffffL >>> (BITS_PER_UNIT-width)) << from;
    }
    /**
     * Changes the bit with index bitIndex in the bitvector bits to the "clear" (false) state.
     *
     * @param     bits   the bitvector.
     * @param     bitIndex   the index of the bit to be cleared.
     */
    public static void clear(long[] bits, long bitIndex) {
        bits[(int)(bitIndex >> ADDRESS_BITS_PER_UNIT)] &= ~(1L << (bitIndex & BIT_INDEX_MASK));
    }
    /**
     * Returns from the bitvector the value of the bit with the specified index.
     * The value is true if the bit with the index bitIndex
     * is currently set; otherwise, returns false.
     *
     * @param     bits   the bitvector.
     * @param     bitIndex   the bit index.
     * @return    the value of the bit with the specified index.
     */
    public static boolean get(long[] bits, long bitIndex) {
        return ((bits[(int)(bitIndex >> ADDRESS_BITS_PER_UNIT)] & (1L << (bitIndex & BIT_INDEX_MASK))) != 0);
    }
    /**
     * Returns a long value representing bits of a bitvector from index from to index to.
     * Bits are returned as a long value with the return value having bit 0 set to bit <code>from</code>, ..., bit <code>to-from</code> set to bit <code>to</code>.
     * All other bits of return value are set to 0.
     * If from &gt; to then returns zero (0L).
     * Precondition (not checked): to-from+1 &le; 64.
     * @param bits the bitvector.
     * @param from index of start bit (inclusive).
     * @param to index of end bit (inclusive).
     * @return the specified bits as long value.
     */
    public static long getLongFromTo(long[] bits, long from, long to) {
        if (from>to) return 0L;

        final int fromIndex = (int)(from >> ADDRESS_BITS_PER_UNIT); //equivalent to from/64
        final int toIndex = (int)(to >> ADDRESS_BITS_PER_UNIT);
        final int fromOffset = (int)(from & BIT_INDEX_MASK); //equivalent to from%64
        final int toOffset = (int)(to & BIT_INDEX_MASK);
        //this is equivalent to the above, but slower:
        //final int fromIndex=from/BITS_PER_UNIT;
        //final int toIndex=to/BITS_PER_UNIT;
        //final int fromOffset=from%BITS_PER_UNIT;
        //final int toOffset=to%BITS_PER_UNIT;


        long mask;
        if (fromIndex==toIndex) { //range does not cross unit boundaries; value to retrieve is contained in one single long value.
            mask=bitMaskWithBitsSetFromTo(fromOffset, toOffset);
            return (bits[fromIndex]	& mask) >>> fromOffset;

        }

        //range crosses unit boundaries; value to retrieve is spread over two long values.
        //get part from first long value
        mask=bitMaskWithBitsSetFromTo(fromOffset, BIT_INDEX_MASK);
        final long x1=(bits[fromIndex] & mask) >>> fromOffset;

        //get part from second long value
        mask=bitMaskWithBitsSetFromTo(0, toOffset);
        final long x2=(bits[toIndex] & mask) << (BITS_PER_UNIT-fromOffset);

        //combine
        return x1|x2;
    }
    /**
     Returns the index of the least significant bit in state "true".
     Returns 32 if no bit is in state "true".
     Examples:
     <pre>
     0x80000000 : 31
     0x7fffffff : 0
     0x00000001 : 0
     0x00000000 : 32
     </pre>
     */
    static public int leastSignificantBit(int value) {
        int i=-1;
        while (++i < 32 && (((1<<i) & value)) == 0);
        return i;
    }
    /**
     * Constructs a low level bitvector that holds size elements, with each element taking bitsPerElement bits.
     * CD. THIS METHOD ESSENTIALLY ROUNDS TO THE NEXT MULTIPLE OF 64 BITS.
     * @param     size   the number of elements to be stored in the bitvector (must be &ge; 0).
     * @param     bitsPerElement   the number of bits one single element takes.
     * @return    a low level bitvector.
     */
    public static long[] makeBitVector(long size, int bitsPerElement) {
        long nBits = size*bitsPerElement;
        //System.out.println("IN BITVECTOR");
        //System.out.println("Using " + nBits + " bits");
        long right_shift = ((nBits-1) >> ADDRESS_BITS_PER_UNIT) ; // This line basically does (nBits-1) / 2^ADDRESS...
        long safe_right_shift = ((nBits-1) >>> ADDRESS_BITS_PER_UNIT) ; // This line basically does (nBits-1) / 2^ADDRESS...
        // System.out.println("Right shift " + right_shift);
        //System.out.println("Safe Right shift " + safe_right_shift);
        int unitIndex = (int)((nBits-1) >> ADDRESS_BITS_PER_UNIT); // How many multiples of 64 bits do we need to store nBits bits?
        //System.out.println(ADDRESS_BITS_PER_UNIT);
        //System.out.println("unitIndex " + unitIndex);
        long[] bitVector = new long[unitIndex + 1];
        //System.out.println("length " + bitVector.length);
        //System.out.println("Total bits: " + (bitVector.length * 64));
        //System.out.println("Num slots available: " + (bitVector.length * 64) / bitsPerElement);
        return bitVector;
    }

    /**
     * Returns the index of the most significant bit in state "true".
     * Returns -1 if no bit is in state "true".
     *
     * Examples:
     * <pre>
     * 0x80000000 : 31
     * 0x7fffffff : 30
     * 0x00000001 : 0
     * 0x00000000 : -1
     * </pre>
     *
     * @param value The integer value for which the most significant bit index is to be found.
     * @return The index of the most significant bit in state "true". Returns -1 if no bit is in state "true".
     */
    static public int mostSignificantBit(int value) {
        int i=32;
        while (--i >=0 && (((1<<i) & value)) == 0);
        return i;
    }

    /**
     * Returns the index within the unit that contains the given bitIndex.
     *
     * @param bitIndex The index of the bit to be checked.
     * @return The index within the unit that contains the given bitIndex.
     */
    protected static long offset(long bitIndex) {
        return bitIndex & BIT_INDEX_MASK; // equivalent to bitIndex%64
    }
    /**
     * Initializes a table with numbers having 1,2,3,...,64 bits set.
     * pows[i] has bits [0..i-1] set.
     * pows[64] == -1L == ~0L == has all 64 bits set : correct.
     * to speedup calculations in subsequent methods.
     */
    private static long[] precomputePows() {
        long[] pows=new long[BITS_PER_UNIT+1];
        long value = ~0L;
        for (int i=BITS_PER_UNIT+1; --i >= 1; ) {
            pows[i]=value >>> (BITS_PER_UNIT-i);
            //System.out.println((i)+":"+pows[i]);
        }
        pows[0]=0L;
        return pows;
    }
    /**
     * Sets the bit with index bitIndex in the bitvector bits to the state specified by value.
     *
     * @param     bits   the bitvector.
     * @param     bitIndex   the index of the bit to be changed.
     * @param     value   the value to be stored in the bit.
     */
    public static void put(long[] bits, long bitIndex, boolean value) {
        if (value)
            set(bits, bitIndex);
        else
            clear(bits, bitIndex);
    }
    /**
     * Sets bits of a bitvector from index <code>from</code> to index <code>to</code> to the bits of <code>value</code>.
     * Bit <code>from</code> is set to bit 0 of <code>value</code>, ..., bit <code>to</code> is set to bit <code>to-from</code> of <code>value</code>.
     * All other bits stay unaffected.
     * If from > to then does nothing.
     * Precondition (not checked): to-from+1 <= 64.
     *
     * @param bits the bitvector.
     * @param value the value to be copied into the bitvector.
     * @param from index of start bit (inclusive).
     * @param to index of end bit (inclusive).
     */
    public static void putLongFromTo(long[] bits, long value, long from, long to) {
        if (from>to) return;

        final int fromIndex=(int)(from >> ADDRESS_BITS_PER_UNIT); //equivalent to from/64
        final int toIndex=(int)(to >> ADDRESS_BITS_PER_UNIT);
        final int fromOffset=(int)(from & BIT_INDEX_MASK); //equivalent to from%64
        final int toOffset=(int)(to & BIT_INDEX_MASK);
	/*
	this is equivalent to the above, but slower:
	int fromIndex=from/BITS_PER_UNIT;
	int toIndex=to/BITS_PER_UNIT;
	int fromOffset=from%BITS_PER_UNIT;
	int toOffset=to%BITS_PER_UNIT;
	*/

        //make sure all unused bits to the left are cleared.
        long mask;
        mask=bitMaskWithBitsSetFromTo(to-from+1, BIT_INDEX_MASK);
        long cleanValue=value & (~mask);

        long shiftedValue;

        if (fromIndex==toIndex) { //range does not cross unit boundaries; should go into one single long value.
            shiftedValue=cleanValue << fromOffset;
            mask=bitMaskWithBitsSetFromTo(fromOffset, toOffset);
            bits[fromIndex] = (bits[fromIndex] & (~mask)) | shiftedValue;
            return;

        }

        //range crosses unit boundaries; value should go into two long values.
        //copy into first long value.
        shiftedValue=cleanValue << fromOffset;
        mask=bitMaskWithBitsSetFromTo(fromOffset, BIT_INDEX_MASK);
        bits[fromIndex] = (bits[fromIndex] & (~mask)) | shiftedValue;

        //copy into second long value.
        shiftedValue=cleanValue >>> (BITS_PER_UNIT - fromOffset);
        mask=bitMaskWithBitsSetFromTo(0, toOffset);
        bits[toIndex] = (bits[toIndex] & (~mask)) | shiftedValue;
    }
    /**
     * Changes the bit with index bitIndex in the bitvector bits to the "set" (true) state.
     *
     * @param     bits   the bitvector.
     * @param     bitIndex   the index of the bit to be set.
     */
    public static void set(long[] bits, long bitIndex) {
        bits[(int)(bitIndex >> ADDRESS_BITS_PER_UNIT)] |= 1L << (bitIndex & BIT_INDEX_MASK);
    }
    /**
     * Returns the index of the unit that contains the given bitIndex.
     *
     * @param bitIndex The index of the bit to be checked.
     * @return The index of the unit that contains the given bitIndex.
     */
    protected static long unit(long bitIndex) {
        return bitIndex >> ADDRESS_BITS_PER_UNIT; // equivalent to bitIndex/64
    }
}
