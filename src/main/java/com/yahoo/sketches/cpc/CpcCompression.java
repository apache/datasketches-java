/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.CompressionData.columnPermutationsForDecoding;
import static com.yahoo.sketches.cpc.CompressionData.columnPermutationsForEncoding;
import static com.yahoo.sketches.cpc.CompressionData.decodingTablesForHighEntropyByte;
import static com.yahoo.sketches.cpc.CompressionData.encodingTablesForHighEntropyByte;
import static com.yahoo.sketches.cpc.CompressionData.lengthLimitedUnaryDecodingTable65;
import static com.yahoo.sketches.cpc.CompressionData.lengthLimitedUnaryEncodingTable65;
import static com.yahoo.sketches.cpc.PairTable.introspectiveInsertionSort;
//import static com.yahoo.sketches.cpc.RuntimeAsserts.rtAssertEquals;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class CpcCompression {
  //visible for test
  static final int NEXT_WORD_IDX = 0; //ptrArr[NEXT_WORD_IDX]
  static final int BIT_BUF = 1;       //ptrArr[BIT_BUF]
  static final int BUF_BITS = 2;      //ptrArr[BUF_BITS]

  //visible for test
  static void writeUnary(
      final int[] compressedWords,
      final long[] ptrArr,
      final int theValue) {

    int nextWordIndex = (int) ptrArr[NEXT_WORD_IDX]; //must be int
    assert (nextWordIndex == ptrArr[NEXT_WORD_IDX]); //catch truncation error
    long bitBuf = ptrArr[BIT_BUF];                   //must be long
    int bufBits = (int) ptrArr[BUF_BITS];            //could be byte

    assert (compressedWords != null);
    assert (nextWordIndex >= 0);
    assert (bitBuf >= 0);
    assert ((bufBits >= 0) && (bufBits <= 31));

    int remaining = theValue;

    while (remaining >= 16) {
      remaining -= 16;
      // Here we output 16 zeros, but we don't need to physically write them into bitbuf
      // because it already contains zeros in that region.
      bufBits += 16; // Record the fact that 16 bits of output have occurred.
      //MAYBE_FLUSH_BITBUF(compressedWords, nextWordIndex);
      if (bufBits >= 32) {
        compressedWords[nextWordIndex++] = (int) bitBuf;
        bitBuf >>>= 32;
        bufBits -= 32;
      }
    }

    assert (remaining >= 0) && (remaining <= 15);

    final long theUnaryCode = 1L << remaining; //must be a long
    bitBuf |= theUnaryCode << bufBits;
    bufBits += (1 + remaining);
    //MAYBE_FLUSH_BITBUF(compressedWords, nextWordIndex);
    if (bufBits >= 32) {
      compressedWords[nextWordIndex++] = (int) bitBuf;
      bitBuf >>>= 32;
      bufBits -= 32;
    }

    ptrArr[NEXT_WORD_IDX] = nextWordIndex;
    assert nextWordIndex == ptrArr[NEXT_WORD_IDX]; //catch sign extension error
    ptrArr[BIT_BUF] = bitBuf;
    ptrArr[BUF_BITS] = bufBits;
  }

  //visible for test
  static long readUnary(
      final int[] compressedWords,
      final long[] ptrArr) {

    int nextWordIndex = (int) ptrArr[NEXT_WORD_IDX]; //must be int
    assert nextWordIndex == ptrArr[NEXT_WORD_IDX]; //catch truncation error
    long bitBuf = ptrArr[BIT_BUF];
    int bufBits = (int) ptrArr[BUF_BITS];

    assert compressedWords != null;
    assert nextWordIndex >= 0;
    assert bitBuf >= 0;
    assert bufBits >= 0;

    long subTotal = 0;
    int trailingZeros;

    //readUnaryLoop:
    while (true) {
      //MAYBE_FILL_BITBUF(compressedWords,nextWordIndex,8); // ensure 8 bits in bit buffer
      if (bufBits < 8) {  // Prepare for an 8-bit peek into the bitstream.
        bitBuf |= ((compressedWords[nextWordIndex++] & 0XFFFF_FFFFL) << bufBits);
        bufBits += 32;
      }

      // These 8 bits include either all or part of the Unary codeword.
      final int peek8 = (int) (bitBuf & 0XFFL);
      trailingZeros = Math.min(8, Integer.numberOfTrailingZeros(peek8));

      assert ((trailingZeros >= 0) && (trailingZeros <= 8)) : "TZ+ " + trailingZeros;

      if (trailingZeros == 8) { // The codeword was partial, so read some more.
        subTotal += 8;
        bufBits -= 8;
        bitBuf >>>= 8;
        continue;
      }
      break;
    }

    bufBits -= (1 + trailingZeros);
    bitBuf >>>= (1 + trailingZeros);

    ptrArr[NEXT_WORD_IDX] = nextWordIndex;
    assert nextWordIndex == ptrArr[NEXT_WORD_IDX]; //catch sign extension error
    ptrArr[BIT_BUF] = bitBuf;
    ptrArr[BUF_BITS] = bufBits;
    return subTotal + trailingZeros;
  }

  /**
   * This returns the number of compressedWords that were actually used.
   * @param byteArray input
   * @param numBytesToEncode input
   * @param encodingTable input
   * @param compressedWords output
   * @return the number of compressedWords that were actually used.
   */
  //visible for test
  //It is the caller's responsibility to ensure that the compressedWords array is long enough.
  static int lowLevelCompressBytes(
      final byte[] byteArray,          // input
      final int numBytesToEncode,      // input, must be an int
      final short[] encodingTable,     // input
      final int[] compressedWords) {   // output

    int nextWordIndex = 0;
    long bitBuf = 0;  // bits are packed into this first, then are flushed to compressedWords
    int bufBits = 0;  // number of bits currently in bitbuf; must be between 0 and 31

    for (int byteIndex = 0; byteIndex < numBytesToEncode; byteIndex++) {
      final int theByte = byteArray[byteIndex] & 0XFF;
      final long codeInfo = (encodingTable[theByte] & 0XFFFFL);
      final long codeVal = codeInfo & 0XFFFL;
      final int codeWordLength = (int) (codeInfo >>> 12);
      bitBuf |= (codeVal << bufBits);
      bufBits += codeWordLength;
      //MAYBE_FLUSH_BITBUF(compressedWords, nextWordIndex);
      if (bufBits >= 32) {
        compressedWords[nextWordIndex++] = (int) bitBuf;
        bitBuf >>>= 32;
        bufBits -= 32;
      }
    }

    //Pad the bitstream with 11 zero-bits so that the decompressor's 12-bit peek
    // can't overrun its input.
    bufBits += 11;
    //MAYBE_FLUSH_BITBUF(compressedWords, nextWordIndex);
    if (bufBits >= 32) {
      compressedWords[nextWordIndex++] = (int) bitBuf;
      bitBuf >>>= 32;
      bufBits -= 32;
    }

    if (bufBits > 0) { // We are done encoding now, so we flush the bit buffer.
      assert (bufBits < 32);
      compressedWords[nextWordIndex++] = (int) bitBuf;
    }
    return nextWordIndex;
  }

  //visible for test
  static void lowLevelUncompressBytes(
      final byte[] byteArray,          // output
      final int numBytesToDecode,      // input (but refers to the output)
      final short[] decodingTable,     // input
      final int[] compressedWords,     // input
      final long numCompressedWords) { // input

    int byteIndex = 0;
    int nextWordIndex = 0;
    long bitBuf = 0;
    int bufBits = 0;

    assert (byteArray != null);
    assert (decodingTable != null);
    assert (compressedWords != null);

    for (byteIndex = 0; byteIndex < numBytesToDecode; byteIndex++) {
      //MAYBE_FILL_BITBUF(compressedWords,wordIndex,12); // ensure 12 bits in bit buffer
      if (bufBits < 12) { // Prepare for a 12-bit peek into the bitstream.
        bitBuf |= ((compressedWords[nextWordIndex++] & 0XFFFF_FFFFL) << bufBits);
        bufBits += 32;
      }

      // These 12 bits will include an entire Huffman codeword.
      final int peek12 = (int) (bitBuf & 0XFFFL);
      final int lookup = decodingTable[peek12] & 0XFFFF;
      final int codeWordLength = lookup >>> 8;
      final byte decodedByte = (byte) (lookup & 0XFF);
      byteArray[byteIndex] = decodedByte;
      bitBuf >>>= codeWordLength;
      bufBits -= codeWordLength;
    }

    // Buffer over-run should be impossible unless there is a bug.
    // However, we might as well check here.
    assert (nextWordIndex <= numCompressedWords);
  }

  /**
   * Here "pairs" refers to row/column pairs that specify the positions of surprising values in
   * the bit matrix.
   * @param pairArray input
   * @param numPairsToEncode input
   * @param numBaseBits input
   * @param compressedWords output
   * @return the number of compressedWords actually used
   */
  //visible for test
  static long lowLevelCompressPairs(
      final int[] pairArray,         // input
      final int numPairsToEncode,    // input
      final int numBaseBits,         // input //cannot exceed 63 or 6 bits, could be byte
      final int[] compressedWords) { // output

    int pairIndex = 0;

    final long[] ptrArr = new long[3];
    int nextWordIndex = 0; //must be int
    long bitBuf = 0;       //must be long
    int bufBits = 0;       //could be byte

    final long golombLoMask = (1L << numBaseBits) - 1L;

    int predictedRowIndex = 0;
    int predictedColIndex = 0;

    for (pairIndex = 0; pairIndex < numPairsToEncode; pairIndex++) {
      final int rowCol = pairArray[pairIndex];
      final int rowIndex = rowCol >>> 6;
      final int colIndex = rowCol & 0X3F; //63

      if (rowIndex != predictedRowIndex) { predictedColIndex = 0; }

      assert (rowIndex >= predictedRowIndex);
      assert (colIndex >= predictedColIndex);

      final long yDelta = rowIndex - predictedRowIndex; //cannot exceed 2^26
      final int xDelta = colIndex - predictedColIndex; //cannot exceed 65

      predictedRowIndex = rowIndex;
      predictedColIndex = colIndex + 1;

      final long codeInfo = lengthLimitedUnaryEncodingTable65[xDelta] & 0XFFFFL;
      final long codeVal = codeInfo & 0XFFFL;
      final int codeLen = (int) (codeInfo >>> 12);
      bitBuf |= (codeVal << bufBits);
      bufBits += codeLen;
      //MAYBE_FLUSH_BITBUF(compressedWords, nextWordIndex);
      if (bufBits >= 32) {
        compressedWords[nextWordIndex++] = (int) bitBuf;
        bitBuf >>>= 32;
        bufBits -= 32;
      }

      final long golombLo = yDelta & golombLoMask; //long for bitBuf
      final long golombHi = yDelta >>> numBaseBits; //cannot exceed 2^26

      //TODO Inline WriteUnary
      ptrArr[NEXT_WORD_IDX] = nextWordIndex;
      ptrArr[BIT_BUF] = bitBuf;
      ptrArr[BUF_BITS] = bufBits;
      assert nextWordIndex == ptrArr[NEXT_WORD_IDX]; //catch sign extension error

      writeUnary(compressedWords, ptrArr, (int) golombHi);

      nextWordIndex = (int) ptrArr[NEXT_WORD_IDX];
      bitBuf = ptrArr[BIT_BUF];
      bufBits = (int) ptrArr[BUF_BITS];
      assert nextWordIndex == ptrArr[NEXT_WORD_IDX]; //catch truncation error
      //END Inline WriteUnary

      bitBuf |= golombLo << bufBits;
      bufBits += numBaseBits;
      //MAYBE_FLUSH_BITBUF(compressedWords, nextWordIndex);
      if (bufBits >= 32) {
        compressedWords[nextWordIndex++] = (int) bitBuf;
        bitBuf >>>= 32;
        bufBits -= 32;
      }
    }

    // Pad the bitstream so that the decompressor's 12-bit peek can't overrun its input.
    int padding = 10 - numBaseBits;
    if (padding < 0) { padding = 0; }
    bufBits += padding;
    //MAYBE_FLUSH_BITBUF(compressedWords, nextWordIndex);
    if (bufBits >= 32) {
      compressedWords[nextWordIndex++] = (int) bitBuf;
      bitBuf >>>= 32;
      bufBits -= 32;
    }

    if (bufBits > 0) { // We are done encoding now, so we flush the bit buffer.
      assert (bufBits < 32);
      compressedWords[nextWordIndex++] = (int) bitBuf;
      //bitBuf = 0;
      //bufBits = 0; // not really necessary
    }
    return nextWordIndex;
  }

  //visible for test
  static void lowLevelUncompressPairs(
      final int[] pairArray,           // output
      final int numPairsToDecode,      // input, size of output, must be int
      final int numBaseBits,           // input, cannot exceed 6 bits
      final int[] compressedWords,     // input
      final long numCompressedWords) { // input

    int pairIndex = 0;

    final long[] ptrArr = new long[3];
    int nextWordIndex = 0;
    long bitBuf = 0;
    int bufBits = 0;

    final long golombLoMask = (1L << numBaseBits) - 1L;

    int predictedRowIndex = 0;
    int predictedColIndex = 0;

    // for each pair we need to read:
    // xDelta (12-bit length-limited unary)
    // yDeltaHi (unary)
    // yDeltaLo (basebits)

    for (pairIndex = 0; pairIndex < numPairsToDecode; pairIndex++) {

      //MAYBE_FILL_BITBUF(compressedWords,wordIndex,12); // ensure 12 bits in bit buffer
      if (bufBits < 12) { // Prepare for a 12-bit peek into the bitstream.
        bitBuf |= ((compressedWords[nextWordIndex++] & 0XFFFF_FFFFL) << bufBits);
        bufBits += 32;
      }

      final int peek12 = (int) (bitBuf & 0XFFFL);
      final int lookup = lengthLimitedUnaryDecodingTable65[peek12] & 0XFFFF;
      final int codeWordLength = lookup >>> 8;
      final int xDelta = lookup & 0XFF;
      bitBuf >>>= codeWordLength;
      bufBits -= codeWordLength;

      //TODO Inline ReadUnary
      ptrArr[NEXT_WORD_IDX] = nextWordIndex;
      ptrArr[BIT_BUF] = bitBuf;
      ptrArr[BUF_BITS] = bufBits;
      assert nextWordIndex == ptrArr[NEXT_WORD_IDX]; //catch sign extension error

      final long golombHi = readUnary(compressedWords, ptrArr);

      nextWordIndex = (int) ptrArr[NEXT_WORD_IDX];
      bitBuf = ptrArr[BIT_BUF];
      bufBits = (int) ptrArr[BUF_BITS];
      assert nextWordIndex == ptrArr[NEXT_WORD_IDX]; //catch truncation error
      //END Inline ReadUnary

      //MAYBE_FILL_BITBUF(compressedWords,wordIndex,numBaseBits); // ensure numBaseBits in bit buffer
      if (bufBits < numBaseBits) { // Prepare for a numBaseBits peek into the bitstream.
        bitBuf |= ((compressedWords[nextWordIndex++] & 0XFFFF_FFFFL) << bufBits);
        bufBits += 32;
      }

      final long golombLo = bitBuf & golombLoMask;
      bitBuf >>>= numBaseBits;
      bufBits -= numBaseBits;
      final long yDelta = (golombHi << numBaseBits) | golombLo;

      // Now that we have yDelta and xDelta, we can compute the pair's row and column.
      if (yDelta > 0) { predictedColIndex = 0; }
      final int rowIndex = predictedRowIndex + (int) yDelta;
      final int colIndex = predictedColIndex + xDelta;
      final int rowCol = (rowIndex << 6) | colIndex;
      pairArray[pairIndex] = rowCol;
      predictedRowIndex = rowIndex;
      predictedColIndex = colIndex + 1;
    }
    // check for buffer over-run
    assert (nextWordIndex <= numCompressedWords)
      : "nextWdIdx: " + nextWordIndex + ", #CompWds: " + numCompressedWords;
  }

  private static int safeLengthForCompressedPairBuf(
      final long k, final long numPairs, final long numBaseBits) {
    assert (numPairs > 0);
    // long ybits = k + numPairs; // simpler and safer UB
    // The following tighter UB on ybits is based on page 198
    // of the textbook "Managing Gigabytes" by Witten, Moffat, and Bell.
    // Notice that if numBaseBits == 0 it coincides with (k + numPairs).
    final long ybits = (numPairs * (1L + numBaseBits)) + (k >>> numBaseBits);
    final long xbits = 12 * numPairs;
    long padding = 10L - numBaseBits;
    if (padding < 0) { padding = 0; }
    final long bits = xbits + ybits + padding;
    //final long words = divideLongsRoundingUp(bits, 32);
    final long words = CpcCompression.divideBy32RoundingUp(bits);
    assert words < (1L << 31);
    return (int) words;
  }

  // Explanation of padding: we write
  // 1) xdelta (huffman, provides at least 1 bit, requires 12-bit lookahead)
  // 2) ydeltaGolombHi (unary, provides at least 1 bit, requires 8-bit lookahead)
  // 3) ydeltaGolombLo (straight B bits).
  // So the 12-bit lookahead is the tight constraint, but there are at least (2 + B) bits emitted,
  // so we would be safe with max (0, 10 - B) bits of padding at the end of the bitstream.
  private static int safeLengthForCompressedWindowBuf(final long k) { // measured in 32-bit words
    // 11 bits of padding, due to 12-bit lookahead, with 1 bit certainly present.
    final long bits = (12 * k) + 11;
    //cannot exceed Integer.MAX_VALUE
    //return (int) (divideLongsRoundingUp(bits, 32));
    return (int) CpcCompression.divideBy32RoundingUp(bits);
  }

  private static int determinePseudoPhase(final int lgK, final long numCoupons) {
    final long k = 1L << lgK;
    final long c = numCoupons;
    // This midrange logic produces pseudo-phases. They are used to select encoding tables.
    // The thresholds were chosen by hand after looking at plots of measured compression.
    if ((1000 * c) < (2375 * k)) {
      if      (   (4 * c) <    (3 * k)) { return ( 16 + 0 ); } // midrange table
      else if (  (10 * c) <   (11 * k)) { return ( 16 + 1 ); } // midrange table
      else if ( (100 * c) <  (132 * k)) { return ( 16 + 2 ); } // midrange table
      else if (   (3 * c) <    (5 * k)) { return ( 16 + 3 ); } // midrange table
      else if ((1000 * c) < (1965 * k)) { return ( 16 + 4 ); } // midrange table
      else if ((1000 * c) < (2275 * k)) { return ( 16 + 5 ); } // midrange table
      else { return 6; } // steady-state table employed before its actual phase
    }
    else {
      // This steady-state logic produces true phases. They are used to select
      // encoding tables, and also column permutations for the "Sliding" flavor.
      assert lgK >= 4;
      final long tmp = c >>> (lgK - 4);
      final int phase = (int) (tmp & 15L);
      assert (phase >= 0) && (phase < 16);
      return phase;
    }
  }

  private static void compressTheWindow(final CompressedState target, final CpcSketch source) {
    final int srcLgK = source.lgK;
    final int srcK = 1 << srcLgK;
    final int windowBufLen = safeLengthForCompressedWindowBuf(srcK);
    final int[] windowBuf = new int[windowBufLen];
    final int pseudoPhase = determinePseudoPhase(srcLgK, source.numCoupons);
    target.cwLengthInts = lowLevelCompressBytes(
        source.slidingWindow,
        srcK,
        encodingTablesForHighEntropyByte[pseudoPhase],
        windowBuf);

    // At this point we free the unused portion of the compression output buffer.
    //  final int[] shorterBuf = Arrays.copyOf(windowBuf, target.cwLength);
    //  target.compressedWindow = shorterBuf;
    target.cwStream = windowBuf; //avoid extra copy
  }

  private static void uncompressTheWindow(final CpcSketch target, final CompressedState source) {
    final int srcLgK = source.lgK;
    final int srcK = 1 << srcLgK;
    final byte[] window = new byte[srcK];
    // bzero ((void *) window, (size_t) k); // zeroing not needed here (unlike the Hybrid Flavor)
    assert (target.slidingWindow == null);
    target.slidingWindow = window;
    final int pseudoPhase = determinePseudoPhase(srcLgK, source.numCoupons);
    assert (source.cwStream != null);
    lowLevelUncompressBytes(target.slidingWindow, srcK,
           decodingTablesForHighEntropyByte[pseudoPhase],
           source.cwStream,
           source.cwLengthInts);
  }

  private static void compressTheSurprisingValues(final CompressedState target, final CpcSketch source,
      final int[] pairs, final int numPairs) {
    assert (numPairs > 0);
    target.numCsv = numPairs;
    final int srcK = 1 << source.lgK;
    final int numBaseBits = CpcCompression.golombChooseNumberOfBaseBits(srcK + numPairs, numPairs);
    final int pairBufLen = safeLengthForCompressedPairBuf(srcK, numPairs, numBaseBits);
    final int[] pairBuf = new int[pairBufLen];

    target.csvLengthInts = (int) lowLevelCompressPairs(pairs, numPairs, numBaseBits, pairBuf);

    // At this point we free the unused portion of the compression output buffer.
    //  final int[] shorterBuf = Arrays.copyOf(pairBuf, target.csvLength);
    //  target.compressedWindow = shorterBuf;

    target.csvStream = pairBuf; //avoid extra copy
  }

  //allocates and returns an array of uncompressed pairs.
  //the length of this array is known to the source sketch.
  private static int[] uncompressTheSurprisingValues(final CompressedState source) {
    final int srcK = 1 << source.lgK;
    final int numPairs = source.numCsv;
    assert numPairs > 0;
    final int[] pairs = new int[numPairs];
    final int numBaseBits = CpcCompression.golombChooseNumberOfBaseBits(srcK + numPairs, numPairs);
    lowLevelUncompressPairs(pairs, numPairs, numBaseBits, source.csvStream, source.csvLengthInts);
    return pairs;
  }

  private static void compressSparseFlavor(final CompressedState target, final CpcSketch source) {
    assert (source.slidingWindow == null); //there is no window to compress
    final PairTable srcPairTable = source.pairTable;
    final int srcNumPairs = srcPairTable.getNumPairs();
    final int[] srcPairArr = PairTable.unwrappingGetItems(srcPairTable, srcNumPairs);
    introspectiveInsertionSort(srcPairArr, 0, srcNumPairs - 1);
    compressTheSurprisingValues(target, source, srcPairArr, srcNumPairs);
  }

  private static void uncompressSparseFlavor(final CpcSketch target, final CompressedState source) {
    assert (source.cwStream == null);
    assert (source.csvStream != null);
    final int[] srcPairArr = uncompressTheSurprisingValues(source);
    final int numPairs = source.numCsv;
    final PairTable table = PairTable.newInstanceFromPairsArray(srcPairArr, numPairs, source.lgK);
    target.pairTable = table;
  }

  //The empty space that this leaves at the beginning of the output array
  // will be filled in later by the caller.
  private static int[] trickyGetPairsFromWindow(final byte[] window, final int k, final int numPairsToGet,
      final int emptySpace) {
    final int outputLength = emptySpace + numPairsToGet;
    final int[] pairs = new int[outputLength];
    int rowIndex = 0;
    int pairIndex = emptySpace;
    for (rowIndex = 0; rowIndex < k; rowIndex++) {
      int wByte = window[rowIndex] & 0XFF;
      while (wByte != 0) {
        final int colIndex = Integer.numberOfTrailingZeros(wByte);
        //      assert (colIndex < 8);
        wByte ^= (1 << colIndex); // erase the 1
        pairs[pairIndex++] = (rowIndex << 6) | colIndex;
      }
    }
    assert (pairIndex == outputLength);
    return (pairs);
  }

  //This is complicated because it effectively builds a Sparse version
  //of a Pinned sketch before compressing it. Hence the name Hybrid.
  private static void compressHybridFlavor(final CompressedState target, final CpcSketch source) {
    final int srcK = 1 << source.lgK;
    final PairTable srcPairTable = source.pairTable;
    final int srcNumPairs = srcPairTable.getNumPairs();
    final int[] srcPairArr = PairTable.unwrappingGetItems(srcPairTable, srcNumPairs);
    introspectiveInsertionSort(srcPairArr, 0, srcNumPairs - 1);
    final byte[] srcSlidingWindow = source.slidingWindow;
    final int srcWindowOffset = source.windowOffset;
    final long srcNumCoupons = source.numCoupons;
    assert (srcSlidingWindow != null);
    assert (srcWindowOffset == 0);
    final long numPairs = srcNumCoupons - srcNumPairs; // because the window offset is zero
    assert numPairs < Integer.MAX_VALUE;
    final int numPairsFromArray = (int) numPairs;

    assert (numPairsFromArray + srcNumPairs) == srcNumCoupons; //for test
    final int[] allPairs
      = trickyGetPairsFromWindow(srcSlidingWindow, srcK, numPairsFromArray, srcNumPairs);

    PairTable.merge(srcPairArr, 0, srcNumPairs,
        allPairs, srcNumPairs, numPairsFromArray,
        allPairs, 0);  // note the overlapping subarray trick

    //FOR TESTING If needed
    //        for (int i = 0; i < (source.numCoupons - 1); i++) {
    //          assert (Integer.compareUnsigned(allPairs[i], allPairs[i + 1]) < 0); }

    compressTheSurprisingValues(target, source, allPairs, (int) srcNumCoupons);
  }

  private static void uncompressHybridFlavor(final CpcSketch target, final CompressedState source) {
    assert (source.cwStream == null);
    assert (source.csvStream != null);
    final int[] pairs = uncompressTheSurprisingValues(source); //fail path 3
    final int numPairs = source.numCsv;
    // In the hybrid flavor, some of these pairs actually
    // belong in the window, so we will separate them out,
    // moving the "true" pairs to the bottom of the array.
    final int srcLgK = source.lgK;
    final int k = 1 << srcLgK;

    final byte[] window = new byte[k];

    int nextTruePair = 0;

    for (int i = 0; i < numPairs; i++) {
      final int rowCol = pairs[i];
      assert (rowCol != -1);
      final int col = rowCol & 63;
      if (col < 8) {
        final int row = rowCol >>> 6;
        window[row] |= (1 << col); // set the window bit
      }
      else {
        pairs[nextTruePair++] = rowCol; // move true pair down
      }
    }

    assert (source.getWindowOffset() == 0);
    target.windowOffset = 0;

    final PairTable table = PairTable.newInstanceFromPairsArray(pairs, nextTruePair, srcLgK);
    target.pairTable = table;
    target.slidingWindow = window;
  }

  private static void compressPinnedFlavor(final CompressedState target, final CpcSketch source) {
    compressTheWindow(target, source);
    final PairTable srcPairTable = source.pairTable;
    final int numPairs = srcPairTable.getNumPairs();

    if (numPairs > 0) {
      final int[] pairs = PairTable.unwrappingGetItems(srcPairTable, numPairs);

      // Here we subtract 8 from the column indices.  Because they are stored in the low 6 bits
      // of each rowCol pair, and because no column index is less than 8 for a "Pinned" sketch,
      // I believe we can simply subtract 8 from the pairs themselves.

      // shift the columns over by 8 positions before compressing (because of the window)
      for (int i = 0; i < numPairs; i++) {
        assert (pairs[i] & 63) >= 8;
        pairs[i] -= 8;
      }

      introspectiveInsertionSort(pairs, 0, numPairs - 1);
      compressTheSurprisingValues(target, source, pairs, numPairs);
    }
  }

  private static void uncompressPinnedFlavor(final CpcSketch target, final CompressedState source) {
    assert (source.cwStream != null);
    uncompressTheWindow(target, source);
    final int srcLgK = source.lgK;
    final int numPairs = source.numCsv;
    if (numPairs == 0) {
      target.pairTable = new PairTable(2, 6 + srcLgK);
    }
    else {
      assert numPairs > 0;
      assert source.csvStream != null;
      final int[] pairs = uncompressTheSurprisingValues(source);
      // undo the compressor's 8-column shift
      for (int i = 0; i < numPairs; i++) {
        assert (pairs[i] & 63) < 56;
        pairs[i] += 8;
      }
      final PairTable table = PairTable.newInstanceFromPairsArray(pairs, numPairs, srcLgK);
      target.pairTable = table;
    }
  }

  //Complicated by the existence of both a left fringe and a right fringe.
  private static void compressSlidingFlavor(final CompressedState target, final CpcSketch source) {

    compressTheWindow(target, source);
    final PairTable srcPairTable = source.pairTable;

    final int numPairs = srcPairTable.getNumPairs();

    if (numPairs > 0) {
      final int[] pairs = PairTable.unwrappingGetItems(srcPairTable, numPairs);

      // Here we apply a complicated transformation to the column indices, which
      // changes the implied ordering of the pairs, so we must do it before sorting.

      final int pseudoPhase = determinePseudoPhase(source.lgK, source.numCoupons); // NB
      assert (pseudoPhase < 16);
      final byte[] permutation = columnPermutationsForEncoding[pseudoPhase];

      final int offset = source.windowOffset;
      assert ((offset > 0) && (offset <= 56));

      for (int i = 0; i < numPairs; i++) {
        final int rowCol = pairs[i];
        final int  row = rowCol >>> 6;
        int col = (rowCol & 63);
        // first rotate the columns into a canonical configuration:
        //   new = ((old - (offset+8)) + 64) mod 64
        col = ((col + 56) - offset) & 63;
        assert (col >= 0) && (col < 56);
        // then apply the permutation
        col = permutation[col];
        pairs[i] = (row << 6) | col;
      }

      introspectiveInsertionSort(pairs, 0, numPairs - 1);
      compressTheSurprisingValues(target, source, pairs, numPairs);
    }
  }

  private static void uncompressSlidingFlavor(final CpcSketch target, final CompressedState source) {
    assert (source.cwStream != null);
    uncompressTheWindow(target, source);
    final int srcLgK = source.lgK;
    final int numPairs = source.numCsv;
    if (numPairs == 0) {
      target.pairTable = new PairTable(2, 6 + srcLgK);
    }
    else {
      assert (numPairs > 0);
      assert (source.csvStream != null);
      final int[] pairs = uncompressTheSurprisingValues(source);
      final int pseudoPhase = determinePseudoPhase(srcLgK, source.numCoupons); // NB
      assert (pseudoPhase < 16);
      final byte[] permutation = columnPermutationsForDecoding[pseudoPhase];

      final int offset = source.getWindowOffset();
      assert (offset > 0) && (offset <= 56);

      for (int i = 0; i < numPairs; i++) {
        final int rowCol = pairs[i];
        final int row = rowCol >>> 6;
        int col = rowCol & 63;
        // first undo the permutation
        col = permutation[col];
        // then undo the rotation: old = (new + (offset+8)) mod 64
        col = (col + (offset + 8)) & 63;
        pairs[i] = (row << 6) | col;
      }

      final PairTable table = PairTable.newInstanceFromPairsArray(pairs, numPairs, srcLgK);
      target.pairTable = table;
    }
  }

  static CompressedState compress(final CpcSketch source, final CompressedState target) {

    final Flavor srcFlavor = source.getFlavor();

    switch (srcFlavor) {
      case EMPTY: break;
      case SPARSE:
        compressSparseFlavor(target, source);
        assert (target.cwStream == null);
        assert (target.csvStream != null);
        break;
      case HYBRID:
        compressHybridFlavor(target, source);
        assert (target.cwStream == null);
        assert (target.csvStream != null);
        break;
      case PINNED:
        compressPinnedFlavor(target, source);
        assert (target.cwStream != null);
        break;
      case SLIDING:
        compressSlidingFlavor(target, source);
        assert (target.cwStream != null);
        break;
        //default: not possible
    }

    return target;
   }

  static CpcSketch uncompress(final CompressedState source, final CpcSketch target) {
    assert (target != null);

    final Flavor srcFlavor = source.getFlavor();
    switch (srcFlavor) {
      case EMPTY: break;
      case SPARSE:
        assert (source.cwStream == null);
        uncompressSparseFlavor(target, source);
        break;
      case HYBRID:
        uncompressHybridFlavor(target, source);
        break;
      case PINNED:
        assert (source.cwStream != null);
        uncompressPinnedFlavor(target, source);
        break;
      case SLIDING:
        uncompressSlidingFlavor(target, source);
        break;
        //default: not possible
    }
    return target;
  }

  private static int golombChooseNumberOfBaseBits(final int k, final long count) {
    assert k >= 1L;
    assert count >= 1L;
    final long quotient = (k - count) / count; // integer division
    if (quotient == 0) { return 0; }
    return (int) floorLog2ofLong(quotient);
  }

  private static long floorLog2ofLong(final long x) { //not a good name
    assert (x >= 1L);
    long p = 0;
    long y = 1;
    while (true) {
      if (y == x) { return p; }
      if (y  > x) { return p - 1; }
      p  += 1;
      y <<= 1;
    }
  }


  private static long divideBy32RoundingUp(final long x) {
    final long tmp = x >>> 5;
    return ((tmp << 5) == x) ? tmp : tmp + 1;
  }

}
