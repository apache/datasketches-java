/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.CompressionData.lengthLimitedUnaryDecodingTable65;
import static com.yahoo.sketches.cpc.CompressionData.lengthLimitedUnaryEncodingTable65;
import static com.yahoo.sketches.cpc.Fm85Util.byteTrailingZerosTable;
import static com.yahoo.sketches.cpc.Fm85Util.divideLongsRoundingUp;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class Fm85Compression {

  static final int NEXT_WORD_IDX = 0; //ptrArr[NEXT_WORD_IDX]
  static final int BIT_BUF = 1;       //ptrArr[BIT_BUF]
  static final int BUF_BITS = 2;      //ptrArr[BUF_BITS]

  /***************************************************************/
  /***************************************************************/

  static void writeUnary(
      final int[] compressedWords,
      final long[] ptrArr,
      final int theValue) { //is long required?

    int nextWordIndex = (int) ptrArr[NEXT_WORD_IDX]; //must be int
    long bitBuf = ptrArr[BIT_BUF];                   //must be long
    int bufBits = (int) ptrArr[BUF_BITS];            //could be byte

    assert nextWordIndex == ptrArr[NEXT_WORD_IDX];
    assert compressedWords != null;
    assert nextWordIndex >= 0;
    assert bitBuf >= 0;
    assert (bufBits >= 0) && (bufBits <= 31);

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
    ptrArr[BIT_BUF] = bitBuf;
    ptrArr[BUF_BITS] = bufBits;
    assert nextWordIndex == ptrArr[NEXT_WORD_IDX];
  }

  /***************************************************************/
  /***************************************************************/

  static long readUnary(
      final int[] compressedWords,
      final long[] ptrArr) {

    int nextWordIndex = (int) ptrArr[NEXT_WORD_IDX];
    long bitBuf = ptrArr[BIT_BUF];
    int bufBits = (int) ptrArr[BUF_BITS];

    assert nextWordIndex == ptrArr[NEXT_WORD_IDX];
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
      trailingZeros = byteTrailingZerosTable[peek8] & 0XFF;

      assert ((trailingZeros >= 0) && (trailingZeros <= 8));

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
    ptrArr[BIT_BUF] = bitBuf;
    ptrArr[BUF_BITS] = bufBits;
    assert nextWordIndex == ptrArr[NEXT_WORD_IDX];
    return subTotal + trailingZeros;
  }

  /***************************************************************/
  /***************************************************************/

  /**
   * This returns the number of compressedWords that were actually used.
   * @param byteArray input
   * @param numBytesToEncode input
   * @param encodingTable input
   * @param compressedWords output
   * @return the number of compressedWords that were actually used.
   */
  //It is the caller's responsibility to ensure that the compressedWords array is long enough.
  static long lowLevelCompressBytes(
      final byte[] byteArray,          // input
      final int numBytesToEncode,      // input //must be an int
      final short[] encodingTable,     // input
      final int[] compressedWords) {   // output

    int nextWordIndex = 0;
    long bitBuf = 0;  // bits are packed into this first, then are flushed to compressedWords
    int bufBits = 0;  // number of bits currently in bitbuf; must be between 0 and 31

    for (int byteIndex = 0; byteIndex < numBytesToEncode; byteIndex++) {
      final int theByte = byteArray[byteIndex] & 0XFF;
      final long codeInfo = (encodingTable[theByte] & 0XFFFFL);
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
      //bitBuf = 0;
      //bufBits = 0; // not really necessary
    }
    return nextWordIndex;
  }

  /***************************************************************/
  /***************************************************************/

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
      final int peek12 = (int) (bitBuf & 0xfffL);
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
    return;
  }

  /***************************************************************/
  /***************************************************************/

  /**
   * Here "pairs" refers to row/column pairs that specify the positions of surprising values in
   * the bit matrix.
   * @param pairArray input
   * @param numPairsToEncode input
   * @param numBaseBits input
   * @param compressedWords output
   * @return the number of compressedWords actually used
   */
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

    final int golombLoMask = (1 << numBaseBits) - 1;

    int predictedRowIndex = 0;
    int predictedColIndex = 0;

    for (pairIndex = 0; pairIndex < numPairsToEncode; pairIndex++) {
      final int rowCol = pairArray[pairIndex];
      final int rowIndex = rowCol >>> 6;
      final int colIndex = rowCol & 0X3F; //63

      if (rowIndex != predictedRowIndex) { predictedColIndex = 0; }

      assert (rowIndex >= predictedRowIndex);
      assert (colIndex >= predictedColIndex);

      final int yDelta = rowIndex - predictedRowIndex; //cannot exceed 2^26
      final int xDelta = colIndex - predictedColIndex;

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

      final int golombLo = yDelta & golombLoMask; //long for bitBuf
      final int golombHi = yDelta >>> numBaseBits; //cannot exceed 2^26, could be int

      //println("i: " + pairIndex + ", X: " + xDelta + ", gHi: " + golombHi + ", gLo: " + golombLo);

      //TODO Inline WriteUnary
      ptrArr[NEXT_WORD_IDX] = nextWordIndex;
      ptrArr[BIT_BUF] = bitBuf;
      ptrArr[BUF_BITS] = bufBits;
      assert nextWordIndex == ptrArr[NEXT_WORD_IDX]; //catch sign extension error
      writeUnary(compressedWords, ptrArr, golombHi);
      nextWordIndex = (int) ptrArr[NEXT_WORD_IDX];
      bitBuf = ptrArr[BIT_BUF];
      bufBits = (int) ptrArr[BUF_BITS];
      assert nextWordIndex == ptrArr[NEXT_WORD_IDX]; //catch truncation error
      //END Inline WriteUnary

      bitBuf |= (((long) golombLo) << bufBits);
      bufBits += numBaseBits;
      //MAYBE_FLUSH_BITBUF(compressedWords, nextWordIndex);
      if (bufBits >= 32) {
        compressedWords[nextWordIndex++] = (int) bitBuf;
        bitBuf >>>= 32;
        bufBits -= 32;
      }
    }

    // Pad the bitstream so that the decompressor's 12-bit peek can't overrun its input.
    long padding = 10L - numBaseBits;
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

  /***************************************************************/
  /***************************************************************/

  static void lowLevelUncompressPairs(
      final int[] pairArray,           // output
      final long numPairsToDecode,     // input (but refers to the output)
      final int numBaseBits,           // input cannot exceed 6 bits
      final int[] compressedWords,     // input
      final long numCompressedWords) { // input

    int pairIndex = 0;

    final long[] ptrArr = new long[3];
    int nextWordIndex = 0;
    long bitBuf = 0;
    int bufBits = 0;

    final long golombLoMask = (1L << numBaseBits) - 1;

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

      //Long golombHi = readUnary (compressedWords, &wordIndex, &bitbuf, &bufbits);

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
      if (bufBits < 12) { // Prepare for a 12-bit peek into the bitstream.
        bitBuf |= ((compressedWords[nextWordIndex++] & 0XFFFF_FFFFL) << bufBits);
        bufBits += 32;
      }

      final long golombLo = bitBuf & golombLoMask;
      //println("i: " + pairIndex + ", X: " + xDelta + ", gHi: " + golombHi + ", gLo: " + golombLo);

      bitBuf >>>= numBaseBits;
      bufBits -= numBaseBits;
      final int yDelta = (int) ((golombHi << numBaseBits) | golombLo);

      // Now that we have yDelta and xDelta, we can compute the pair's row and column.
      if (yDelta > 0) { predictedColIndex = 0; }
      final int rowIndex = predictedRowIndex + yDelta;
      final int colIndex = predictedColIndex + xDelta;
      final int rowCol = (rowIndex << 6) | colIndex;
      pairArray[pairIndex] = rowCol;
      predictedRowIndex = rowIndex;
      predictedColIndex = colIndex + 1;
    }
    assert (nextWordIndex <= numCompressedWords); // check for buffer over-run
  }

  /***************************************************************/
  /***************************************************************/

  static long safeLengthForCompressedPairBuf(
      final long k, final long numPairs, final long numBaseBits) {
    assert (numPairs > 0);
    // Long ybits = k + numPairs; // simpler and safer UB
    // The following tighter UB on ybits is based on page 198
    // of the textbook "Managing Gigabytes" by Witten, Moffat, and Bell.
    // Notice that if numBaseBits == 0 it coincides with (k + numPairs).
    final long ybits = (numPairs * (1L + numBaseBits)) + (k >>> numBaseBits);
    final long xbits = 12 * numPairs;
    long padding = 10L - numBaseBits;
    if (padding < 0) {
      padding = 0;
    }
    final long bits = xbits + ybits + padding;
    return (divideLongsRoundingUp(bits, 32));
  }

  // Explanation of padding: we write
  // 1) xdelta (huffman, provides at least 1 bit, requires 12-bit lookahead)
  // 2) ydeltaGolombHi (unary, provides at least 1 bit, requires 8-bit lookahead)
  // 3) ydeltaGolombLo (straight B bits).
  // So the 12-bit lookahead is the tight constraint, but there are at least (2 + B) bits emitted,
  // so we would be safe with max (0, 10 - B) bits of padding at the end of the bitstream.


  /***************************************************************/
  /***************************************************************/


  static int safeLengthForCompressedWindowBuf(final long k) { // measured in 32-bit words
    // 11 bits of padding, due to 12-bit lookahead, with 1 bit certainly present.
    final long bits = (12 * k) + 11;
    return (int) (divideLongsRoundingUp(bits, 32));
  }

  /***************************************************************/
  /***************************************************************/

  static short determinePseudoPhase(final short lgK, final long c) {
    final long k = (1L << lgK);
    // This midrange logic produces pseudo-phases. They are used to select encoding tables.
    // The thresholds were chosen by hand after looking at plots of measured compression.
    if ((1000 * c) < (2375 * k)) {
      if      (   (4 * c) <    (3 * k)) {
        return ( 16 + 0 );  // midrange table
      } else if (  (10 * c) <   (11 * k)) {
        return ( 16 + 1 );  // midrange table
      } else if ( (100 * c) <  (132 * k)) {
        return ( 16 + 2 );  // midrange table
      } else if (   (3 * c) <    (5 * k)) {
        return ( 16 + 3 );  // midrange table
      } else if ((1000 * c) < (1965 * k)) {
        return ( 16 + 4 );  // midrange table
      } else if ((1000 * c) < (2275 * k)) {
        return ( 16 + 5 );  // midrange table
      }
      else {
        return ( 6 );  // steady-state table employed before its actual phase
      }
    }
    else { // This steady-state logic produces true phases. They are used to select
      // encoding tables, and also column permutations for the "Sliding" flavor.
      assert (lgK >= 4);
      final long tmp = c >>> (lgK - 4);
      final long phase = tmp & 15;
      assert ((phase >= 0) && (phase < 16));
      return ((short) phase);
    }
  }

  /***************************************************************/
  /***************************************************************/

//  void compressTheWindow(final Fm85 target, final Fm85 source) {
//    final long k = (1L << source.lgK);
//    final int windowBufLen = safeLengthForCompressedWindowBuf(k);
//    final int[] windowBuf = new int[windowBufLen];
//    assert (windowBuf != null);
//    Short pseudoPhase = determinePseudoPhase (source->lgK, source->numCoupons);
//    target.cwLength = lowLevelCompressBytes (source->slidingWindow, k,
//                encodingTablesForHighEntropyByte[pseudoPhase],
//                windowBuf);
//
//    // At this point we free the unused portion of the compression output buffer.
//    // Note: realloc caused strange timing spikes for lgK = 11 and 12.
//
//    final int[] shorterBuf = new int[target.cwLength];
//    if (shorterBuf == null) { throw new SketchesStateException("Out of Memory"); }
//    memcpy ((void *) shorterBuf, (void *) windowBuf, ((size_t) target->cwLength) * sizeof(U32));
//    free (windowBuf);
//    target->compressedWindow = shorterBuf;
//
//    return;
//  }


  /***************************************************************/
  /***************************************************************/

//
//  void uncompressTheWindow (FM85 * target, FM85 * source) {
//    Long k = (1LL << source->lgK);
//    U8 * window = (U8 *) malloc ((size_t) (k * sizeof(U8)));
//    assert (window != NULL);
//    // bzero ((void *) window, (size_t) k); // zeroing not needed here (unlike the Hybrid Flavor)
//    assert (target->slidingWindow == NULL);
//    target->slidingWindow = window;
//    Short pseudoPhase = determinePseudoPhase (source->lgK, source->numCoupons);
//    assert (source->compressedWindow != NULL);
//    lowLevelUncompressBytes (target->slidingWindow, k,
//           decodingTablesForHighEntropyByte[pseudoPhase],
//           source->compressedWindow,
//           source->cwLength);
//    return;
//  }

  /***************************************************************/
  /***************************************************************/

  //  void compressTheSurprisingValues (FM85 * target, FM85 * source, U32 * pairs, Long numPairs) {
  //    assert (numPairs > 0);
  //    target->numCompressedSurprisingValues = numPairs;
  //    Long k = (1LL << source->lgK);
  //    Long numBaseBits = golombChooseNumberOfBaseBits (k + numPairs, numPairs);
  //    Long pairBufLen = safeLengthForCompressedPairBuf (k, numPairs, numBaseBits);
  //    U32 * pairBuf = (U32 *) malloc ((size_t) (pairBufLen * sizeof(U32)));
  //    assert (pairBuf != NULL);
  //
  //    target->csvLength = lowLevelCompressPairs (pairs, numPairs, numBaseBits, pairBuf);
  //
  //    // At this point we free the unused portion of the compression output buffer.
  //    // Note: realloc caused strange timing spikes for lgK = 11 and 12.
  //
  //    U32 * shorterBuf = (U32 *) malloc (((size_t) target->csvLength) * sizeof(U32));
  //    if (shorterBuf == NULL) { FATAL_ERROR ("Out of Memory"); }
  //    memcpy ((void *) shorterBuf, (void *) pairBuf, ((size_t) target->csvLength) * sizeof(U32));
  //    free (pairBuf);
  //    target->compressedSurprisingValues = shorterBuf;
  //  }

  /***************************************************************/
  /***************************************************************/

  //allocates and returns an array of uncompressed pairs.
  //the length of this array is known to the source sketch.

//  U32 * uncompressTheSurprisingValues (FM85 * source) {
//   assert (source->isCompressed == 1);
//   Long k = (1LL << source->lgK);
//   Long numPairs = source->numCompressedSurprisingValues;
//   assert (numPairs > 0);
//   U32 * pairs = (U32 *) malloc ((size_t) numPairs * sizeof(U32));
//   assert (pairs != NULL);
//   Long numBaseBits = golombChooseNumberOfBaseBits (k + numPairs, numPairs);
//   lowLevelUncompressPairs(pairs, numPairs, numBaseBits,
//         source->compressedSurprisingValues, source->csvLength);
//   return (pairs);
//  }

  /***************************************************************/
  /***************************************************************/

//  void compressEmptyFlavor (FM85 * target, FM85 * source) {
//   return; // nothing to do, so just return
//  }

  /***************************************************************/
  /***************************************************************/

//  void uncompressEmptyFlavor (FM85 * target, FM85 * source) {
//   return; // nothing to do, so just return
//  }

  /***************************************************************/
  /***************************************************************/

//void compressSparseFlavor (FM85 * target, FM85 * source) {
// assert (source->slidingWindow == NULL); // there is no window to compress
// Long numPairs = 0;
// U32 * pairs = u32TableUnwrappingGetItems (source->surprisingValueTable, &numPairs);
// introspectiveInsertionSort(pairs, 0, numPairs-1);
// compressTheSurprisingValues (target, source, pairs, numPairs);
// free (pairs);
// return;
//}

  /***************************************************************/
  /***************************************************************/

//  void uncompressSparseFlavor (FM85 * target, FM85 * source) {
//   assert (source->compressedWindow == NULL);
//   assert (source->compressedSurprisingValues != NULL);
//   U32 * pairs = uncompressTheSurprisingValues (source);
//   Long numPairs = source->numCompressedSurprisingValues;
//   u32Table * table = makeU32TableFromPairsArray (pairs, numPairs, source->lgK);
//   target->surprisingValueTable = table;
//   free (pairs);
//   return;
//  }

  /***************************************************************/
  /***************************************************************/
  //The empty space that this leaves at the beginning of the output array
  //will be filled in later by the caller.

//  U32 * trickyGetPairsFromWindow (U8 * window, Long k, Long numPairsToGet, Long emptySpace) {
//   Long outputLength = emptySpace + numPairsToGet;
//   U32 * pairs = (U32 *) malloc ((size_t) (outputLength * sizeof(U32)));
//   assert (pairs != NULL);
//   Long rowIndex = 0;
//   Long pairIndex = emptySpace;
//   for (rowIndex = 0; rowIndex < k; rowIndex++) {
//     U8 byte = window[rowIndex];
//     while (byte != 0) {
//       Short colIndex = byteTrailingZerosTable[byte];
//       //      assert (colIndex < 8);
//       byte = byte ^ (1 << colIndex); // erase the 1
//       pairs[pairIndex++] = (U32) ((rowIndex << 6) | colIndex);
//     }
//   }
//   assert (pairIndex == outputLength);
//   return (pairs);
//  }


  /***************************************************************/
  /***************************************************************/

//This is complicated because it effectively builds a Sparse version
//of a Pinned sketch before compressing it. Hence the name Hybrid.

//  void compressHybridFlavor (FM85 * target, FM85 * source) {
//   //  Long i;
//   Long k = (1LL << source->lgK);
//   Long numPairsFromTable = 0;
//   U32 * pairsFromTable = u32TableUnwrappingGetItems (source->surprisingValueTable, &numPairsFromTable);
//   introspectiveInsertionSort(pairsFromTable, 0, numPairsFromTable-1);
//   assert (source->slidingWindow != NULL);
//   assert (source->windowOffset == 0);
//   Long numPairsFromArray = source->numCoupons - numPairsFromTable; // because the window offset is zero
//
//   U32 * allPairs
//       = trickyGetPairsFromWindow(source->slidingWindow, k, numPairsFromArray, numPairsFromTable);
//
//   u32Merge (pairsFromTable, 0, numPairsFromTable,
//       allPairs, numPairsFromTable, numPairsFromArray,
//       allPairs, 0);  // note the overlapping subarray trick
//
//   //  for (i = 0; i < source->numCoupons-1; i++) { assert (allPairs[i] < allPairs[i+1]); }
//
//   compressTheSurprisingValues (target, source, allPairs, source->numCoupons);
//   free (pairsFromTable);
//   free (allPairs);
//   return;
//  }

  /***************************************************************/
  /***************************************************************/

//  void uncompressHybridFlavor (FM85 * target, FM85 * source) {
//   assert (source->compressedWindow == NULL);
//   assert (source->compressedSurprisingValues != NULL);
//   U32 * pairs = uncompressTheSurprisingValues (source);
//   Long numPairs = source->numCompressedSurprisingValues;
//   // In the hybrid flavor, some of these pairs actually
//   // belong in the window, so we will separate them out,
//   // moving the "true" pairs to the bottom of the array.
//
//   Long k = (1LL << source->lgK);
//
//   U8 * window = (U8 *) malloc ((size_t) (k * sizeof(U8)));
//   assert (window != NULL);
//   bzero ((void *) window, (size_t) k); // important: zero the memory
//
//   Long nextTruePair = 0;
//   Long i;
//
//   for (i = 0; i < numPairs; i++) {
//     U32 rowCol = pairs[i];
//     assert (rowCol != ALL32BITS);
//     Short col = (Short) (rowCol & 63);
//     if (col < 8) {
//       Long  row = (Long) (rowCol >>> 6);
//       window[row] |= (1 << col); // set the window bit
//     }
//     else {
//       pairs[nextTruePair++] = rowCol; // move true pair down
//     }
//   }
//
//   assert (source->windowOffset == 0);
//   target->windowOffset = 0;
//
//   u32Table * table = makeU32TableFromPairsArray (pairs,
//              nextTruePair,
//              source->lgK);
//   target->surprisingValueTable = table;
//   target->slidingWindow = window;
//
//   free (pairs);
//
//   return;
//  }

  /***************************************************************/
  /***************************************************************/

//  void compressPinnedFlavor (FM85 * target, FM85 * source) {
//
//   compressTheWindow (target, source);
//
//   Long numPairs = source->surprisingValueTable->numItems;
//   //  if (numPairs == 0) {
//   //    fprintf (stderr,"A"); fflush (stderr);
//   //  }
//   if (numPairs > 0) {
//     Long chkNumPairs;
//     U32 * pairs = u32TableUnwrappingGetItems (source->surprisingValueTable, &chkNumPairs);
//     assert (chkNumPairs == numPairs);
//
//     // Here we subtract 8 from the column indices.  Because they are stored in the low 6 bits
//     // of each rowCol pair, and because no column index is less than 8 for a "Pinned" sketch,
//     // I believe we can simply subtract 8 from the pairs themselves.
//
//     Long i; // shift the columns over by 8 positions before compressing (because of the window)
//     for (i = 0; i < numPairs; i++) {
//       assert ((pairs[i] & 63) >= 8);
//       pairs[i] -= 8;
//     }
//
//     introspectiveInsertionSort(pairs, 0, numPairs-1);
//     compressTheSurprisingValues (target, source, pairs, numPairs);
//     free (pairs);
//   }
//   return;
//  }

  /***************************************************************/
  /***************************************************************/

//  void uncompressPinnedFlavor (FM85 * target, FM85 * source) {
//   assert (source->compressedWindow != NULL);
//   uncompressTheWindow (target, source);
//   Long numPairs = source->numCompressedSurprisingValues;
//   if (numPairs == 0) {
//     target->surprisingValueTable = u32TableMake (2, 6 + source->lgK);
//     //    fprintf (stderr,"B"); fflush (stderr);
//   }
//   else {
//     assert (numPairs > 0);
//     assert (source->compressedSurprisingValues != NULL);
//     U32 * pairs = uncompressTheSurprisingValues (source);
//     Long i; // undo the compressor's 8-column shift
//     for (i = 0; i < numPairs; i++) {
//       assert ((pairs[i] & 63) < 56);
//       pairs[i] += 8;
//     }
//     u32Table * table = makeU32TableFromPairsArray (pairs, numPairs, source->lgK);
//     target->surprisingValueTable = table;
//     free (pairs);
//   }
//   return;
//  }

  /***************************************************************/
  /***************************************************************/
  //Complicated by the existence of both a left fringe and a right fringe.

//  void compressSlidingFlavor (FM85 * target, FM85 * source) {
//
//   compressTheWindow (target, source);
//
//   Long numPairs = source->surprisingValueTable->numItems;
//   //  if (numPairs == 0) {
//   //    fprintf (stderr,"C"); fflush (stderr);
//   //  }
//
//   if (numPairs > 0) {
//     Long chkNumPairs;
//     U32 * pairs = u32TableUnwrappingGetItems (source->surprisingValueTable, &chkNumPairs);
//     assert (chkNumPairs == numPairs);
//
//     // Here we apply a complicated transformation to the column indices, which
//     // changes the implied ordering of the pairs, so we must do it before sorting.
//
//     Short pseudoPhase = determinePseudoPhase (source->lgK, source->numCoupons); // NB
//     assert (pseudoPhase < 16);
//     U8 * permutation = columnPermutationsForEncoding[pseudoPhase];
//
//     Short offset = source->windowOffset;
//     assert (offset > 0 && offset <= 56);
//
//     Long i;
//     for (i = 0; i < numPairs; i++) {
//       U32 rowCol = pairs[i];
//       Long  row = (Long)  (rowCol >>> 6);
//       Short col = (Short) (rowCol & 63);
//       // first rotate the columns into a canonical configuration: new = ((old - (offset+8)) + 64) mod 64
//       col = (col + 56 - offset) & 63;
//       assert (col >= 0 && col < 56);
//       // then apply the permutation
//       col = permutation[col];
//       pairs[i] = (U32) ((row << 6) | col);
//     }
//
//     introspectiveInsertionSort(pairs, 0, numPairs-1);
//     compressTheSurprisingValues (target, source, pairs, numPairs);
//     free (pairs);
//   }
//   return;
//  }

  /***************************************************************/
  /***************************************************************/

//  void uncompressSlidingFlavor (FM85 * target, FM85 * source) {
//   assert (source->compressedWindow != NULL);
//   uncompressTheWindow (target, source);
//
//   Long numPairs = source->numCompressedSurprisingValues;
//   if (numPairs == 0) {
//     target->surprisingValueTable = u32TableMake (2, 6 + source->lgK);
//     //    fprintf (stderr,"D"); fflush (stderr);
//   }
//   else {
//     assert (numPairs > 0);
//     assert (source->compressedSurprisingValues != NULL);
//     U32 * pairs = uncompressTheSurprisingValues (source);
//
//     Short pseudoPhase = determinePseudoPhase (source->lgK, source->numCoupons); // NB
//     assert (pseudoPhase < 16);
//     U8 * permutation = columnPermutationsForDecoding[pseudoPhase];
//
//     Short offset = source->windowOffset;
//     assert (offset > 0 && offset <= 56);
//
//     Long i;
//     for (i = 0; i < numPairs; i++) {
//       U32 rowCol = pairs[i];
//       Long  row = (Long)  (rowCol >>> 6);
//       Short col = (Short) (rowCol & 63);
//       // first undo the permutation
//       col = permutation[col];
//       // then undo the rotation: old = (new + (offset+8)) mod 64
//       col = (col + (offset+8)) & 63;
//       pairs[i] = (U32) ((row << 6) | col);
//     }
//
//     u32Table * table = makeU32TableFromPairsArray (pairs, numPairs, source->lgK);
//     target->surprisingValueTable = table;
//
//     free (pairs);
//   }
//   return;
//  }

  /***************************************************************/
  /***************************************************************/

  //Note: in the final system, compressed and uncompressed sketches will have different types

//  FM85 * fm85Compress (FM85 * source) {
//   assert (source->isCompressed == 0);
//
//   FM85 * target = (FM85 *) malloc (sizeof(FM85));
//   assert (target != NULL);
//
//   target->lgK = source->lgK;
//   target->numCoupons = source->numCoupons;
//   target->windowOffset = source->windowOffset;
//   target->firstInterestingColumn = source->firstInterestingColumn;
//   target->mergeFlag = source->mergeFlag;
//   target->kxp = source->kxp;
//   target->hipEstAccum = source->hipEstAccum;
//   target->hipErrAccum = source->hipErrAccum;
//
//   target->isCompressed = 1;
//
//   // initialize the variables that belong in a compressed sketch
//   target->numCompressedSurprisingValues = 0;
//   target->compressedSurprisingValues = (U32 *) NULL;
//   target->csvLength = 0;
//   target->compressedWindow = (U32 *) NULL;
//   target->cwLength = 0;
//
//   // clear the variables that don't belong in a compressed sketch
//   target->slidingWindow = NULL;
//   target->surprisingValueTable = NULL;
//
//   enum flavorType flavor = determineSketchFlavor(source);
//   switch (flavor) {
//   case EMPTY: compressEmptyFlavor  (target, source); break;
//   case SPARSE:
//     compressSparseFlavor (target, source);
//     assert (target->compressedWindow == NULL);
//     assert (target->compressedSurprisingValues != NULL);
//     break;
//   case HYBRID:
//     compressHybridFlavor (target, source);
//     assert (target->compressedWindow == NULL);
//     assert (target->compressedSurprisingValues != NULL);
//     break;
//   case PINNED:
//     compressPinnedFlavor (target, source);
//     assert (target->compressedWindow != NULL);
//     //    assert (target->compressedSurprisingValues != NULL);
//     break;
//   case SLIDING:
//     compressSlidingFlavor(target, source);
//     assert (target->compressedWindow != NULL);
//     //    assert (target->compressedSurprisingValues != NULL);
//     break;
//   default: FATAL_ERROR ("Unknown sketch flavor");
//   }
//
//   return target;
//  }

  /***************************************************************/
  /***************************************************************/

  //Note: in the final system, compressed and uncompressed sketches will have different types

//  FM85 * fm85Uncompress (FM85 * source) {
//   assert (source->isCompressed == 1);
//
//   FM85 * target = (FM85 *) malloc (sizeof(FM85));
//   assert (target != NULL);
//
//   target->lgK = source->lgK;
//   target->numCoupons = source->numCoupons;
//   target->windowOffset = source->windowOffset;
//   target->firstInterestingColumn = source->firstInterestingColumn;
//   target->mergeFlag = source->mergeFlag;
//   target->kxp = source->kxp;
//   target->hipEstAccum = source->hipEstAccum;
//   target->hipErrAccum = source->hipErrAccum;
//
//   target->isCompressed = 0;
//
//   // initialize the variables that belong in an updateable sketch
//   target->slidingWindow = (U8 *) NULL;
//   target->surprisingValueTable = (u32Table *) NULL;
//
//   // clear the variables that don't belong in an updateable sketch
//   target->numCompressedSurprisingValues = 0;
//   target->compressedSurprisingValues = (U32 *) NULL;
//   target->csvLength = 0;
//   target->compressedWindow = (U32 *) NULL;
//   target->cwLength = 0;
//
//   enum flavorType flavor = determineSketchFlavor(source);
//   switch (flavor) {
//   case EMPTY: uncompressEmptyFlavor  (target, source); break;
//   case SPARSE:
//     assert (source->compressedWindow == NULL);
//     uncompressSparseFlavor (target, source);
//     break;
//   case HYBRID:
//     uncompressHybridFlavor (target, source);
//     break;
//   case PINNED:
//     assert (source->compressedWindow != NULL);
//     uncompressPinnedFlavor (target, source);
//     break;
//   case SLIDING: uncompressSlidingFlavor(target, source); break;
//   default: FATAL_ERROR ("Unknown sketch flavor");
//   }
//
//   return target;
//  }

  static void println(String s) { System.out.println(s); }

}
