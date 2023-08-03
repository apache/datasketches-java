package org.apache.datasketches.kll;

import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryN;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;

import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class implements an off-heap, read-only KllItemsSketch using WritableMemory.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
public class KllDirectCompactItemsSketch<T> extends KllItemsSketch<T> {
  private Memory mem;

  /**
   * Internal implementation of the wrapped Memory KllSketch.
   * @param memVal the MemoryValadate object
   * @param comparator to compare items
   * @param serDe Serializer / deserializer for items of type <i>T</i> and <i>T[]</i>.
   */
  KllDirectCompactItemsSketch( //called below and KllItemsSketch
      final KllMemoryValidate memVal,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    super(memVal.sketchStructure, comparator, serDe);
    this.mem = memVal.srcMem;
    readOnly = true;
    levelsArr = memVal.levelsArr; //always converted to writable form.
  }

  @Override
  public int getK() {
    return getMemoryK(mem);
  }

  @Override
  public T getMaxItem() {
    if (sketchStructure == COMPACT_EMPTY || getN() == 0) { return null; }
    if (sketchStructure == COMPACT_SINGLE) {
      return serDe.deserializeFromMemory(mem, DATA_START_ADR_SINGLE_ITEM, 1)[0];
    }
    //sketchStructure == COMPACT_FULL
    final int offset = DATA_START_ADR + getNumLevels() * Integer.BYTES;
    return serDe.deserializeFromMemory(mem, offset, 2)[1];
  }

  @Override
  public T getMinItem() {
    if (sketchStructure == COMPACT_EMPTY || getN() == 0) { return null; }
    if (sketchStructure == COMPACT_SINGLE) {
      return serDe.deserializeFromMemory(mem, DATA_START_ADR_SINGLE_ITEM, 1)[0];
    }
    //sketchStructure == COMPACT_FULL
    final int offset = DATA_START_ADR + getNumLevels() * Integer.BYTES;
    return serDe.deserializeFromMemory(mem, offset, 1)[0];
  }

  @Override
  public long getN() {
    if (sketchStructure == COMPACT_EMPTY) { return 0; }
    if (sketchStructure == COMPACT_SINGLE) { return 1; }
    return getMemoryN(mem);
  }

  //restricted

  private int getCompactDataOffset() { //Sketch cannot be empty
    return sketchStructure == COMPACT_SINGLE
        ? DATA_START_ADR_SINGLE_ITEM
        : DATA_START_ADR + getNumLevels() * Integer.BYTES + getMinMaxSizeBytes();
  }

  @Override
  int getM() {
    return getMemoryM(mem);
  }

  @Override
  MemoryRequestServer getMemoryRequestServer() {
    return null; // not used. Must return null.
  }

  @Override
  int getMinK() {
    if (sketchStructure == COMPACT_EMPTY || sketchStructure == COMPACT_SINGLE) { return getMemoryK(mem); }
    return getMemoryMinK(mem);
  }

  @Override
  byte[] getMinMaxByteArr() {
    if (sketchStructure == COMPACT_EMPTY || getN() == 0) { return new byte[0]; }
    if (sketchStructure == COMPACT_SINGLE) {
      final int bytesSingle = serDe.sizeOf(mem, DATA_START_ADR_SINGLE_ITEM, 1);
      final byte[] byteArr = new byte[2 * bytesSingle];
      mem.getByteArray(DATA_START_ADR_SINGLE_ITEM, byteArr, 0, bytesSingle);
      mem.getByteArray(DATA_START_ADR_SINGLE_ITEM, byteArr, bytesSingle, bytesSingle);
    }
    //sketchStructure == COMPACT_FULL
    final int offset = DATA_START_ADR + getNumLevels() * Integer.BYTES;
    final int bytesMinMax = serDe.sizeOf(mem, offset, 2);
    final byte[] byteArr = new byte[bytesMinMax];
    mem.getByteArray(offset, byteArr, 0, bytesMinMax);
    return byteArr;
  }

  @Override
  int getMinMaxSizeBytes() {
    if (sketchStructure == COMPACT_EMPTY || getN() == 0) { return 0; }
    if (sketchStructure == COMPACT_SINGLE) {
      return serDe.sizeOf(mem, DATA_START_ADR_SINGLE_ITEM, 1) * 2;
    }
    //sketchStructure == COMPACT_FULL
    final int offset = DATA_START_ADR + getNumLevels() * Integer.BYTES;
    return serDe.sizeOf(mem, offset, 2);
  }

  @Override
  byte[] getRetainedItemsByteArr() {
    if (sketchStructure == COMPACT_EMPTY || getN() == 0) { return new byte[0]; }
    final int offset = getCompactDataOffset(); //both single & full
    final int bytes = serDe.sizeOf(mem, offset, getNumRetained());
    final byte[] byteArr = new byte[bytes];
    mem.getByteArray(offset, byteArr, 0, bytes);
    return byteArr;
  }

  @Override
  int getRetainedItemsSizeBytes() {
    if (sketchStructure == COMPACT_EMPTY || getN() == 0) { return 0; }
    final int offset = getCompactDataOffset(); //both single & full
    return serDe.sizeOf(mem, offset, getNumRetained());
  }

  @Override
  Object getSingleItem() {
    if (getN() != 1) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    final int offset = getCompactDataOffset(); //both single & full
    return serDe.deserializeFromMemory(mem, offset, 1)[0];
  }

  @Override
  byte[] getSingleItemByteArr() {
    if (getN() != 1) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    final int offset = getCompactDataOffset(); //both single & full
    final int bytes = serDe.sizeOf(mem, offset, 1);
    final byte[] byteArr = new byte[bytes];
    mem.getByteArray(offset, byteArr, 0, bytes);
    return byteArr;
  }

  @Override
  int getSingleItemSizeBytes() {
    if (getN() != 1) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    final int offset = getCompactDataOffset(); //both single & full
    final int bytes = serDe.sizeOf(mem, offset, 1);
    return bytes;
  }

  @Override
  Object[] getTotalItemsArray() {
    if (getN() == 0) { return new Object[0]; }
    if (getN() == 1) { return new Object[] { getSingleItem() }; }
    final int offset = getCompactDataOffset();
    final int numRetItems = getNumRetained();
    final int numCapItems = levelsArr[getNumLevels()];
    final Object[] retItems = serDe.deserializeFromMemory(mem, offset, numRetItems);
    final Object[] capItems = new Object[numCapItems];
    System.arraycopy(retItems, 0, capItems, levelsArr[0], numRetItems);
    return capItems;
  }

  @Override
  byte[] getTotalItemsByteArr() {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  WritableMemory getWritableMemory() {
    return (WritableMemory)mem;
  }

  @Override
  void incN() {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void incNumLevels() {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  boolean isLevelZeroSorted() {
    return getMemoryLevelZeroSortedFlag(mem);
  }

  @Override
  void setItemsArray(final Object[] ItemsArr) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void setItemsArrayAt(final int index, final Object item) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void setMaxItem(final Object item) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void setMinItem(final Object item) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void setMinK(final int minK) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void setN(final long n) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void setNumLevels(final int numLevels) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

  @Override
  void setWritablMemory(final WritableMemory wmem) {
    throw new SketchesArgumentException(UNSUPPORTED_MSG);
  }

}
