package org.apache.datasketches.common;

import org.apache.datasketches.memory.Memory;

/**
 * Methods for inquiring the status of a backing Memory object.
 */
public interface MemoryStatus {

  /**
   * Returns true if this object's internal data is backed by a Memory object,
   * which may be on-heap or off-heap.
   * @return true if this object's internal data is backed by a Memory object.
   */
  default boolean hasMemory() { return false; }

  /**
   * Returns true if this object's internal data is backed by direct (off-heap) Memory.
   * @return true if this object's internal data is backed by direct (off-heap) Memory.
   */
  default boolean isDirect() { return false; }

  /**
   * Returns true if the backing resource of <i>this</i> is identical with the backing resource
   * of <i>that</i>. The capacities must be the same.  If <i>this</i> is a region,
   * the region offset must also be the same.
   *
   * @param that A different non-null and alive Memory object.
   * @return true if the backing resource of <i>this</i> is identical with the backing resource
   * of <i>that</i>.
   * @throws SketchesArgumentException if <i>that</i> is not alive (already closed).
   */
  default boolean isSameResource(final Memory that) { return false; }

}
