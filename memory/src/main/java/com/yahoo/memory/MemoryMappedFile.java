/*
 * Copyright 2015, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE
 * file at the project root for terms.
 */

package com.yahoo.memory;

import static com.yahoo.memory.UnsafeUtil.unsafe;

import java.io.File;
import java.io.FileDescriptor;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import sun.misc.Cleaner;
import sun.nio.ch.FileChannelImpl;

/**
 * MemoryMappedFile class extends NativeMemory and is used to memory map files (including those &gt;
 * 2GB) off heap.
 *
 * <p>This class leverages the JVM Cleaner class that replaces {@link java.lang.Object#finalize()}
 * and serves as a back-up if the calling class does not call {@link #freeMemory()}.</p>
 *
 * @author Praveenkumar Venkatesan
 */
//@SuppressWarnings("restriction")
public class MemoryMappedFile extends NativeMemory {

  private FileChannel fileChannel_ = null;
  private RandomAccessFile randomAccessFile_ = null;
  private MappedByteBuffer dummyMbbInstance_ = null;
  private final Cleaner cleaner;

  /**
   * Constructor for memory mapping a file.
   *
   * <p>
   * Memory maps a file directly in off heap leveraging native map0 method used in
   * FileChannelImpl.c. The owner will have read write access to that address space.
   * </p>
   *
   * @param file - File to be mapped
   * @param position - memory map starting from this position in the file
   * @param len - Memory map at most len bytes &gt; 0 starting from {@code position}
   *
   * @throws Exception file not found or RuntimeException, etc.
   */
  public MemoryMappedFile(final File file, final long position, final long len) throws Exception {
    super(0L, null, null);

    if (position < 0L) {
      throw new IllegalArgumentException("Negative position");
    }
    if (len < 0L) {
      throw new IllegalArgumentException("Negative size");
    }
    if (position + len < 0) {
      throw new IllegalArgumentException("Position + size overflow");
    }

    this.randomAccessFile_ = new RandomAccessFile(file, "rw");
    this.fileChannel_ = randomAccessFile_.getChannel();
    super.nativeRawStartAddress_ = map(position, len);
    super.capacityBytes_ = len;
    super.memReq_ = null;

    // len can be more than the file.length
    randomAccessFile_.setLength(len);
    createDummyMbbInstance();

    this.cleaner = Cleaner.create(this,
        new Deallocator(randomAccessFile_, nativeRawStartAddress_, capacityBytes_));
  }

  /**
   * Loads content into physical memory. This method makes a best effort to ensure that, when it
   * returns, this buffer's content is resident in physical memory. Invoking this method may cause
   * some number of page faults and I/O operations to occur.
   *
   * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/nio/MappedByteBuffer.html#load--">
   * java/nio/MappedByteBuffer.load</a>
   */
  public void load() {
    madvise();

    // Read a byte from each page to bring it into memory.
    final int ps = unsafe.pageSize();
    final int count = pageCount(ps, capacityBytes_);
    long a = nativeRawStartAddress_;
    for (int i = 0; i < count; i++) {
      unsafe.getByte(a);
      a += ps;
    }
  }

  /**
   * Tells whether or not the content is resident in physical memory. A return value of true implies
   * that it is highly likely that all of the data in this buffer is resident in physical memory and
   * may therefore be accessed without incurring any virtual-memory page faults or I/O operations. A
   * return value of false does not necessarily imply that the content is not resident in physical
   * memory. The returned value is a hint, rather than a guarantee, because the underlying operating
   * system may have paged out some of the buffer's data by the time that an invocation of this
   * method returns.
   *
   * @return true if loaded
   *
   * @see <a href=
   * "https://docs.oracle.com/javase/8/docs/api/java/nio/MappedByteBuffer.html#isLoaded--"> java
   * /nio/MappedByteBuffer.isLoaded</a>
   */
  public boolean isLoaded() {
    try {
      final int ps = unsafe.pageSize();
      final int pageCount = pageCount(ps, capacityBytes_);
      final Method method =
          MappedByteBuffer.class.getDeclaredMethod("isLoaded0", long.class, long.class, int.class);
      method.setAccessible(true);
      return (boolean) method.invoke(dummyMbbInstance_, nativeRawStartAddress_, capacityBytes_,
          pageCount);
    } catch (final Exception e) {
      throw new RuntimeException(
          String.format("Encountered %s exception while loading", e.getClass()));
    }
  }

  /**
   * Forces any changes made to this content to be written to the storage device containing the
   * mapped file.
   *
   * <p>
   * If the file mapped into this buffer resides on a local storage device then when this method
   * returns it is guaranteed that all changes made to the buffer since it was created, or since
   * this method was last invoked, will have been written to that device.
   * </p>
   *
   * <p>
   * If the file does not reside on a local device then no such guarantee is made.
   * </p>
   *
   * <p>
   * If this buffer was not mapped in read/write mode
   * (java.nio.channels.FileChannel.MapMode.READ_WRITE) then invoking this method has no effect.
   * </p>
   *
   * @see <a href=
   * "https://docs.oracle.com/javase/8/docs/api/java/nio/MappedByteBuffer.html#force--"> java/
   * nio/MappedByteBuffer.force</a>
   */
  public void force() {
    try {
      final Method method = MappedByteBuffer.class.getDeclaredMethod("force0", FileDescriptor.class,
          long.class, long.class);
      method.setAccessible(true);
      method.invoke(dummyMbbInstance_, randomAccessFile_.getFD(), nativeRawStartAddress_,
          capacityBytes_);
    } catch (final Exception e) {
      throw new RuntimeException(String.format("Encountered %s exception in force", e.getClass()));
    }
  }

  @Override
  public void freeMemory() {
    super.capacityBytes_ = 0L;
    super.memReq_ = null;
    cleaner.clean();
    nativeRawStartAddress_ = 0L;
  }

  // Restricted methods

  static final int pageCount(final int ps, final long length) {
    return (int) ( (length == 0) ? 0 : (length - 1L) / ps + 1L);
  }

  private void createDummyMbbInstance() throws RuntimeException {
    try {
      final Class<?> cl = Class.forName("java.nio.DirectByteBuffer");
      final Constructor<?> ctor =
          cl.getDeclaredConstructor(int.class, long.class, FileDescriptor.class, Runnable.class);
      ctor.setAccessible(true);
      dummyMbbInstance_ = (MappedByteBuffer) ctor.newInstance(0, // some junk capacity
          nativeRawStartAddress_, null, null);
    } catch (final Exception e) {
      throw new RuntimeException(
          "Could not create Dummy MappedByteBuffer instance: " + e.getClass());
    }
  }

  /**
   * madvise is a system call made by load0 native method
   */
  private void madvise() throws RuntimeException {
    try {
      final Method method = MappedByteBuffer.class.getDeclaredMethod("load0", long.class, long.class);
      method.setAccessible(true);
      method.invoke(dummyMbbInstance_, nativeRawStartAddress_, capacityBytes_);
    } catch (final Exception e) {
      throw new RuntimeException(
          String.format("Encountered %s exception while loading", e.getClass()));
    }
  }

  /**
   * Creates a mapping of the file on disk starting at position and of size length to pages in OS.
   * May throw OutOfMemory error if you have exhausted memory. Force garbage collection and
   * re-attempt.
   */
  private long map(final long position, final long len) throws RuntimeException {
    final int pagePosition = (int) (position % unsafe.pageSize());
    final long mapPosition = position - pagePosition;
    final long mapSize = len + pagePosition;

    try {
      final Method method =
          FileChannelImpl.class.getDeclaredMethod("map0", int.class, long.class, long.class);
      method.setAccessible(true);
      final long addr = (long) method.invoke(fileChannel_, 1, mapPosition, mapSize);
      return addr;
    } catch (final Exception e) {
      throw new RuntimeException(
          String.format("Encountered %s exception while mapping", e.getClass()));
    }
  }

  private static final class Deallocator implements Runnable {
    private RandomAccessFile randomAccessFile_;
    private FileChannel fileChannel_;
    private long nativeRawStartAddress_;
    private long capacityBytes_;

    private Deallocator(final RandomAccessFile randomAccessFile,
        final long nativeRawStartAddress, final long capacityBytes) {
      assert (randomAccessFile != null);
      assert (nativeRawStartAddress != 0);
      assert (capacityBytes != 0);
      this.randomAccessFile_ = randomAccessFile;
      this.fileChannel_ = randomAccessFile.getChannel();
      this.nativeRawStartAddress_ = nativeRawStartAddress;
      this.capacityBytes_ = capacityBytes;
    }

    /**
     * Removes existing mapping
     */
    private void unmap() throws RuntimeException {
      try {
        final Method method = FileChannelImpl.class.getDeclaredMethod("unmap0", long.class, long.class);
        method.setAccessible(true);
        method.invoke(fileChannel_, nativeRawStartAddress_, capacityBytes_);
        randomAccessFile_.close();
      } catch (final Exception e) {
        throw new RuntimeException(
            String.format("Encountered %s exception while freeing memory", e.getClass()));
      }
    }

    @Override
    public void run() {
      if (fileChannel_ != null) {
        unmap();
      }

      nativeRawStartAddress_ = 0L;
    }
  } //End of class Deallocator

}
