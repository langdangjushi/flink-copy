package org.apache.flink.core.memory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** created by pengmingguo on 1/22/19 */
public abstract class MemorySegment {

  protected static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

  protected static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

  private static final boolean LITTLE_ENDIAN = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);

  protected final byte[] heapMemory;

  /** the address to the data, relative to the heap memory byte array */
  protected long address;

  protected final long addressLimit;

  protected final int size;

  private final Object owner;

  MemorySegment(byte[] buffer, Object owner) {
    if (buffer == null) {
      throw new NullPointerException("buffer");
    }
    this.heapMemory = buffer;
    this.address = BYTE_ARRAY_BASE_OFFSET;
    this.size = buffer.length;
    this.addressLimit = this.address + this.size;
    this.owner = owner;
  }

  MemorySegment(long offHeapAddress, int size, Object owner) {
    if (offHeapAddress < 0) {
      throw new IllegalArgumentException("negative pointer or size");
    }
    if (offHeapAddress >= Long.MAX_VALUE - Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "Segment initialized with too large address: "
              + offHeapAddress
              + " ; max allowed address is "
              + (Long.MAX_VALUE - Integer.MAX_VALUE - 1));
    }
    this.heapMemory = null;
    this.address = offHeapAddress;
    this.addressLimit = this.address + size;
    this.size = size;
    this.owner = owner;
  }

  public int size() {
    return size;
  }

  public boolean isFreed() {
    return address > addressLimit;
  }

  public void free() {
    address = addressLimit + 1;
  }

  public boolean isOffHeap() {
    return heapMemory == null;
  }

  public byte[] getArray() {
    if (heapMemory != null) {
      return heapMemory;
    } else {
      throw new IllegalStateException("Memory segment does not represent heap memory");
    }
  }

  public long getAddress() {
    if (heapMemory == null) {
      return address;
    } else {
      throw new IllegalStateException("Memory segment does not represent off heap memory");
    }
  }

  public abstract ByteBuffer wrap(int offset, int length);

  public Object getOwner() {
    return owner;
  }

  public abstract byte get(int index);

  public abstract void put(int index, byte b);

  public abstract void get(int index, byte[] dst);

  public abstract void put(int index, byte[] src);

  public abstract void get(int index, byte[] dst, int offset, int length);

  public abstract void put(int index, byte[] src, int offset, int length);

  public abstract boolean getBoolean(int index);

  public abstract void putBoolean(int index, boolean value);

  public final char getChar(int index) {
    long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      return UNSAFE.getChar(heapMemory, pos);
    } else if (address > addressLimit) {
      throw new IllegalStateException("This segment has been freed.");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  public final char getCharLittleEndian(int index) {
    if (LITTLE_ENDIAN) {
      return getChar(index);
    } else {
      return Character.reverseBytes(getChar(index));
    }
  }

  public final char getCharBigEndian(int index) {
    if (LITTLE_ENDIAN) {
      return Character.reverseBytes(getChar(index));
    } else {
      return getChar(index);
    }
  }

  public void putChar(int index, char value) {
    long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      UNSAFE.putChar(heapMemory, pos, value);
    } else if (address > addressLimit) {
      throw new IllegalStateException("segment has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  public final void putCharLittleEndian(int index, char value) {
    if (LITTLE_ENDIAN) {
      putChar(index, value);
    } else {
      putChar(index, Character.reverseBytes(value));
    }
  }

  public final void putCharBigEndian(int index, char value) {
    if (LITTLE_ENDIAN) {
      putChar(index, Character.reverseBytes(value));
    } else {
      putChar(index, value);
    }
  }

  public final short getShort(int index) {
    long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      return UNSAFE.getShort(heapMemory, pos);
    } else if (address > addressLimit) {
      throw new IllegalStateException("segment has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  public final short getShortLittleEndian(int index) {
    if (LITTLE_ENDIAN) {
      return getShort(index);
    } else {
      return Short.reverseBytes(getShort(index));
    }
  }

  public final short getShortBigEndian(int index) {
    if (LITTLE_ENDIAN) {
      return Short.reverseBytes(getShort(index));
    } else {
      return getShort(index);
    }
  }

  public final void putShort(int index, short value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      UNSAFE.putShort(heapMemory, pos, value);
    } else if (address > addressLimit) {
      throw new IllegalStateException("segment has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  public final void putShortLittleEndian(int index, short value) {
    if (LITTLE_ENDIAN) {
      putShort(index, value);
    } else {
      putShort(index, Short.reverseBytes(value));
    }
  }

  public final void putShortBigEndian(int index, short value) {
    if (LITTLE_ENDIAN) {
      putShort(index, Short.reverseBytes(value));
    } else {
      putShort(index, value);
    }
  }

  /**
   * Reads an int value (32bit, 4 bytes) from the given position, in the system's native byte order.
   * This method offers the best speed for integer reading and should be used unless a specific byte
   * order is required. In most cases, it suffices to know that the byte order in which the value is
   * written is the same as the one in which it is read (such as transient storage in memory, or
   * serialization for I/O and network), making this method the preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The int value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 4.
   */
  public final int getInt(int index) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 4) {
      return UNSAFE.getInt(heapMemory, pos);
    } else if (address > addressLimit) {
      throw new IllegalStateException("segment has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads an int value (32bit, 4 bytes) from the given position, in little-endian byte order. This
   * method's speed depends on the system's native byte order, and it is possibly slower than {@link
   * #getInt(int)}. For most cases (such as transient storage in memory or serialization for I/O and
   * network), it suffices to know that the byte order in which the value is written is the same as
   * the one in which it is read, and {@link #getInt(int)} is the preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The int value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 4.
   */
  public final int getIntLittleEndian(int index) {
    if (LITTLE_ENDIAN) {
      return getInt(index);
    } else {
      return Integer.reverseBytes(getInt(index));
    }
  }

  /**
   * Reads an int value (32bit, 4 bytes) from the given position, in big-endian byte order. This
   * method's speed depends on the system's native byte order, and it is possibly slower than {@link
   * #getInt(int)}. For most cases (such as transient storage in memory or serialization for I/O and
   * network), it suffices to know that the byte order in which the value is written is the same as
   * the one in which it is read, and {@link #getInt(int)} is the preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The int value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 4.
   */
  public final int getIntBigEndian(int index) {
    if (LITTLE_ENDIAN) {
      return Integer.reverseBytes(getInt(index));
    } else {
      return getInt(index);
    }
  }

  /**
   * Writes the given int value (32bit, 4 bytes) to the given position in the system's native byte
   * order. This method offers the best speed for integer writing and should be used unless a
   * specific byte order is required. In most cases, it suffices to know that the byte order in
   * which the value is written is the same as the one in which it is read (such as transient
   * storage in memory, or serialization for I/O and network), making this method the preferable
   * choice.
   *
   * @param index The position at which the value will be written.
   * @param value The int value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 4.
   */
  public final void putInt(int index, int value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 4) {
      UNSAFE.putInt(heapMemory, pos, value);
    } else if (address > addressLimit) {
      throw new IllegalStateException("segment has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes the given int value (32bit, 4 bytes) to the given position in little endian byte order.
   * This method's speed depends on the system's native byte order, and it is possibly slower than
   * {@link #putInt(int, int)}. For most cases (such as transient storage in memory or serialization
   * for I/O and network), it suffices to know that the byte order in which the value is written is
   * the same as the one in which it is read, and {@link #putInt(int, int)} is the preferable
   * choice.
   *
   * @param index The position at which the value will be written.
   * @param value The int value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 4.
   */
  public final void putIntLittleEndian(int index, int value) {
    if (LITTLE_ENDIAN) {
      putInt(index, value);
    } else {
      putInt(index, Integer.reverseBytes(value));
    }
  }

  /**
   * Writes the given int value (32bit, 4 bytes) to the given position in big endian byte order.
   * This method's speed depends on the system's native byte order, and it is possibly slower than
   * {@link #putInt(int, int)}. For most cases (such as transient storage in memory or serialization
   * for I/O and network), it suffices to know that the byte order in which the value is written is
   * the same as the one in which it is read, and {@link #putInt(int, int)} is the preferable
   * choice.
   *
   * @param index The position at which the value will be written.
   * @param value The int value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 4.
   */
  public final void putIntBigEndian(int index, int value) {
    if (LITTLE_ENDIAN) {
      putInt(index, Integer.reverseBytes(value));
    } else {
      putInt(index, value);
    }
  }

  /**
   * Reads a long value (64bit, 8 bytes) from the given position, in the system's native byte order.
   * This method offers the best speed for long integer reading and should be used unless a specific
   * byte order is required. In most cases, it suffices to know that the byte order in which the
   * value is written is the same as the one in which it is read (such as transient storage in
   * memory, or serialization for I/O and network), making this method the preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The long value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final long getLong(int index) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 8) {
      return UNSAFE.getLong(heapMemory, pos);
    } else if (address > addressLimit) {
      throw new IllegalStateException("segment has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads a long integer value (64bit, 8 bytes) from the given position, in little endian byte
   * order. This method's speed depends on the system's native byte order, and it is possibly slower
   * than {@link #getLong(int)}. For most cases (such as transient storage in memory or
   * serialization for I/O and network), it suffices to know that the byte order in which the value
   * is written is the same as the one in which it is read, and {@link #getLong(int)} is the
   * preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The long value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final long getLongLittleEndian(int index) {
    if (LITTLE_ENDIAN) {
      return getLong(index);
    } else {
      return Long.reverseBytes(getLong(index));
    }
  }

  /**
   * Reads a long integer value (64bit, 8 bytes) from the given position, in big endian byte order.
   * This method's speed depends on the system's native byte order, and it is possibly slower than
   * {@link #getLong(int)}. For most cases (such as transient storage in memory or serialization for
   * I/O and network), it suffices to know that the byte order in which the value is written is the
   * same as the one in which it is read, and {@link #getLong(int)} is the preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The long value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final long getLongBigEndian(int index) {
    if (LITTLE_ENDIAN) {
      return Long.reverseBytes(getLong(index));
    } else {
      return getLong(index);
    }
  }

  /**
   * Writes the given long value (64bit, 8 bytes) to the given position in the system's native byte
   * order. This method offers the best speed for long integer writing and should be used unless a
   * specific byte order is required. In most cases, it suffices to know that the byte order in
   * which the value is written is the same as the one in which it is read (such as transient
   * storage in memory, or serialization for I/O and network), making this method the preferable
   * choice.
   *
   * @param index The position at which the value will be written.
   * @param value The long value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final void putLong(int index, long value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 8) {
      UNSAFE.putLong(heapMemory, pos, value);
    } else if (address > addressLimit) {
      throw new IllegalStateException("segment has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes the given long value (64bit, 8 bytes) to the given position in little endian byte order.
   * This method's speed depends on the system's native byte order, and it is possibly slower than
   * {@link #putLong(int, long)}. For most cases (such as transient storage in memory or
   * serialization for I/O and network), it suffices to know that the byte order in which the value
   * is written is the same as the one in which it is read, and {@link #putLong(int, long)} is the
   * preferable choice.
   *
   * @param index The position at which the value will be written.
   * @param value The long value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final void putLongLittleEndian(int index, long value) {
    if (LITTLE_ENDIAN) {
      putLong(index, value);
    } else {
      putLong(index, Long.reverseBytes(value));
    }
  }

  /**
   * Writes the given long value (64bit, 8 bytes) to the given position in big endian byte order.
   * This method's speed depends on the system's native byte order, and it is possibly slower than
   * {@link #putLong(int, long)}. For most cases (such as transient storage in memory or
   * serialization for I/O and network), it suffices to know that the byte order in which the value
   * is written is the same as the one in which it is read, and {@link #putLong(int, long)} is the
   * preferable choice.
   *
   * @param index The position at which the value will be written.
   * @param value The long value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final void putLongBigEndian(int index, long value) {
    if (LITTLE_ENDIAN) {
      putLong(index, Long.reverseBytes(value));
    } else {
      putLong(index, value);
    }
  }

  /**
   * Reads a single-precision floating point value (32bit, 4 bytes) from the given position, in the
   * system's native byte order. This method offers the best speed for float reading and should be
   * used unless a specific byte order is required. In most cases, it suffices to know that the byte
   * order in which the value is written is the same as the one in which it is read (such as
   * transient storage in memory, or serialization for I/O and network), making this method the
   * preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The float value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 4.
   */
  public final float getFloat(int index) {
    return Float.intBitsToFloat(getInt(index));
  }

  /**
   * Reads a single-precision floating point value (32bit, 4 bytes) from the given position, in
   * little endian byte order. This method's speed depends on the system's native byte order, and it
   * is possibly slower than {@link #getFloat(int)}. For most cases (such as transient storage in
   * memory or serialization for I/O and network), it suffices to know that the byte order in which
   * the value is written is the same as the one in which it is read, and {@link #getFloat(int)} is
   * the preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The long value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final float getFloatLittleEndian(int index) {
    return Float.intBitsToFloat(getIntLittleEndian(index));
  }

  /**
   * Reads a single-precision floating point value (32bit, 4 bytes) from the given position, in big
   * endian byte order. This method's speed depends on the system's native byte order, and it is
   * possibly slower than {@link #getFloat(int)}. For most cases (such as transient storage in
   * memory or serialization for I/O and network), it suffices to know that the byte order in which
   * the value is written is the same as the one in which it is read, and {@link #getFloat(int)} is
   * the preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The long value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final float getFloatBigEndian(int index) {
    return Float.intBitsToFloat(getIntBigEndian(index));
  }

  /**
   * Writes the given single-precision float value (32bit, 4 bytes) to the given position in the
   * system's native byte order. This method offers the best speed for float writing and should be
   * used unless a specific byte order is required. In most cases, it suffices to know that the byte
   * order in which the value is written is the same as the one in which it is read (such as
   * transient storage in memory, or serialization for I/O and network), making this method the
   * preferable choice.
   *
   * @param index The position at which the value will be written.
   * @param value The float value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 4.
   */
  public final void putFloat(int index, float value) {
    putInt(index, Float.floatToRawIntBits(value));
  }

  /**
   * Writes the given single-precision float value (32bit, 4 bytes) to the given position in little
   * endian byte order. This method's speed depends on the system's native byte order, and it is
   * possibly slower than {@link #putFloat(int, float)}. For most cases (such as transient storage
   * in memory or serialization for I/O and network), it suffices to know that the byte order in
   * which the value is written is the same as the one in which it is read, and {@link
   * #putFloat(int, float)} is the preferable choice.
   *
   * @param index The position at which the value will be written.
   * @param value The long value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final void putFloatLittleEndian(int index, float value) {
    putIntLittleEndian(index, Float.floatToRawIntBits(value));
  }

  /**
   * Writes the given single-precision float value (32bit, 4 bytes) to the given position in big
   * endian byte order. This method's speed depends on the system's native byte order, and it is
   * possibly slower than {@link #putFloat(int, float)}. For most cases (such as transient storage
   * in memory or serialization for I/O and network), it suffices to know that the byte order in
   * which the value is written is the same as the one in which it is read, and {@link
   * #putFloat(int, float)} is the preferable choice.
   *
   * @param index The position at which the value will be written.
   * @param value The long value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final void putFloatBigEndian(int index, float value) {
    putIntBigEndian(index, Float.floatToRawIntBits(value));
  }

  /**
   * Reads a double-precision floating point value (64bit, 8 bytes) from the given position, in the
   * system's native byte order. This method offers the best speed for double reading and should be
   * used unless a specific byte order is required. In most cases, it suffices to know that the byte
   * order in which the value is written is the same as the one in which it is read (such as
   * transient storage in memory, or serialization for I/O and network), making this method the
   * preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The double value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final double getDouble(int index) {
    return Double.longBitsToDouble(getLong(index));
  }

  /**
   * Reads a double-precision floating point value (64bit, 8 bytes) from the given position, in
   * little endian byte order. This method's speed depends on the system's native byte order, and it
   * is possibly slower than {@link #getDouble(int)}. For most cases (such as transient storage in
   * memory or serialization for I/O and network), it suffices to know that the byte order in which
   * the value is written is the same as the one in which it is read, and {@link #getDouble(int)} is
   * the preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The long value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final double getDoubleLittleEndian(int index) {
    return Double.longBitsToDouble(getLongLittleEndian(index));
  }

  /**
   * Reads a double-precision floating point value (64bit, 8 bytes) from the given position, in big
   * endian byte order. This method's speed depends on the system's native byte order, and it is
   * possibly slower than {@link #getDouble(int)}. For most cases (such as transient storage in
   * memory or serialization for I/O and network), it suffices to know that the byte order in which
   * the value is written is the same as the one in which it is read, and {@link #getDouble(int)} is
   * the preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The long value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final double getDoubleBigEndian(int index) {
    return Double.longBitsToDouble(getLongBigEndian(index));
  }

  /**
   * Writes the given double-precision floating-point value (64bit, 8 bytes) to the given position
   * in the system's native byte order. This method offers the best speed for double writing and
   * should be used unless a specific byte order is required. In most cases, it suffices to know
   * that the byte order in which the value is written is the same as the one in which it is read
   * (such as transient storage in memory, or serialization for I/O and network), making this method
   * the preferable choice.
   *
   * @param index The position at which the memory will be written.
   * @param value The double value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final void putDouble(int index, double value) {
    putLong(index, Double.doubleToRawLongBits(value));
  }

  /**
   * Writes the given double-precision floating-point value (64bit, 8 bytes) to the given position
   * in little endian byte order. This method's speed depends on the system's native byte order, and
   * it is possibly slower than {@link #putDouble(int, double)}. For most cases (such as transient
   * storage in memory or serialization for I/O and network), it suffices to know that the byte
   * order in which the value is written is the same as the one in which it is read, and {@link
   * #putDouble(int, double)} is the preferable choice.
   *
   * @param index The position at which the value will be written.
   * @param value The long value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final void putDoubleLittleEndian(int index, double value) {
    putLongLittleEndian(index, Double.doubleToRawLongBits(value));
  }

  /**
   * Writes the given double-precision floating-point value (64bit, 8 bytes) to the given position
   * in big endian byte order. This method's speed depends on the system's native byte order, and it
   * is possibly slower than {@link #putDouble(int, double)}. For most cases (such as transient
   * storage in memory or serialization for I/O and network), it suffices to know that the byte
   * order in which the value is written is the same as the one in which it is read, and {@link
   * #putDouble(int, double)} is the preferable choice.
   *
   * @param index The position at which the value will be written.
   * @param value The long value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the segment
   *     size minus 8.
   */
  public final void putDoubleBigEndian(int index, double value) {
    putLongBigEndian(index, Double.doubleToRawLongBits(value));
  }

  public abstract void get(DataOutput out, int offset, int length) throws IOException;

  public abstract void put(DataInput in, int offset, int length) throws IOException;

  public abstract void get(int offset, ByteBuffer target, int numBytes);

  public abstract void put(int offset, ByteBuffer source, int numBytes);

  public final void copyTo(int offset, MemorySegment target, int targetOffset, int numBytes) {
    byte[] thisHeapRef = this.heapMemory;
    byte[] otherHeapRef = target.heapMemory;
    long thisPointer = this.address + offset;
    long otherPointer = target.address + targetOffset;

    if ((numBytes | offset | targetOffset) >= 0
        && thisPointer <= this.addressLimit - numBytes
        && otherPointer <= target.addressLimit - numBytes) {
      UNSAFE.copyMemory(thisHeapRef, thisPointer, otherHeapRef, otherPointer, numBytes);
    } else if (this.address > this.addressLimit) {
      throw new IllegalStateException("This memory segment has been freed.");
    } else if (target.address > target.addressLimit) {
      throw new IllegalStateException("target memory segment has been freed.");
    } else {
      throw new IndexOutOfBoundsException(
          String.format(
              "offset=%d, targetOffset=%d, numBytes=%d, address=%d, targetAddress=%d",
              offset, targetOffset, numBytes, this.address, target.address));
    }
  }

  public final int compare(MemorySegment seg2, int offset1, int offset2, int len) {
    while (len >= 8) {
      long l1 = this.getLongBigEndian(offset1);
      long l2 = seg2.getLongBigEndian(offset2);

      if (l1 != l2) {
        // TODO
        return (l1 < l2) ^ (l1 < 0) ^ (l2 < 0) ? -1 : 1;
      }
      offset1 += 8;
      offset2 += 8;
      len -= 8;
    }
    while (len > 0) {
      // TODO
      int b1 = this.get(offset1) & 0xff;
      int b2 = seg2.get(offset2) & 0xff;
      int cmp = b1 - b2;
      if (cmp != 0) {
        return cmp;
      }
      offset1++;
      offset2++;
      len--;
    }
    return 0;
  }

  public final void swapBytes(
      byte[] tempBuffer, MemorySegment seg2, int offset1, int offset2, int len) {
    if ((offset1 | offset2 | len | (tempBuffer.length - len)) >= 0) {
      long thisPos = this.address + offset1;
      long otherPos = seg2.address + offset2;

      if (thisPos <= this.addressLimit - len && otherPos <= seg2.addressLimit - len) {
        UNSAFE.copyMemory(this.heapMemory, thisPos, tempBuffer, BYTE_ARRAY_BASE_OFFSET, len);

        UNSAFE.copyMemory(seg2.heapMemory, otherPos, this.heapMemory, thisPos, len);

        UNSAFE.copyMemory(tempBuffer, BYTE_ARRAY_BASE_OFFSET, seg2.heapMemory, otherPos, len);
        return;
      } else if (this.address > this.addressLimit) {
        throw new IllegalStateException("this memory segment has been freed.");
      } else if (seg2.address > seg2.addressLimit) {
        throw new IllegalStateException("other memory segment has been freed.");
      }
      // index is in fact invalid
      throw new IndexOutOfBoundsException(
          String.format(
              "offset1=%d, offset2=%d, len=%d, bufferSize=%d, address1=%d, address2=%d",
              offset1, offset2, len, tempBuffer.length, this.address, seg2.address));
    }
  }
}
