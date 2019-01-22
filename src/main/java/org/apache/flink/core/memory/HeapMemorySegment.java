package org.apache.flink.core.memory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/** created by pengmingguo on 1/22/19 */
public class HeapMemorySegment extends MemorySegment {

  private byte[] memory;

  HeapMemorySegment(byte[] memory) {
    this(memory, null);
  }

  HeapMemorySegment(byte[] memory, Object owner) {
    super(Objects.requireNonNull(memory), owner);
    this.memory = memory;
  }

  @Override
  public void free() {
    super.free();
    this.memory = null;
  }

  @Override
  public ByteBuffer wrap(int offset, int length) {
    try {
      return ByteBuffer.wrap(this.memory, offset, length);
    } catch (NullPointerException e) {
      throw new IllegalStateException("segment has been freed");
    }
  }

  @Override
  public byte[] getArray() {
    return this.heapMemory;
  }

  @Override
  public byte get(int index) {
    return this.memory[index];
  }

  @Override
  public void put(int index, byte b) {
    this.memory[index] = b;
  }

  @Override
  public void get(int index, byte[] dst) {
    get(index, dst, 0, dst.length);
  }

  @Override
  public void put(int index, byte[] src) {
    put(index, src, 0, src.length);
  }

  @Override
  public void get(int index, byte[] dst, int offset, int length) {
    System.arraycopy(this.memory, index, dst, offset, length);
  }

  @Override
  public void put(int index, byte[] src, int offset, int length) {
    System.arraycopy(src, offset, this.memory, index, length);
  }

  @Override
  public boolean getBoolean(int index) {
    return this.memory[index] != 0;
  }

  @Override
  public void putBoolean(int index, boolean value) {
    this.memory[index] = (byte) (value ? 1 : 0);
  }

  @Override
  public void get(DataOutput out, int offset, int length) throws IOException {
    out.write(this.memory, offset, length);
  }

  @Override
  public void put(DataInput in, int offset, int length) throws IOException {
    in.readFully(this.memory, offset, length);
  }

  @Override
  public void get(int offset, ByteBuffer target, int numBytes) {
    target.put(this.memory, offset, numBytes);
  }

  @Override
  public void put(int offset, ByteBuffer source, int numBytes) {
    source.get(this.memory, offset, numBytes);
  }

  public static final class HeapMemorySegmentFactory {
    public HeapMemorySegment wrap(byte[] memory) {
      return new HeapMemorySegment(memory);
    }

    public HeapMemorySegment allocateUnpooledSegment(int size, Object owner) {
      return new HeapMemorySegment(new byte[size], owner);
    }

    public HeapMemorySegment wrapPooledHeapMemory(byte[] memory, Object owner) {
      return new HeapMemorySegment(memory, owner);
    }

    HeapMemorySegmentFactory() {}
  }

  public static final HeapMemorySegmentFactory FACTORY = new HeapMemorySegmentFactory();
}
