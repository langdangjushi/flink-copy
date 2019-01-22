package org.apache.flink.core.memory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import javax.swing.plaf.basic.BasicButtonUI;

/**
 * created by pengmingguo on 1/22/19
 */
public class HybridMemorySegment extends MemorySegment {

  private final ByteBuffer offHeapBuffer;

  HybridMemorySegment(ByteBuffer buffer) {
    this(buffer, null);
  }

  HybridMemorySegment(ByteBuffer buffer, Object owner) {
    super(checkBufferAndGetAddress(buffer), buffer.capacity(), owner);
    this.offHeapBuffer = buffer;
  }

  HybridMemorySegment(byte[] buffer) {
    this(buffer, null);
  }

  HybridMemorySegment(byte[] buffer, Object owner){
    super(buffer, owner);
    this.offHeapBuffer = null;
}

public ByteBuffer getOffHeapBuffer() {
    if (offHeapBuffer != null) {
      return offHeapBuffer;
    } else {
      throw new IllegalStateException("memeory does not represent off heap memory");
    }
}

  @Override
  public ByteBuffer wrap(int offset, int length) {
    if (address <= addressLimit) {
      if (heapMemory != null) {
        return ByteBuffer.wrap(heapMemory,offset,length);
      }else {
        try {
          ByteBuffer wrapper = offHeapBuffer.duplicate();
          wrapper.limit(offset + length);
          wrapper.position(offset);
          return wrapper;
        } catch (IllegalArgumentException e) {
          throw new IndexOutOfBoundsException();
        }
      }
    } else {
      throw new IllegalStateException("segment has been freed");
    }
  }

  @Override
  public byte get(int index) {
    long pos = address + index;
    if (index >= 0 && pos < addressLimit) {
      return UNSAFE.getByte(heapMemory,pos);
    }
    else if (address > addressLimit) {
      throw new IllegalStateException("segment has been freed");
    }
    else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  @Override
  public void put(int index, byte b) {

  }

  @Override
  public void get(int index, byte[] dst) {

  }

  @Override
  public void put(int index, byte[] src) {

  }

  @Override
  public void get(int index, byte[] dst, int offset, int length) {

  }

  @Override
  public void put(int index, byte[] src, int offset, int length) {

  }

  @Override
  public boolean getBoolean(int index) {
    return false;
  }

  @Override
  public void putBoolean(int index, boolean value) {

  }

  @Override
  public void get(DataOutput out, int offset, int length) throws IOException {

  }

  @Override
  public void put(DataInput in, int offset, int length) throws IOException {

  }

  @Override
  public void get(int offset, ByteBuffer target, int numBytes) {
    if (( offset | numBytes | (offset + numBytes)) < 0) {
      throw new IndexOutOfBoundsException();
    }
    int targetOffset = target.position();
    int remaining = target.remaining();
    if (remaining < numBytes) {
      throw new BufferOverflowException();
    }
    if (target.isDirect()) {
      if (target.isReadOnly()) {
        throw new ReadOnlyBufferException();
      }
    }
    long targetPointer = getAddress(target) + targetOffset;
    long sourcePointer = address + offset;

    if (sourcePointer <= addressLimit - numBytes) {
      UNSAFE.copyMemory(heapMemory, sourcePointer, null, targetPointer, numBytes);
      target.position(targetOffset + numBytes);
    }
  }

  @Override
  public void put(int offset, ByteBuffer source, int numBytes) {

  }

  private static final Field ADDRESS_FIELD;

  static {
    try {
      ADDRESS_FIELD  =  java.nio.Buffer.class.getDeclaredField("address");
      ADDRESS_FIELD.setAccessible(true);
    }catch (Throwable throwable){
      throw new RuntimeException("cannot initialize hybridmemorysegment ");
    }
  }


  private static  long getAddress(ByteBuffer buffer) {
    if (buffer == null) {
      throw new NullPointerException("buffer is null");
    }
    try {
      return (long) ADDRESS_FIELD.get(buffer);
    } catch (IllegalAccessException t) {
      throw new RuntimeException("could not access direct byte buffer address",t);
    }
  }

  private static long checkBufferAndGetAddress(ByteBuffer buffer) {
    if (buffer == null) {
      throw new NullPointerException("buffer is null");
    }
    if (!buffer.isDirect()) {
      throw new IllegalArgumentException("can't initialize from non-direct bytebuffer");
    }

    return getAddress(buffer);
  }
}
