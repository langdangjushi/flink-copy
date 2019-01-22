package org.apache.flink.core.memory;

import org.junit.Test;

/**
 * created by pengmingguo on 1/22/19
 */
public class MemorySegmentTest {
  @Test
  public void test1() {
    byte[] array = new byte[5];
    new MemorySegment(array,null);
  }

}
