/*
 * MIT License
 *
 * Copyright (c) 2020 SvenAugustus
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package xyz.flysium.bd.distributedlock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * 分布式锁
 *
 * @author Sven Augustus
 * @version 1.0
 */
public interface DistributedLock extends Closeable {

  final Logger logger = LoggerFactory.getLogger(DistributedLock.class);

  /**
   * 阻塞获得锁
   *
   * @throws Exception 获得锁过程异常
   */
  default void lock() throws Exception {
    tryLock(-1);
  }

  /**
   * 尝试阻塞获得锁，除非超时 timeout 毫秒
   *
   * @param timeout 超时时间，单位毫秒
   * @return 是否获得锁
   * @throws Exception 获得锁过程异常
   */
  boolean tryLock(int timeout) throws Exception;

  /**
   * 释放锁
   *
   * @throws Exception 释放锁过程异常
   */
  void unlock() throws Exception;

}
