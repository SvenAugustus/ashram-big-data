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


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import xyz.flysium.bd.utils.ZookeeperUtils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Curator（ZooKeeper） 分布式锁
 *
 * @author Sven Augustus
 * @version 1.0
 */
public abstract class AbstractCuratorDistributedLock implements DistributedLock {

  /**
   * CuratorFramework 客户端实例
   */
  private final CuratorFramework client;
  /**
   * 锁的节点路径，如 /testLock/app1/lock
   */
  private final InterProcessLock lock;

  AbstractCuratorDistributedLock(CuratorFramework client, InterProcessLock lock) {
    this.client = client;
    this.lock = lock;
  }

  @Override
  public void lock() throws Exception {
    lock.acquire();
  }

  @Override
  public boolean tryLock(int timeout) throws Exception {
    return lock.acquire(timeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public void unlock() throws Exception {
    lock.release();
  }

  @Override
  public void close() throws IOException {
    ZookeeperUtils.closeCuratorClient(client);
  }

}
