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

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;
import xyz.flysium.bd.utils.ZookeeperUtils;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * ZooKeeper 分布式锁
 *
 * @author Sven Augustus
 * @version 1.0
 */
public abstract class AbstractZookeeperDistributedLock implements DistributedLock {

  /**
   * Zookeeper 客户端实例
   */
  protected final ZooKeeper zookeeper;
  /**
   * 锁的节点路径，如 /testLock/app1/lock
   */
  protected final String path;
  /**
   * 锁的父亲节点路径，如 /testLock/app1
   */
  protected final String parentPath;
  /**
   * 节点属性，用来标记是否获得锁
   */
  private Stat stat = new Stat();
  /**
   * 获得锁前阻塞等待
   */
  private CountDownLatch latch = new CountDownLatch(1);

  /**
   * 支持本地线程的可重入锁
   */
  private final boolean reentrant;
  /**
   * 当前重入的次数
   */
  private int reentrantCount = 0;
  /**
   * 持有的线程
   */
  private Thread currentHoldThread = null;

  protected final MyLockContext context = new MyLockContext();

  public AbstractZookeeperDistributedLock(ZooKeeper zookeeper, String path, boolean reentrant) {
    this.zookeeper = zookeeper;
    PathUtils.validatePath(path);
    this.path = path;
    int index = this.path.lastIndexOf("/");
    this.parentPath = (index < 0) ? "/" : this.path.substring(0, index);
    this.reentrant = reentrant;
  }

  /**
   * 尝试阻塞获得锁，除非超时 timeout 毫秒
   *
   * @param timeout 超时时间，单位毫秒
   */
  @Override
  public boolean tryLock(int timeout) throws Exception {
    if (!reentrant) {
      doLock(timeout);
    }
    // 支持重入锁
    else {
      if (reentrantCount < 1) {
        doLock(timeout);
      } else if (currentHoldThread != Thread.currentThread()) {
        throw new IllegalStateException(
            String.format("Excepted Thread：%s, Actual Thread: %s", currentHoldThread.getId(),
                Thread.currentThread().getId()));
      }
      reentrantCount++;
    }
    // 判定是否获得锁
    return stat != null;
  }

  /**
   * 释放锁
   */
  @Override
  public void unlock() {
    if (stat == null) {
      return;
    }
    if (!reentrant) {
      doUnLock();
      return;
    }
    if (currentHoldThread == Thread.currentThread()) {
      if (reentrantCount <= 1) {
        doUnLock();
      }
      reentrantCount--;
    } else {
      throw new IllegalStateException(
          String.format("Excepted Thread：%s, Actual Thread: %s", currentHoldThread.getId(),
              Thread.currentThread().getId()));
    }
  }

  /**
   * 阻塞获得锁
   */
  private void doLock(int timeout) throws Exception {
    context.setCurrentHoldThread(Thread.currentThread());
    try {
      // 创建节点
      create();
      // 阻塞
      if (timeout > 0) {
        latch.await(timeout, TimeUnit.MILLISECONDS);
      } else {
        latch.await();
      }
    } catch (Throwable e) {
      logger.error("Lock Error ：" + path, e);
      throw e;
    }
    currentHoldThread = Thread.currentThread();
  }

  /**
   * 解锁
   */
  private void doUnLock() {
    try {
      // 释放节点
      delete();
    } catch (Throwable e) {
      logger.error("Unlock Error ：" + path, e);
    }
  }

  @Override
  public void close() throws IOException {
    ZookeeperUtils.closeConnection(zookeeper);
  }

  /**
   * 解除阻塞，获得锁
   *
   * @param stat 节点属性
   */
  protected void returnLock(Stat stat) {
    this.stat = stat;
    // 解除阻塞
    latch.countDown();
    // reset the CountDownLatch
    latch = new CountDownLatch(1);
  }

  /**
   * 创建节点
   *
   * @throws Exception 创建过程发生异常
   */
  protected abstract void create() throws Exception;

  /**
   * 释放节点
   *
   * @throws Exception 释放过程发生异常
   */
  protected abstract void delete() throws Exception;

}
