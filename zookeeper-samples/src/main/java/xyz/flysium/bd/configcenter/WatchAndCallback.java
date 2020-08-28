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

package xyz.flysium.bd.configcenter;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Watcher and Data Callback
 *
 * <p>
 * {@link WatchAndCallback} 提供了两类API:
 * <li>获取存在的配置信息，如果不存在则阻塞等待：{@link WatchAndCallback#awaitExistsReturn()}，{@link
 * WatchAndCallback#awaitExistsReturn(int)}</li>
 * <li>阻塞等待数据变化，一但变化则返回配置信息：{@link WatchAndCallback#awaitDataChangedReturn()}，{@link
 * WatchAndCallback#awaitDataChangedReturn(int)}</li>
 *
 * @author Sven Augustus
 * @version 1.0
 */
public class WatchAndCallback implements Watcher, DataCallback, StatCallback {

  private final Logger logger = LoggerFactory.getLogger(WatchAndCallback.class);
  /**
   * Zookeeper 客户端实例
   */
  protected final ZooKeeper zookeeper;
  /**
   * 节点路径，如 /AppConf/testConf
   */
  protected final String path;
  /**
   * 获得数据前阻塞等待
   */
  private CountDownLatch latch = new CountDownLatch(1);

  private final MyConfigurationContext ctx = new MyConfigurationContext(false);
  private volatile MyConfiguration configuration;

  public WatchAndCallback(ZooKeeper zookeeper, String path) {
    this.zookeeper = zookeeper;
    this.path = path;
  }

  /**
   * 获取存在的配置信息，如果不存在则阻塞等待
   *
   * @return 配置信息
   */
  public MyConfiguration awaitExistsReturn() {
    return awaitExistsReturn(0);
  }

  /**
   * 获取存在的配置信息，如果不存在则阻塞等待 timeout 毫秒
   * <p>如果超时也会返回
   *
   * @param timeout 超时等待时间，单位毫秒
   * @return 配置信息
   */
  public MyConfiguration awaitExistsReturn(int timeout) {
    return awaitCallback(timeout, false);
  }

  /**
   * 阻塞等待数据变化，一但变化则返回配置信息
   *
   * @return 配置信息
   */
  public MyConfiguration awaitDataChangedReturn() {
    return awaitDataChangedReturn(0);
  }

  /**
   * 阻塞等待 timeout 毫秒数据变化，一但变化则返回配置信息
   * <p>如果超时也会返回
   *
   * @param timeout 超时等待时间，单位毫秒
   * @return 配置信息
   */
  public MyConfiguration awaitDataChangedReturn(int timeout) {
    return awaitCallback(timeout, true);
  }

  /**
   * 返回配置信息
   * <p>
   * <li> readOnlyChanged =true，如果数据没有发生变化，会阻塞等待 timeout 毫秒 </li>
   * <li> readOnlyChanged =false，如果存在几乎立刻返回，如果不存在，会阻塞等待 timeout 毫秒 </li>
   *
   * @param timeout         超时等待时间，单位毫秒
   * @param readOnlyChanged 是否只有当数据变化才返回配置
   * @return 配置信息
   */
  private MyConfiguration awaitCallback(int timeout, boolean readOnlyChanged) {
    ctx.setReadOnlyChanged(readOnlyChanged);
    // register Watcher , StatCallback
    zookeeper.exists(path, this, this, ctx);
    try {
      if (timeout > 0) {
        latch.await(timeout, TimeUnit.MILLISECONDS);
      } else {
        latch.await();
      }
    } catch (InterruptedException e) {
      logger.error("await error ：" + path, e);
    }
    return configuration;
  }

  private void returnData(MyConfiguration configuration) {
    this.configuration = configuration;
    // 解除阻塞
    latch.countDown();
    // reset the CountDownLatch
    latch = new CountDownLatch(1);
  }

  // Watcher
  @Override
  public void process(WatchedEvent event) {
    logger.debug("Path Watcher: {}， event: {} ", this.path, event);
    switch (event.getType()) {
      case NodeCreated:
        // register Watcher , DataCallback
        zookeeper.getData(path, this, this, ctx);
        break;
      case NodeDeleted:
        returnData(new MyConfiguration(null));
        break;
      case NodeDataChanged:
        // register Watcher , DataCallback
        zookeeper.getData(path, this, this, ctx);
        break;
      default:
        break;
    }
  }

  // DataCallback
  @Override
  public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
    logger.debug("Path DataCallback: {}， result stat: {} ", this.path, stat);
    if (KeeperException.Code.OK.intValue() == rc) {
      if (data != null) {
        returnData(new MyConfiguration(new String(data)));
      }
    }
  }

  // StatCallback
  @Override
  public void processResult(int rc, String path, Object ctx, Stat stat) {
    logger.debug("Path StatCallback: {}， result stat: {} ", this.path, stat);
    MyConfigurationContext context = (MyConfigurationContext) ctx;
    if (context.isReadOnlyChanged()) {
      // do nothing.
      return;
    }
    // 节点不存在，解除阻塞返回 null
    if (stat == null) {
      returnData(new MyConfiguration(null));
      return;
    }
    // 节点存在，获取返回数据，解除阻塞
    try {
      byte[] data = zookeeper.getData(path, this, stat);
      if (data != null) {
        returnData(new MyConfiguration(new String(data)));
      }
    } catch (KeeperException | InterruptedException e) {
      logger.error("Read Data Error：" + path, e);
    }
  }

}
