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

package xyz.flysium.bd.utils;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.flysium.bd.distributedlock.DistributedLock;

import java.util.concurrent.CountDownLatch;

/**
 * Default Watcher for Session
 *
 * @author Sven Augustus
 * @version 1.0
 */
public class DefaultWatcher implements Watcher {

  final Logger logger = LoggerFactory.getLogger(DistributedLock.class);

  private CountDownLatch latch;

  public DefaultWatcher(CountDownLatch latch) {
    this.latch = latch;
  }

  @Override
  public void process(WatchedEvent event) {
    logger.debug("Default Watch for Session: " + event);
    switch (event.getState()) {
      case Unknown:
        break;
      case Disconnected:
        break;
      case NoSyncConnected:
        break;
      case SyncConnected:
        latch.countDown();
        latch = new CountDownLatch(1);
        break;
      case AuthFailed:
        break;
      case ConnectedReadOnly:
        break;
      case SaslAuthenticated:
        break;
      case Expired:
        break;
    }
  }

}
