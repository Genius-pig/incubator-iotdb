/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.bufferwriteV2;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.iotdb.db.engine.UnsealedTsFileProcessorV2;
import org.apache.iotdb.db.engine.pool.FlushPoolManager;

public class FlushManager {

  private ConcurrentLinkedQueue<UnsealedTsFileProcessorV2> unsealedTsFileProcessorQueue = new ConcurrentLinkedQueue<>();

  private FlushPoolManager flushPool = FlushPoolManager.getInstance();

  private Runnable flushThread = () -> {
    UnsealedTsFileProcessorV2 udfProcessor = unsealedTsFileProcessorQueue.poll();
    try {
      udfProcessor.flushOneMemTable();
    } catch (IOException e) {
      // TODO do sth
    }
    udfProcessor.setManagedByFlushManager(false);
    registerUnsealedTsFileProcessor(udfProcessor);
  };

  /**
   * Add BufferWriteProcessor to asyncFlush manager
   */
  public boolean registerUnsealedTsFileProcessor(UnsealedTsFileProcessorV2 unsealedTsFileProcessor) {
    synchronized (unsealedTsFileProcessor) {
      if (!unsealedTsFileProcessor.isManagedByFlushManager() && unsealedTsFileProcessor.getFlushingMemTableSize() > 0) {
        unsealedTsFileProcessorQueue.add(unsealedTsFileProcessor);
        unsealedTsFileProcessor.setManagedByFlushManager(true);
        flushPool.submit(flushThread);
        return true;
      }
      return false;
    }
  }

  private FlushManager() {
  }

  public static FlushManager getInstance() {
    return InstanceHolder.instance;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static FlushManager instance = new FlushManager();
  }

}