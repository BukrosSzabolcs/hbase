/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.chaos.actions;

import java.io.IOException;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Threads;

/**
* Base class for restarting HBaseServer's
*/
public class RestartActionBaseAction extends Action {
  long sleepTime; // how long should we sleep

  public RestartActionBaseAction(long sleepTime) {
    this.sleepTime = sleepTime;
  }

  void sleep(long sleepTime) {
    LOG.info("Sleeping for:" + sleepTime);
    Threads.sleep(sleepTime);
  }

  void restartMaster(ServerName server, long sleepTime) throws IOException {
    sleepTime = Math.max(sleepTime, 1000);
    // Don't try the kill if we're stopping
    if (context.isStopping()) {
      return;
    }

    killMaster(server);
    sleep(sleepTime);
    startMaster(server);
  }

  /**
   * Stop and then restart the region server instaedof killing it.
   * @param server
   * @param sleepTime
   * @throws IOException
   */
  void gracefulRestartRs(ServerName server, long sleepTime) throws IOException {
    sleepTime = Math.max(sleepTime, 1000);
    // Don't try the stop if we're stopping already
    if (context.isStopping()) {
      return;
    }
    stopRs(server);
    sleep(sleepTime);
    startRs(server);
  }

  void restartRs(ServerName server, long sleepTime) throws IOException {
    sleepTime = Math.max(sleepTime, 1000);
    // Don't try the kill if we're stopping
    if (context.isStopping()) {
      return;
    }
    killRs(server);
    sleep(sleepTime);
    startRs(server);
  }

  void restartZKNode(ServerName server, long sleepTime) throws IOException {
    sleepTime = Math.max(sleepTime, 1000);
    // Don't try the kill if we're stopping
    if (context.isStopping()) {
      return;
    }
    killZKNode(server);
    sleep(sleepTime);
    startZKNode(server);
  }

  void restartDataNode(ServerName server, long sleepTime) throws IOException {
    sleepTime = Math.max(sleepTime, 1000);
    // Don't try the kill if we're stopping
    if (context.isStopping()) {
      return;
    }
    killDataNode(server);
    sleep(sleepTime);
    startDataNode(server);
  }

  void restartNameNode(ServerName server, long sleepTime) throws IOException {
    sleepTime = Math.max(sleepTime, 1000);
    // Don't try the kill if we're stopping
    if (context.isStopping()) {
      return;
    }
    killNameNode(server);
    sleep(sleepTime);
    startNameNode(server);
  }

}
