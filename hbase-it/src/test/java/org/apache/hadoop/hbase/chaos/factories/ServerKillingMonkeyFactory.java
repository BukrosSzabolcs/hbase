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

package org.apache.hadoop.hbase.chaos.factories;

import org.apache.hadoop.hbase.chaos.actions.Action;
import org.apache.hadoop.hbase.chaos.actions.DumpClusterStatusAction;
import org.apache.hadoop.hbase.chaos.actions.ForceBalancerAction;
import org.apache.hadoop.hbase.chaos.actions.GracefulRollingRestartRsAction;
import org.apache.hadoop.hbase.chaos.actions.RestartActiveMasterAction;
import org.apache.hadoop.hbase.chaos.actions.RestartRandomRsExceptMetaAction;
import org.apache.hadoop.hbase.chaos.actions.RollingBatchRestartRsExceptMetaAction;
import org.apache.hadoop.hbase.chaos.actions.RollingBatchSuspendResumeRsAction;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.chaos.policies.CompositeSequentialPolicy;
import org.apache.hadoop.hbase.chaos.policies.DoActionsOncePolicy;
import org.apache.hadoop.hbase.chaos.policies.PeriodicRandomActionPolicy;

/**
 * Creates ChaosMonkeys for doing server restart actions, but not
 * flush / compact / snapshot kind of actions.
 */
public class ServerKillingMonkeyFactory extends MonkeyFactory {

  private long gracefulRollingRestartTSSLeepTime;
  private long rollingBatchSuspendRSSleepTime;
  private float rollingBatchSuspendtRSRatio;
  private boolean killMetaRs;

  @Override
  public ChaosMonkey build() {
    loadProperties();

    // Destructive actions to mess things around. Cannot run batch restart
    Action[] actions1 = new Action[] {
        new RestartRandomRsExceptMetaAction(60000),
        new RestartActiveMasterAction(5000),
        new RollingBatchRestartRsExceptMetaAction(5000, 1.0f, 2), //only allow 2 servers to be dead
      new ForceBalancerAction(),
      new GracefulRollingRestartRsAction(gracefulRollingRestartTSSLeepTime, killMetaRs),
      new RollingBatchSuspendResumeRsAction(rollingBatchSuspendRSSleepTime,
          rollingBatchSuspendtRSRatio, killMetaRs)
    };

    // Action to log more info for debugging
    Action[] actions2 = new Action[] {
        new DumpClusterStatusAction()
    };

    return new PolicyBasedChaosMonkey(util,
      new CompositeSequentialPolicy(
          new DoActionsOncePolicy(60 * 1000, actions1),
          new PeriodicRandomActionPolicy(60 * 1000, actions1)),
      new PeriodicRandomActionPolicy(60 * 1000, actions2));
  }

  private void loadProperties() {
    gracefulRollingRestartTSSLeepTime = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.GRACEFUL_RESTART_RS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_GRACEFUL_RESTART_RS_SLEEP_TIME + ""));
    rollingBatchSuspendRSSleepTime = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.ROLLING_BATCH_SUSPEND_RS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_ROLLING_BATCH_SUSPEND_RS_SLEEP_TIME+ ""));
    rollingBatchSuspendtRSRatio = Float.parseFloat(this.properties.getProperty(
        MonkeyConstants.ROLLING_BATCH_SUSPEND_RS_RATIO,
        MonkeyConstants.DEFAULT_ROLLING_BATCH_SUSPEND_RS_RATIO + ""));
    killMetaRs = Boolean.parseBoolean(this.properties.getProperty(
      MonkeyConstants.KILL_META_RS,
      MonkeyConstants.DEFAULT_KILL_META_RS + ""));
  }
}
