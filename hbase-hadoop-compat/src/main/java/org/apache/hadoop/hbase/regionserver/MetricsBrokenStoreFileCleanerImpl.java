/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableTimeHistogram;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Implementation of {@link MetricsBrokenStoreFileCleaner} to track metrics for a specific
 * RegionServer.
 */
@InterfaceAudience.Private public class MetricsBrokenStoreFileCleanerImpl extends BaseSourceImpl
  implements MetricsBrokenStoreFileCleaner {

  private MutableFastCounter brokenStoreFileCleanerDeletes;
  private MutableFastCounter brokenStoreFileCleanerFailedDeletes;
  private MutableFastCounter brokenStoreFileCleanerRuns;
  private MutableTimeHistogram brokenStoreFileCleanerTimer;

  public MetricsBrokenStoreFileCleanerImpl(String metricsName, String metricsDescription,
    String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

    brokenStoreFileCleanerDeletes = getMetricsRegistry().newCounter(DELETES, DELETES_DESC, 0L);
    brokenStoreFileCleanerFailedDeletes =
      getMetricsRegistry().newCounter(FAILED_DELETES, FAILED_DELETES_DESC, 0L);
    brokenStoreFileCleanerRuns = getMetricsRegistry().newCounter(RUNS, RUNS_DESC, 0L);
    brokenStoreFileCleanerTimer = getMetricsRegistry().newTimeHistogram(RUNTIME, RUNTIME_DESC);
  }

  public MetricsBrokenStoreFileCleanerImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  @Override public void incrementBrokenStoreFileCleanerDeletes(long deletes) {
    brokenStoreFileCleanerDeletes.incr(deletes);
  }

  @Override public void incrementBrokenStoreFileCleanerFailedDeletes(long failedDeletes) {
    brokenStoreFileCleanerFailedDeletes.incr(failedDeletes);
  }

  @Override public void incrementBrokenStoreFileCleanerRuns() {
    brokenStoreFileCleanerRuns.incr();
  }

  @Override public void updateBrokenStoreFileCleanerTimer(long millis) {
    brokenStoreFileCleanerTimer.add(millis);
  }
}
