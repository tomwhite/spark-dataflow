/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

public interface SparkPipelineOptions extends PipelineOptions {
  @Description("The url of the spark master to connect to, (e.g. spark://host:port, local[4]).")
  @Default.String("local[1]")
  String getSparkMaster();

  void setSparkMaster(String master);

  @Description("Set to true if running a streaming pipeline.")
  boolean isStreaming();
  void setStreaming(boolean value);
}
