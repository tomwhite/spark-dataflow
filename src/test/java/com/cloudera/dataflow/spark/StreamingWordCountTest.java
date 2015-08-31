/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;
import java.io.Serializable;
import org.joda.time.Duration;
import org.junit.Test;

public class StreamingWordCountTest implements Serializable {
  @Test
  public void testRun() throws Exception {
    SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
    options.setStreaming(true);
    options.setRunner(SparkPipelineRunner.class);
    options.setSparkMaster("local[2]");
    Pipeline p = Pipeline.create(options);

    PCollection<String> inputWords = p.apply(new UnboundedIO.UnboundedWords());
    PCollection<String> windowedWords = inputWords
        .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(1))));
    PCollection<String> output = windowedWords.apply(new SimpleWordCountTest.CountWords());
    output.apply(new UnboundedIO.Console()); // send to console

    // TODO: capture output somehow to test it

    EvaluationResult res = SparkPipelineRunner.create().run(p);

    Thread.sleep(5000);

    res.close();
  }

}
