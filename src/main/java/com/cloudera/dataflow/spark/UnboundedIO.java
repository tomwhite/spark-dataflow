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

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.joda.time.Instant;

/**
 * Simple unbounded source and sink to get started with streaming.
 */
public class UnboundedIO {
  public static class UnboundedWords extends PTransform<PInput,
      PCollection<String>> {
    @Override
    public PCollection<String> apply(PInput input) {
      return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
          WindowingStrategy.globalDefault(), PCollection.IsBounded.UNBOUNDED);
    }
  }

  public static class UnboundedWordsReceiver extends Receiver<TimestampedValue<String>> {

    // Use Thread rather than ScheduledExecutorService since Thread is Serializable
    private Thread thread;

    public UnboundedWordsReceiver() {
      super(StorageLevel.MEMORY_ONLY());
    }

    @Override
    public StorageLevel storageLevel() {
      return StorageLevel.MEMORY_ONLY();
    }

    @Override
    public void onStart() {
      thread = new Thread() {
        @Override
        public void run() {
          int count = 0;
          while (!isStopped()) {
            if (count % 2 == 0) {
              store(TimestampedValue.of("pig", Instant.now()));
            } else {
              store(TimestampedValue.of("hen", Instant.now()));
            }
            count++;
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              Thread.interrupted();
              e.printStackTrace();
            }
          }
        }
      };
      thread.start();
    }

    @Override
    public void onStop() {
      try {
        thread.join();
      } catch (InterruptedException e) {
        Thread.interrupted();
        e.printStackTrace();
      }
    }
  }

  public static class Console extends PTransform<PCollection<String>, PDone> {
    @Override
    public PDone apply(PCollection<String> input) {
      return PDone.in(input.getPipeline());
    }
  }

}
