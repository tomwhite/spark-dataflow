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

import java.io.Serializable;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.junit.Test;
import scala.Tuple2;

public class SparkStreamingWordCountTest implements Serializable {
  @Test
  public void testRun() throws Exception {
    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("StreamingWordCount");
    JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
    JavaReceiverInputDStream<String> lines = jssc.receiverStream(new FakeReceiver());
    JavaDStream<String> words = lines.flatMap(
        new FlatMapFunction<String, String>() {
          @Override
          public Iterable<String> call(String x) {
            return Arrays.asList(x.split(" "));
          }
        });
    // Count each word in each batch
    JavaPairDStream<String, Integer> pairs = words.mapToPair(
        new PairFunction<String, String, Integer>() {
          @Override public Tuple2<String, Integer> call(String s) {
            return new Tuple2<String, Integer>(s, 1);
          }
        });
    JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
        new Function2<Integer, Integer, Integer>() {
          @Override public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
        });

// Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print();

    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
  }

  static class FakeReceiver extends Receiver<String> {

    private Thread thread; // Thread rather than ScheduledExecutorService since Thread is Serializable

    public FakeReceiver() {
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
          while(!isStopped()) {
            if (count % 2 == 0) {
              store("pig");
            } else {
              store("goat");
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
}
