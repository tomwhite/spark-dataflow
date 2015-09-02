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

import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Helper functions for working with windows.
 */
public final class WindowingHelpers {
  private WindowingHelpers() {
  }

  /**
   * A function for converting a value to a {@link WindowedValue}. The resulting
   * {@link WindowedValue} will be in no windows, and will have the default timestamp
   * and pane.
   *
   * @param <T>   The type of the object.
   * @return A function that accepts an object and returns its {@link WindowedValue}.
   */
  public static <T> Function<T, WindowedValue<T>> windowFunction() {
    return new Function<T, WindowedValue<T>>() {
      @Override
      public WindowedValue<T> call(T t) {
        return WindowedValue.valueInEmptyWindows(t);
      }
    };
  }

  /**
   * A function for extracting the value from a {@link WindowedValue}.
   *
   * @param <T>   The type of the object.
   * @return A function that accepts a {@link WindowedValue} and returns its value.
   */
  public static <T> Function<WindowedValue<T>, T> unwindowFunction() {
    return new Function<WindowedValue<T>, T>() {
      @Override
      public T call(WindowedValue<T> t) {
        return t.getValue();
      }
    };
  }

  /**
   * A function for converting a {@link WindowedValue} key-value pair to a
   * {@link WindowedValue} key and a separate (non-windowed) value.
   *
   * @param <K>   The type of the key.
   * @param <V>   The type of the value.
   * @return A function that accepts a {@link WindowedValue} key-value pair and returns
   * a {@link WindowedValue} key and a separate (non-windowed) value.
   */
  public static <K, V> PairFunction<WindowedValue<KV<K, V>>,
      WindowedValue<K>, V> toPairWindowFunction() {
    return new PairFunction<WindowedValue<KV<K, V>>,
        WindowedValue<K>, V>() {
      @Override
      public Tuple2<WindowedValue<K>, V> call(WindowedValue<KV<K, V>> wkv) {
        KV<K, V> kv = wkv.getValue();
        return new Tuple2<>(wkv.withValue(kv.getKey()), kv.getValue());
      }
    };
  }

  /**
   * A function for converting a {@link Tuple2} of a {@link WindowedValue} key and a
   * separate (non-windowed) value to a {@link WindowedValue} key-value pair.
   *
   * @param <K>   The type of the key.
   * @param <V>   The type of the value.
   * @return A function that accepts a {@link Tuple2} of a {@link WindowedValue} key and a
   * separate (non-windowed) value and returns a {@link WindowedValue} key-value pair.
   */
  public static <K, V> Function<Tuple2<WindowedValue<K>, V>,
      WindowedValue<KV<K, V>>> fromPairWindowFunction() {
    return new Function<Tuple2<WindowedValue<K>, V>, WindowedValue<KV<K, V>>>() {
      @Override
      public WindowedValue<KV<K, V>> call(Tuple2<WindowedValue<K>, V> t2) {
        return t2._1().withValue(KV.of(t2._1().getValue(), t2._2()));
      }
    };
  }
}
