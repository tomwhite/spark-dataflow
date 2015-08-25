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
import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.SetCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CombineGloballyTest {

  private static final String[] WORDS_ARRAY = {
      "a", "b", "c",
      "d", "", "f"};
  private static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

  @Test
  public void test() throws Exception {
    SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    PCollection<String> inputWords = p.apply(Create.of(WORDS)).setCoder(StringUtf8Coder.of());
    PCollection<String> output = inputWords.apply(Combine.globally(new WordMerger()));

    EvaluationResult res = SparkPipelineRunner.create().run(p);
    assertEquals("[, a, b, c, d, f]", Iterables.getOnlyElement(res.get(output)));
    res.close();
  }

  public static class WordMerger extends Combine.CombineFn<String, Set<String>, String>
      implements Serializable {

    @Override
    public Set<String> createAccumulator() {
      // return null to test that nulls are allowed
      return null;
    }

    @Override
    public Coder<Set<String>> getAccumulatorCoder(CoderRegistry registry,
        Coder<String> inputCoder) throws CannotProvideCoderException {
      return SetCoder.of(StringUtf8Coder.of());
    }

    @Override
    public Set<String> addInput(Set<String> accumulator, String input) {
      return combine(accumulator, input);
    }

    @Override
    public Set<String> mergeAccumulators(Iterable<Set<String>> accumulators) {
      Set<String> set = new TreeSet<>();
      for (Set<String> accum : accumulators) {
        if (accum != null) {
          set.addAll(accum);
        }
      }
      return set;
    }

    @Override
    public String extractOutput(Set<String> accumulator) {
      List<String> sorted = new ArrayList<>(accumulator);
      Collections.sort(sorted);
      return sorted.toString();
    }

    private static Set<String> combine(Set<String> accum, String datum) {
      if (null == accum) {
        Set<String> set = new TreeSet<>();
        set.add(datum);
        return set;
      } else {
        accum.add(datum);
        return accum;
      }
    }
  }
}
