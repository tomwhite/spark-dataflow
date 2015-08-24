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

import static com.cloudera.dataflow.spark.ShardNameBuilder.getOutputDirectory;
import static com.cloudera.dataflow.spark.ShardNameBuilder.getOutputFilePrefix;
import static com.cloudera.dataflow.spark.ShardNameBuilder.getOutputFileTemplate;
import static com.cloudera.dataflow.spark.ShardNameBuilder.replaceShardCount;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.google.api.client.util.Maps;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import com.cloudera.dataflow.hadoop.HadoopIO;

/**
 * Supports translation between a DataFlow transform, and Spark's operations on RDDs.
 */
public final class TransformTranslator {

  private TransformTranslator() {
  }

  private static class FieldGetter {
    private final Map<String, Field> fields;

    FieldGetter(Class<?> clazz) {
      this.fields = Maps.newHashMap();
      for (Field f : clazz.getDeclaredFields()) {
        f.setAccessible(true);
        this.fields.put(f.getName(), f);
      }
    }

    public <T> T get(String fieldname, Object value) {
      try {
        @SuppressWarnings("unchecked")
        T fieldValue = (T) fields.get(fieldname).get(value);
        return fieldValue;
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private static <T> TransformEvaluator<Flatten.FlattenPCollectionList<T>> flattenPColl() {
    return new TransformEvaluator<Flatten.FlattenPCollectionList<T>>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(Flatten.FlattenPCollectionList<T> transform, EvaluationContext context) {
        PCollectionList<T> pcs = context.getInput(transform);
        JavaRDD<T>[] rdds = new JavaRDD[pcs.size()];
        for (int i = 0; i < rdds.length; i++) {
          rdds[i] = (JavaRDD<T>) context.getRDD(pcs.get(i));
        }
        JavaRDD<T> rdd = context.getSparkContext().union(rdds);
        context.setOutputRDD(transform, rdd);
      }
    };
  }

  private static <K, V> TransformEvaluator<GroupByKey.GroupByKeyOnly<K, V>> gbk() {
    return new TransformEvaluator<GroupByKey.GroupByKeyOnly<K, V>>() {
      @Override
      public void evaluate(GroupByKey.GroupByKeyOnly<K, V> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDDLike<KV<K, V>, ?> inRDD =
            (JavaRDDLike<KV<K, V>, ?>) context.getInputRDD(transform);
        @SuppressWarnings("unchecked")
        KvCoder<K, V> coder = (KvCoder<K, V>) context.getInput(transform).getCoder();
        final Coder<K> keyCoder = coder.getKeyCoder();
        final Coder<V> valueCoder = coder.getValueCoder();

        // Use coders to convert objects in the PCollection to byte arrays, so they
        // can be transferred over the network for the shuffle.
        JavaRDDLike<KV<K, Iterable<V>>, ?> outRDD = fromPair(toPair(inRDD)
            .mapToPair(CoderHelpers.toByteFunction(keyCoder, valueCoder))
            .groupByKey()
            .mapToPair(CoderHelpers.fromByteFunctionIterable(keyCoder, valueCoder)));
        context.setOutputRDD(transform, outRDD);
      }
    };
  }

  private static <K, V> JavaPairRDD<K, V> toPair(JavaRDDLike<KV<K, V>, ?> rdd) {
    return rdd.mapToPair(new PairFunction<KV<K, V>, K, V>() {
      @Override
      public Tuple2<K, V> call(KV<K, V> kv) {
        return new Tuple2<>(kv.getKey(), kv.getValue());
      }
    });
  }

  private static <K, V> JavaRDDLike<KV<K, V>, ?> fromPair(JavaPairRDD<K, V> rdd) {
    return rdd.map(new Function<Tuple2<K, V>, KV<K, V>>() {
      @Override
      public KV<K, V> call(Tuple2<K, V> t2) {
        return KV.of(t2._1(), t2._2());
      }
    });
  }

  private static <I, O> TransformEvaluator<ParDo.Bound<I, O>> parDo() {
    return new TransformEvaluator<ParDo.Bound<I, O>>() {
      @Override
      public void evaluate(ParDo.Bound<I, O> transform, EvaluationContext context) {
        DoFnFunction<I, O> dofn =
            new DoFnFunction<>(transform.getFn(),
                context.getRuntimeContext(),
                getSideInputs(transform.getSideInputs(), context));
        @SuppressWarnings("unchecked")
        JavaRDDLike<I, ?> inRDD = (JavaRDDLike<I, ?>) context.getInputRDD(transform);
        context.setOutputRDD(transform, inRDD.mapPartitions(dofn));
      }
    };
  }

  private static <I, O> TransformEvaluator<ParDo.BoundMulti<I, O>> multiDo() {
    return new TransformEvaluator<ParDo.BoundMulti<I, O>>() {
      @Override
      public void evaluate(ParDo.BoundMulti<I, O> transform, EvaluationContext context) {
        TupleTag<O> mainOutputTag = transform.getMainOutputTag();
        MultiDoFnFunction<I, O> multifn = new MultiDoFnFunction<>(
            transform.getFn(),
            context.getRuntimeContext(),
            mainOutputTag,
            getSideInputs(transform.getSideInputs(), context));

        @SuppressWarnings("unchecked")
        JavaRDDLike<I, ?> inRDD = (JavaRDDLike<I, ?>) context.getInputRDD(transform);
        JavaPairRDD<TupleTag<?>, Object> all = inRDD
            .mapPartitionsToPair(multifn)
            .cache();

        PCollectionTuple pct = context.getOutput(transform);
        for (Map.Entry<TupleTag<?>, PCollection<?>> e : pct.getAll().entrySet()) {
          @SuppressWarnings("unchecked")
          JavaPairRDD<TupleTag<?>, Object> filtered =
              all.filter(new TupleTagFilter(e.getKey()));
          context.setRDD(e.getValue(), filtered.values());
        }
      }
    };
  }


  private static <T> TransformEvaluator<TextIO.Read.Bound<T>> readText() {
    return new TransformEvaluator<TextIO.Read.Bound<T>>() {
      @Override
      public void evaluate(TextIO.Read.Bound<T> transform, EvaluationContext context) {
        String pattern = transform.getFilepattern();
        JavaRDD<String> rdd = context.getSparkContext().textFile(pattern);
        context.setOutputRDD(transform, rdd);
      }
    };
  }

  private static <T> TransformEvaluator<TextIO.Write.Bound<T>> writeText() {
    return new TransformEvaluator<TextIO.Write.Bound<T>>() {
      @Override
      public void evaluate(TextIO.Write.Bound<T> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaPairRDD<T, Void> last = ((JavaRDDLike<T, ?>) context.getInputRDD(transform))
            .mapToPair(new PairFunction<T, T,
                Void>() {
              @Override
              public Tuple2<T, Void> call(T t) throws Exception {
                return new Tuple2<>(t, null);
              }
            });
        ShardTemplateInformation shardTemplateInfo =
            new ShardTemplateInformation(transform.getNumShards(),
                transform.getShardTemplate(), transform.getFilenamePrefix(),
                transform.getFilenameSuffix());
        writeHadoopFile(last, new Configuration(), shardTemplateInfo, Text.class,
            NullWritable.class, TemplatedTextOutputFormat.class);
      }
    };
  }

  private static <T> TransformEvaluator<AvroIO.Read.Bound<T>> readAvro() {
    return new TransformEvaluator<AvroIO.Read.Bound<T>>() {
      @Override
      public void evaluate(AvroIO.Read.Bound<T> transform, EvaluationContext context) {
        String pattern = transform.getFilepattern();
        JavaSparkContext jsc = context.getSparkContext();
        @SuppressWarnings("unchecked")
        JavaRDD<AvroKey<T>> avroFile = (JavaRDD<AvroKey<T>>) (JavaRDD<?>)
            jsc.newAPIHadoopFile(pattern,
                                 AvroKeyInputFormat.class,
                                 AvroKey.class, NullWritable.class,
                                 new Configuration()).keys();
        JavaRDD<T> rdd = avroFile.map(
            new Function<AvroKey<T>, T>() {
              @Override
              public T call(AvroKey<T> key) {
                return key.datum();
              }
            });
        context.setOutputRDD(transform, rdd);
      }
    };
  }

  private static <T> TransformEvaluator<AvroIO.Write.Bound<T>> writeAvro() {
    return new TransformEvaluator<AvroIO.Write.Bound<T>>() {
      @Override
      public void evaluate(AvroIO.Write.Bound<T> transform, EvaluationContext context) {
        Job job;
        try {
          job = Job.getInstance();
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
        AvroJob.setOutputKeySchema(job, transform.getSchema());
        @SuppressWarnings("unchecked")
        JavaPairRDD<AvroKey<T>, NullWritable> last =
            ((JavaRDDLike<T, ?>) context.getInputRDD(transform))
            .mapToPair(new PairFunction<T, AvroKey<T>, NullWritable>() {
              @Override
              public Tuple2<AvroKey<T>, NullWritable> call(T t) throws Exception {
                return new Tuple2<>(new AvroKey<>(t), NullWritable.get());
              }
            });
        ShardTemplateInformation shardTemplateInfo =
            new ShardTemplateInformation(transform.getNumShards(),
            transform.getShardTemplate(), transform.getFilenamePrefix(),
            transform.getFilenameSuffix());
        writeHadoopFile(last, job.getConfiguration(), shardTemplateInfo,
            AvroKey.class, NullWritable.class, TemplatedAvroKeyOutputFormat.class);
      }
    };
  }

  private static <K, V> TransformEvaluator<HadoopIO.Read.Bound<K, V>> readHadoop() {
    return new TransformEvaluator<HadoopIO.Read.Bound<K, V>>() {
      @Override
      public void evaluate(HadoopIO.Read.Bound<K, V> transform, EvaluationContext context) {
        String pattern = transform.getFilepattern();
        JavaSparkContext jsc = context.getSparkContext();
        @SuppressWarnings ("unchecked")
        JavaPairRDD<K, V> file = jsc.newAPIHadoopFile(pattern,
            transform.getFormatClass(),
            transform.getKeyClass(), transform.getValueClass(),
            new Configuration());
        JavaRDD<KV<K, V>> rdd = file.map(new Function<Tuple2<K, V>, KV<K, V>>() {
          @Override
          public KV<K, V> call(Tuple2<K, V> t2) throws Exception {
            return KV.of(t2._1(), t2._2());
          }
        });
        context.setOutputRDD(transform, rdd);
      }
    };
  }

  private static <K, V> TransformEvaluator<HadoopIO.Write.Bound<K, V>> writeHadoop() {
    return new TransformEvaluator<HadoopIO.Write.Bound<K, V>>() {
      @Override
      public void evaluate(HadoopIO.Write.Bound<K, V> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaPairRDD<K, V> last = ((JavaRDDLike<KV<K, V>, ?>) context
            .getInputRDD(transform))
            .mapToPair(new PairFunction<KV<K, V>, K, V>() {
              @Override
              public Tuple2<K, V> call(KV<K, V> t) throws Exception {
                return new Tuple2<>(t.getKey(), t.getValue());
              }
            });
        ShardTemplateInformation shardTemplateInfo =
            new ShardTemplateInformation(transform.getNumShards(),
                transform.getShardTemplate(), transform.getFilenamePrefix(),
                transform.getFilenameSuffix());
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> e : transform.getConfigurationProperties().entrySet()) {
          conf.set(e.getKey(), e.getValue());
        }
        writeHadoopFile(last, conf, shardTemplateInfo,
            transform.getKeyClass(), transform.getValueClass(), transform.getFormatClass());
      }
    };
  }

  private static final class ShardTemplateInformation {
    private final int numShards;
    private final String shardTemplate;
    private final String filenamePrefix;
    private final String filenameSuffix;

    private ShardTemplateInformation(int numShards, String shardTemplate, String
        filenamePrefix, String filenameSuffix) {
      this.numShards = numShards;
      this.shardTemplate = shardTemplate;
      this.filenamePrefix = filenamePrefix;
      this.filenameSuffix = filenameSuffix;
    }

    public int getNumShards() {
      return numShards;
    }

    public String getShardTemplate() {
      return shardTemplate;
    }

    public String getFilenamePrefix() {
      return filenamePrefix;
    }

    public String getFilenameSuffix() {
      return filenameSuffix;
    }
  }

  private static <K, V> void writeHadoopFile(JavaPairRDD<K, V> rdd, Configuration conf,
      ShardTemplateInformation shardTemplateInfo, Class<?> keyClass, Class<?> valueClass,
      Class<? extends FileOutputFormat> formatClass) {
    int numShards = shardTemplateInfo.getNumShards();
    String shardTemplate = shardTemplateInfo.getShardTemplate();
    String filenamePrefix = shardTemplateInfo.getFilenamePrefix();
    String filenameSuffix = shardTemplateInfo.getFilenameSuffix();
    if (numShards != 0) {
      // number of shards was set explicitly, so repartition
      rdd = rdd.repartition(numShards);
    }
    int actualNumShards = rdd.partitions().size();
    String template = replaceShardCount(shardTemplate, actualNumShards);
    String outputDir = getOutputDirectory(filenamePrefix, template);
    String filePrefix = getOutputFilePrefix(filenamePrefix, template);
    String fileTemplate = getOutputFileTemplate(filenamePrefix, template);

    conf.set(ShardNameTemplateHelper.OUTPUT_FILE_PREFIX, filePrefix);
    conf.set(ShardNameTemplateHelper.OUTPUT_FILE_TEMPLATE, fileTemplate);
    conf.set(ShardNameTemplateHelper.OUTPUT_FILE_SUFFIX, filenameSuffix);
    rdd.saveAsNewAPIHadoopFile(outputDir, keyClass, valueClass, formatClass, conf);
  }

  private static <T> TransformEvaluator<Window.Bound<T>> window() {
    return new TransformEvaluator<Window.Bound<T>>() {
      @Override
      public void evaluate(Window.Bound<T> transform, EvaluationContext context) {
        // TODO: detect and support non-global windows
        context.setOutputRDD(transform, context.getInputRDD(transform));
      }
    };
  }

  private static <T> TransformEvaluator<Create.Values<T>> create() {
    return new TransformEvaluator<Create.Values<T>>() {
      @Override
      public void evaluate(Create.Values<T> transform, EvaluationContext context) {
        Iterable<T> elems = transform.getElements();
        // Use a coder to convert the objects in the PCollection to byte arrays, so they
        // can be transferred over the network.
        Coder<T> coder = context.getOutput(transform).getCoder();
        context.setOutputRDDFromValues(transform, elems, coder);
      }
    };
  }

  private static <R, W> TransformEvaluator<View.CreatePCollectionView<R, W>> createPCollView() {
    return new TransformEvaluator<View.CreatePCollectionView<R, W>>() {
      @Override
      public void evaluate(View.CreatePCollectionView<R, W> transform, EvaluationContext context) {
        Iterable<WindowedValue<?>> iter = Iterables.transform(
            context.get(context.getInput(transform)), new WindowingFunction<R>());
        context.setPView(context.getOutput(transform), iter);
      }
    };
  }

  private static class WindowingFunction<R> implements com.google.common.base.Function<R,
      WindowedValue<?>> {
    @Override
    public WindowedValue<R> apply(R t) {
      return WindowedValue.valueInGlobalWindow(t);
    }
  }

  private static final class TupleTagFilter<V>
      implements Function<Tuple2<TupleTag<V>, Object>, Boolean> {

    private final TupleTag<V> tag;

    private TupleTagFilter(TupleTag<V> tag) {
      this.tag = tag;
    }

    @Override
    public Boolean call(Tuple2<TupleTag<V>, Object> input) {
      return tag.equals(input._1());
    }
  }

  private static Map<TupleTag<?>, BroadcastHelper<?>> getSideInputs(
      List<PCollectionView<?>> views,
      EvaluationContext context) {
    if (views == null) {
      return ImmutableMap.of();
    } else {
      Map<TupleTag<?>, BroadcastHelper<?>> sideInputs = Maps.newHashMap();
      for (PCollectionView<?> view : views) {
        Iterable<WindowedValue<?>> collectionView = context.getPCollectionView(view);
        Coder<Iterable<WindowedValue<?>>> coderInternal = view.getCoderInternal();
        BroadcastHelper<?> helper = BroadcastHelper.create(collectionView, coderInternal);
        //broadcast side inputs
        helper.broadcast(context.getSparkContext());
        sideInputs.put(view.getTagInternal(), helper);
      }
      return sideInputs;
    }
  }

  private static final Map<Class<? extends PTransform>, TransformEvaluator<?>> EVALUATORS = Maps
      .newHashMap();

  static {
    EVALUATORS.put(TextIO.Read.Bound.class, readText());
    EVALUATORS.put(TextIO.Write.Bound.class, writeText());
    EVALUATORS.put(AvroIO.Read.Bound.class, readAvro());
    EVALUATORS.put(AvroIO.Write.Bound.class, writeAvro());
    EVALUATORS.put(HadoopIO.Read.Bound.class, readHadoop());
    EVALUATORS.put(HadoopIO.Write.Bound.class, writeHadoop());
    EVALUATORS.put(ParDo.Bound.class, parDo());
    EVALUATORS.put(ParDo.BoundMulti.class, multiDo());
    EVALUATORS.put(GroupByKey.GroupByKeyOnly.class, gbk());
    EVALUATORS.put(Flatten.FlattenPCollectionList.class, flattenPColl());
    EVALUATORS.put(Create.Values.class, create());
    EVALUATORS.put(View.CreatePCollectionView.class, createPCollView());
    EVALUATORS.put(Window.Bound.class, window());
  }

  public static <PT extends PTransform<?, ?>> boolean hasTransformEvaluator(Class<PT> clazz) {
    return EVALUATORS.containsKey(clazz);
  }

  public static <PT extends PTransform<?, ?>> TransformEvaluator<PT> getTransformEvaluator(Class<PT>
                                                                                           clazz) {
    @SuppressWarnings("unchecked")
    TransformEvaluator<PT> transform = (TransformEvaluator<PT>) EVALUATORS.get(clazz);
    if (transform == null) {
      throw new IllegalStateException("No TransformEvaluator registered for " + clazz);
    }
    return transform;
  }
}
