package com.cloudera.dataflow.spark;

import com.cloudera.dataflow.spark.coders.SeqCoder;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.DoubleCoder;
import com.google.cloud.dataflow.sdk.coders.InstantCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.coders.VarLongCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.common.base.Optional;
import com.google.common.reflect.TypeToken;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import scala.collection.immutable.List;
import scala.reflect.ClassTag;
import scala.reflect.OptManifest;

public class CoderSerializer extends Serializer implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(CoderSerializer.class);

  private static class CoderSerializationStream<T> extends SerializationStream {

    private CoderRegistry coderRegistry;
    private final OutputStream out;

    public CoderSerializationStream(CoderRegistry coderRegistry, OutputStream out) {
      this.coderRegistry = coderRegistry;
      this.out = out;
    }

    @Override
    public <U> SerializationStream writeObject(U t, ClassTag<U> tag) {
      Class<?> cls = tag.runtimeClass();
      try {
        // If U is a collection type (e.g. List or Seq) then we can't get the component
        // type (since we only have ClassTag, not TypeTag). The result is that we can't
        // find the correct coder here.
        Optional<? extends Coder<?>> coder = coderRegistry.getCoder(TypeToken.of(cls));
        if (!coder.isPresent()) {
          throw new RuntimeException("No coder found for class " + cls);
        }
        ((Coder<U>) coder.get()).encode(t, out, Coder.Context.OUTER);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    @Override
    public void flush() {
      try {
        out.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      try {
        out.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class CoderDeserializationStream<T> extends DeserializationStream {

    private CoderRegistry coderRegistry;
    private InputStream in;

    public CoderDeserializationStream(CoderRegistry coderRegistry, InputStream in) {
      this.coderRegistry = coderRegistry;
      this.in = in;
    }

    @Override
    public <U> U readObject(ClassTag<U> tag) {
      Class<?> cls = tag.runtimeClass();
      try {
        Optional<? extends Coder<?>> coder = coderRegistry.getCoder(TypeToken.of(cls));
        if (!coder.isPresent()) {
          throw new RuntimeException("No coder found for class " + cls);
        }
        return ((Coder<U>) coder.get()).decode(in, Coder.Context.OUTER);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      try {
        in.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class CoderSerializerInstance extends SerializerInstance {

    private CoderRegistry coderRegistry;

    public CoderSerializerInstance(CoderRegistry coderRegistry) {
      this.coderRegistry = coderRegistry;
    }

    @Override
    public <T> ByteBuffer serialize(T t, ClassTag<T> tag) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      SerializationStream out = serializeStream(bos);
      out.writeObject(t, tag);
      out.close();
      return ByteBuffer.wrap(bos.toByteArray());
    }

    @Override
    public <T> T deserialize(ByteBuffer bytes, ClassTag<T> tag) {
      ByteBufferInputStream bis =
          new ByteBufferInputStream(Collections.singletonList(bytes));
      DeserializationStream in = deserializeStream(bis);
      return in.readObject(tag);
    }

    @Override
    public <T> T deserialize(ByteBuffer bytes, ClassLoader loader, ClassTag<T> tag) {
      return deserialize(bytes, tag);
    }

    @Override
    public SerializationStream serializeStream(OutputStream s) {
      return new CoderSerializationStream<>(coderRegistry, s);
    }

    @Override
    public DeserializationStream deserializeStream(InputStream s) {
      return new CoderDeserializationStream<>(coderRegistry, s);
    }
  }

  @Override
  public SerializerInstance newInstance() {
    CoderRegistry coderRegistry = new CoderRegistry();
    coderRegistry.registerStandardCoders();
    coderRegistry.registerCoder(Seq.class, SeqCoder.class);

    return new CoderSerializerInstance(coderRegistry);
  }

}
