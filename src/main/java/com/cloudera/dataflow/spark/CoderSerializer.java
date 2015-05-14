package com.cloudera.dataflow.spark;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.DoubleCoder;
import com.google.cloud.dataflow.sdk.coders.InstantCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.coders.VarLongCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
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
import scala.reflect.ClassTag;

public class CoderSerializer extends Serializer implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(CoderSerializer.class);

  private static class CoderSerializationStream<T> extends SerializationStream {

    private Map<Class<?>, Coder<?>> coders;
    private final OutputStream out;

    public CoderSerializationStream(Map<Class<?>, Coder<?>> coders, OutputStream out) {
      this.coders = coders;
      this.out = out;
    }

    @Override
    public <U> SerializationStream writeObject(U t, ClassTag<U> tag) {
      Class<?> cls = tag.runtimeClass();
      try {
        Coder<U> coder = (Coder<U>) coders.get(cls);
        if (coder == null) {
          throw new RuntimeException("No coder found for class " + cls);
        }
        coder.encode(t, out, Coder.Context.OUTER);
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

    private Map<Class<?>, Coder<?>> coders;
    private InputStream in;

    public CoderDeserializationStream(Map<Class<?>, Coder<?>> coders, InputStream in) {
      this.coders = coders;
      this.in = in;
    }

    @Override
    public <U> U readObject(ClassTag<U> tag) {
      Class<?> cls = tag.runtimeClass();
      try {
        Coder<U> coder = (Coder<U>) coders.get(cls);
        if (coder == null) {
          throw new RuntimeException("No coder found for class " + cls);
        }
        return coder.decode(in, Coder.Context.OUTER);
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

    private Map<Class<?>, Coder<?>> coders;

    public CoderSerializerInstance(Map<Class<?>, Coder<?>> coders) {
      this.coders = coders;
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
      return new CoderSerializationStream<>(coders, s);
    }

    @Override
    public DeserializationStream deserializeStream(InputStream s) {
      return new CoderDeserializationStream<>(coders, s);
    }
  }

  @Override
  public SerializerInstance newInstance() {
    Map<Class<?>, Coder<?>> coders = new HashMap<>();
    // TODO - get coders from a Pipeline's CoderRegistry
    coders.put(Double.class, DoubleCoder.of());
    coders.put(Instant.class, InstantCoder.of());
    coders.put(Integer.class, VarIntCoder.of());
    coders.put(Long.class, VarLongCoder.of());
    coders.put(String.class, StringUtf8Coder.of());
    coders.put(TableRow.class, TableRowJsonCoder.of());
    coders.put(Void.class, VoidCoder.of());
    coders.put(byte[].class, ByteArrayCoder.of());

    return new CoderSerializerInstance(coders);
  }

}
