package com.cloudera.dataflow.spark.coders;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.ListCoder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class SeqCoder<T> extends StandardCoder<Seq<T>> {

  public static <T> SeqCoder<T> of(Coder<T> elemCoder) {
    return new SeqCoder<>(elemCoder);
  }

  public static <T> List<Object> getInstanceComponents(Seq<T> exampleValue) {
    return ListCoder.getInstanceComponents(JavaConversions.asJavaList(exampleValue));
  }

  ///////////

  private ListCoder<T> listCoder;

  public SeqCoder(Coder<T> elemCoder) {
    this.listCoder = ListCoder.of(elemCoder);
  }

  @Override
  public void encode(Seq<T> t, OutputStream outputStream, Context context) throws CoderException, IOException {
    listCoder.encode(JavaConversions.asJavaList(t), outputStream, context);
  }

  @Override
  public Seq<T> decode(InputStream inputStream, Context context) throws CoderException, IOException {
    return JavaConversions.asScalaBuffer(listCoder.decode(inputStream, context)).toList();
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return listCoder.getCoderArguments();
  }

  @Override
  public boolean isDeterministic() {
    return listCoder.isDeterministic();
  }
}
