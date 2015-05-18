package com.google.cloud.dataflow.sdk.coders;

import java.util.ArrayList;

public class CoderHelper {

  /*
   * Like getDefaultCoder in CoderRegistry, but takes a Class<?> parameter.
   * Note that this needs to be in the com.google.cloud.dataflow.sdk.coders package to
   * allow access to CoderRegistry#getDefaultCoderFactory.
   */
  public static <T> Coder<T> getDefaultCoder(CoderRegistry coderRegistry, T exampleValue,
      Class<?> clazz) {

    if (clazz.getTypeParameters().length == 0) {
      // Trust that getDefaultCoder returns a valid
      // Coder<T> for non-generic clazz.
      @SuppressWarnings("unchecked")
      Coder<T> coder = (Coder<T>) coderRegistry.getDefaultCoder(clazz);
      return coder;
    } else {
      CoderFactory factory = coderRegistry.getDefaultCoderFactory(clazz);
      if (factory == null) {
        return null;
      }

      java.util.List<Object> components = factory.getInstanceComponents(exampleValue);
      if (components == null) {
        return null;
      }

      // componentcoders = components.map(this.getDefaultCoder)
      java.util.List<Coder<?>> componentCoders = new ArrayList<>();
      for (Object component : components) {
        Coder<?> componentCoder = coderRegistry.getDefaultCoder(component);
        if (componentCoder == null) {
          return null;
        } else {
          componentCoders.add(componentCoder);
        }
      }

      // Trust that factory.create maps from valid component coders
      // to a valid Coder<T>.
      @SuppressWarnings("unchecked")
      Coder<T> coder = (Coder<T>) factory.create(componentCoders);
      return coder;
    }
  }
}
