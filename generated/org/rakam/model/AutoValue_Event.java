package org.rakam.model;

import org.apache.avro.generic.GenericData.Record;

@javax.annotation.Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Event extends Event {
  private final String project;
  private final String collection;
  private final Record properties;

  AutoValue_Event(
      String project,
      String collection,
      Record properties) {
    if (project == null) {
      throw new NullPointerException("Null project");
    }
    this.project = project;
    if (collection == null) {
      throw new NullPointerException("Null collection");
    }
    this.collection = collection;
    if (properties == null) {
      throw new NullPointerException("Null properties");
    }
    this.properties = properties;
  }

  @Override
  public String project() {
    return project;
  }

  @Override
  public String collection() {
    return collection;
  }

  @Override
  public Record properties() {
    return properties;
  }

  @Override
  public String toString() {
    return "Event{"
        + "project=" + project
        + ", collection=" + collection
        + ", properties=" + properties
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Event) {
      Event that = (Event) o;
      return (this.project.equals(that.project()))
          && (this.collection.equals(that.collection()))
          && (this.properties.equals(that.properties()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h = 1;
    h *= 1000003;
    h ^= project.hashCode();
    h *= 1000003;
    h ^= collection.hashCode();
    h *= 1000003;
    h ^= properties.hashCode();
    return h;
  }
}
