package io.gingersnapproject.cdc.consumer;

import io.gingersnapproject.cdc.chain.Event;
import io.gingersnapproject.cdc.util.Serialization;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.infinispan.commons.dataconversion.internal.Json;

public abstract class AbstractChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {

   protected Event create(ChangeEvent<SourceRecord, SourceRecord> ev) {
      Json key = create(ev.value().key());
      Json value = create(ev.value().value());
      return new Event(key, value);
   }

   protected Json create(Object object) {
      if (object instanceof Struct)
         return Serialization.convert((Struct) object);

      throw new IllegalStateException("Object is not a struct");
   }

   protected void uncheckedCommit(ChangeEvent<SourceRecord, SourceRecord> ev, DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer) {
      try {
         committer.markProcessed(ev);
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

}
