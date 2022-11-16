package org.gingesnap.cdc.consumer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.gingesnap.cdc.CacheBackend;
import org.gingesnap.cdc.EngineWrapper;
import org.gingesnap.cdc.util.CompletionStages;
import org.gingesnap.cdc.util.Serialization;
import org.infinispan.commons.dataconversion.internal.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

public class BatchConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {
   private static final Logger log = LoggerFactory.getLogger(BatchConsumer.class);
   private final CacheBackend cache;
   private final EngineWrapper engine;

   public BatchConsumer(CacheBackend cache, EngineWrapper engine) {
      this.cache = cache;
      this.engine = engine;
   }

   @Override
   public void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> records, DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer) {
      log.info("Processing {} entries", records.size());

      try {
         for (ChangeEvent<SourceRecord, SourceRecord> ev : records) {
            // TODO: we may be able to do this in parallel later or asynchronously even
            Object maybeValue = ev.value().value();
            if (maybeValue instanceof Struct) {
               Json value = Serialization.convert((Struct) maybeValue);
               CompletionStages.join(process(value));
            }
            committer.markProcessed(ev);
         }
         committer.markBatchFinished();
      } catch (Throwable t) {
         log.info("Exception encountered writing updates for engine {}", engine.getName(), t);
         engine.notifyError();
      }
   }

   private CompletionStage<Void> process(Json value) {
      log.info("Received event...");

      Json jsonBefore = value.at("before");
      Json jsonAfter = value.at("after");

      log.info("BEFORE -> {}", jsonBefore);
      log.info("AFTER -> {}", jsonAfter);
      String op = value.at("op").asString();
      switch (op) {
         //create
         case "c":
         // update
         case "u":
         // snapshot
         case "r":
            return cache.put(jsonAfter);
         //delete
         case "d":
            return cache.remove(jsonBefore);
         default:
            log.info("Unrecognized operation [{}] for {}", value.at("op"), value);
            return CompletableFuture.completedFuture(null);
      }
   }
}
