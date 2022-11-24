package io.gingersnapproject.cdc.consumer;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.gingersnapproject.cdc.chain.Event;
import io.gingersnapproject.cdc.chain.EventContext;
import io.gingersnapproject.cdc.chain.EventProcessingChain;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ViewMaintenanceConsumer extends AbstractChangeConsumer  {
   private static final Logger log = LoggerFactory.getLogger(ViewMaintenanceConsumer.class);
   private final EventProcessingChain chain;

   public ViewMaintenanceConsumer(EventProcessingChain chain) {
      this.chain = chain;
   }

   @Override
   public void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> records, DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer) {
      log.info("Processing {} entries", records.size());

      try {
         // Processes events in the order they are received.
         // Trying to ensure our in-memory database is a replica, applying everything in the same
         // order as the original database.
         for (ChangeEvent<SourceRecord, SourceRecord> ce : records) {
            Event ev = create(ce);
            chain.process(ev, new EventContext());
            committer.markProcessed(ce);
         }
         committer.markBatchFinished();
      } catch (Throwable t) {
         // TODO notify of engine failure.
         log.error("Failed processing event", t);
      }
   }
}
