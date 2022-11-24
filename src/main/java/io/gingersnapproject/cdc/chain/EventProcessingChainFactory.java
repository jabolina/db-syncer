package io.gingersnapproject.cdc.chain;

import java.sql.SQLException;

import io.gingersnapproject.cdc.CacheBackend;
import io.gingersnapproject.cdc.configuration.Rule;

public class EventProcessingChainFactory {

   public static EventProcessingChain create(Rule.SingleRule rule, CacheBackend backend) {
      // The filter link should always be the head.
      EventProcessingChain chain = EventProcessingChain.chained(new EventFilterLink(rule));

      if (isUsingQuery(rule)) {
         try {
            chain.add(new ViewMaintenanceLink(rule));
         } catch (SQLException e) {
            throw new RuntimeException(e);
         }
      }

      // The cache link should always be last.
      chain.add(new CacheBackendLink(backend));
      return chain;
   }

   // TODO: verify if using query.
   private static boolean isUsingQuery(Rule.SingleRule ignore) {
      return true;
   }
}
