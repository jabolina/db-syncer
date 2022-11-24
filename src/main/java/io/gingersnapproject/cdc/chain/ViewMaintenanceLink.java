package io.gingersnapproject.cdc.chain;

import io.gingersnapproject.cdc.chain.ivm.DDLViewMaintenance;
import io.gingersnapproject.cdc.chain.ivm.DeleteViewMaintenance;
import io.gingersnapproject.cdc.chain.ivm.InsertViewMaintenance;
import io.gingersnapproject.cdc.chain.ivm.UpdateViewMaintenance;
import io.gingersnapproject.cdc.chain.ivm.ViewMaintenance;
import io.gingersnapproject.cdc.configuration.Rule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;


/**
 * Link responsible for maintaining a view, available only in query mode.
 *
 * <p>This link keeps an in-memory SQL database. Every event applies to the database. DDL events change the
 * internal structure, and data change events are carefully processed.</p>
 *
 * <p>The change event identifies which of the specialized implementations can handle the event. The maintenance
 * can return null or an E: null means abort, and E means proceed. It delegates all event processing to
 * the VM implementations.</p>
 *
 * <p>It is crucially important that this link receives most of the events. That is DDL and data changes.
 * Skipping events like charset changes <i>may</i> be ok.</p>
 *
 * @see ViewMaintenance
 * @author Jose Bolina
 */
public class ViewMaintenanceLink extends EventProcessingChain {

   private final Connection connection;
   private final List<ViewMaintenance> ivm;

   public ViewMaintenanceLink(Rule.SingleRule rule) throws SQLException {
      this.connection = DriverManager.getConnection("jdbc:h2:mem:debezium;INIT=CREATE SCHEMA IF NOT EXISTS debezium;MODE=MYSQL;NON_KEYWORDS=DATABASE;DATABASE_TO_LOWER=TRUE;");

      // TODO: query is configured
      String query = "select c.id as user_id, c.fullname, cm.model from customer c, car_model cm where c.id = cm.owner";
      this.ivm = List.of(
            new DDLViewMaintenance(connection),
            new InsertViewMaintenance(connection, query),
            new UpdateViewMaintenance(connection, query),
            new DeleteViewMaintenance(connection, query)
      );
   }

   @Override
   public boolean process(Event event, EventContext ctx) {
      for (ViewMaintenance management : ivm) {
         if (management.isSupported(event)) {
            Event next = management.handle(event);
            if (next != null) return processNext(next, ctx);
         }
      }
      return false;
   }
}
