package io.gingersnapproject.cdc.chain.ivm;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import io.gingersnapproject.cdc.chain.Event;

public class DDLViewMaintenance extends ViewMaintenance {
   private final Set<String> previousDdl;

   public DDLViewMaintenance(Connection connection) {
      super(connection, null);
      this.previousDdl = new HashSet<>();
   }

   @Override
   public Event handle(Event event) {
      String ddl = event.value().at("ddl").asString();
      boolean failed = false;

      try {
         execute(ddl);
      } catch (SQLException e) {
         failed = true;
      }

      try {
         Iterator<String> it = previousDdl.iterator();
         while (it.hasNext()) {
            execute(it.next());
            it.remove();
         }
      } catch (SQLException ignore) {}

      if (failed) previousDdl.add(ddl);
      return null;
   }

   private void execute(String ddl) throws SQLException {
      Statement statement = connection.createStatement();
      statement.execute(ddl);
   }

   @Override
   public boolean isSupported(Event event) {
      return event.value().has("ddl") && event.value().at("ddl") != null;
   }
}
