package io.gingersnapproject.cdc.chain.ivm;

import org.infinispan.commons.dataconversion.internal.Json;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import io.gingersnapproject.cdc.chain.Event;

public class DeleteViewMaintenance extends ViewMaintenance {

   public DeleteViewMaintenance(Connection connection, String query) {
      super(connection, query);
   }

   @Override
   public Event handle(Event event) {
      try (ResultSet before = load()) {
         delete(event);
         if (before == null) return null;

         try (ResultSet after = load()) {
            if (after == null) return null;

            // After a deletion, the before has more elements.
            Json toDelete = difference(before, after);
            if (toDelete == null) return null;

            Json value = event.value();
            value.set("before", toDelete);
            value.set("after", null);
            return event;
         }
      } catch (SQLException ignore) { }
      return null;
   }

   @Override
   public boolean isSupported(Event event) {
      return event.value().has("op") && event.value().at("op").asString().equals("d");
   }

   private void delete(Event event) {
      Json before = event.value().at("before");
      ColumnsValues cv = ColumnsValues.extract(before);
      String pk = extractPrimaryKeyName(event);
      StringBuilder delete = new StringBuilder("delete from ")
            .append(tableName(event))
            .append(" where ")
            .append(pk).append("='")
            .append(cv.primaryKeyValue("id"))
            .append("'");

      try {
         Statement statement = connection.createStatement();
         statement.execute(delete.toString());
      } catch (SQLException e) {
         throw new RuntimeException(e);
      }
   }
}
