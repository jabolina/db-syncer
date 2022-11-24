package io.gingersnapproject.cdc.chain.ivm;

import org.infinispan.commons.dataconversion.internal.Json;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import io.gingersnapproject.cdc.chain.Event;

public class InsertViewMaintenance extends ViewMaintenance {

   public InsertViewMaintenance(Connection connection, String query) {
      super(connection, query);
   }

   @Override
   public Event handle(Event event) {
      try (ResultSet before = load()) {
         insert(event);
         if (before == null) return null;

         try (ResultSet after = load()) {
            if (after == null) return null;

            // After an insertion, the after has more elements.
            Json toInsert = difference(after, before);
            if (toInsert == null) return null;

            event.value().set("after", toInsert);
            return event;
         }
      } catch (SQLException ignore) { }
      return null;
   }

   @Override
   public boolean isSupported(Event event) {
      if (!event.value().has("op")) return false;
      String op = event.value().at("op").asString();
      return op.equals("c") || op.equals("r");
   }

   private void insert(Event event) {
      ColumnsValues cv = ColumnsValues.extract(event.value().at("after"));
      StringBuilder insert = new StringBuilder("insert into ")
            .append(tableName(event))
            .append(cv.formatColumns())
            .append(" values ")
            .append(cv.formatValues());

      try {
         Statement statement = connection.createStatement();
         statement.execute(insert.toString());
      } catch (SQLException e) {
         throw new RuntimeException(e);
      }
   }
}
