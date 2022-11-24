package io.gingersnapproject.cdc.chain.ivm;

import org.infinispan.commons.dataconversion.internal.Json;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import io.gingersnapproject.cdc.chain.Event;

public class UpdateViewMaintenance extends ViewMaintenance {

   public UpdateViewMaintenance(Connection connection, String query) {
      super(connection, query);
   }

   @Override
   public Event handle(Event event) {
      try (ResultSet before = load()) {
         update(event);
         if (before == null) return null;

         try (ResultSet after = load()) {
            if (after == null) return null;

            while (before.next() && after.next()) {
               Json b = parse(before);
               Json a = parse(after);
               if (!b.equals(a)) {
                  Json value = event.value();
                  value.set("before", b);
                  value.set("after", a);
                  return event;
               }
            }
         }
      } catch (SQLException ignore) { }
      return null;
   }

   @Override
   public boolean isSupported(Event event) {
      return event.value().has("op") && event.value().at("op").asString().equals("u");
   }

   private void update(Event event) {
      ColumnsValues cv = ColumnsValues.extract(event.value().at("after"));
      String pk = extractPrimaryKeyName(event);
      StringBuilder update = new StringBuilder("update ")
            .append(tableName(event))
            .append(" set ");

      List<String> sets = new ArrayList<>();
      for (KeyValue kv : cv) {
         if (kv.key().equals(pk)) continue;
         sets.add(String.format("%s='%s' ", kv.key(), kv.value()));
      }

      update.append(String.join(",", sets));
      update.append(" where ")
            .append(pk).append("='").append(cv.primaryKeyValue(pk)).append("'");

      try {
         Statement statement = connection.createStatement();
         statement.execute(update.toString());
      } catch (SQLException e) {
         throw new RuntimeException(e);
      }
   }
}
