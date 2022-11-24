package io.gingersnapproject.cdc.chain.ivm;

import org.infinispan.commons.dataconversion.internal.Json;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public record ColumnsValues(List<String> columns, List<String> values) implements Iterable<KeyValue> {

   @Override
   public Iterator<KeyValue> iterator() {
      List<KeyValue> kv = new ArrayList<>();
      for (int i = 0; i < columns.size(); i++) {
         String column = columns.get(i);
         String value = values.get(i);
         kv.add(new KeyValue(column, value));
      }
      return kv.iterator();
   }

   public String formatColumns() {
      return String.format("(%s)", String.join(",", columns));
   }

   public String formatValues() {
      return String.format("('%s')", String.join("','", values));
   }

   public String primaryKeyValue(String column) {
      for (int i = 0; i < columns.size(); i++) {
         if (column.equals(columns.get(i))) {
            return values.get(i);
         }
      }

      throw new RuntimeException("Primary key colum " + column + " not found!");
   }

   public static ColumnsValues extract(Json value) {
      List<String> columns = new ArrayList<>();
      List<String> values = new ArrayList<>();
      for (Map.Entry<String, Json> entry : value.asJsonMap().entrySet()) {
         columns.add(entry.getKey());
         values.add(entry.getValue().asString());
      }

      return new ColumnsValues(columns, values);
   }
}
