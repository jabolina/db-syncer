package org.gingesnap.cdc.util;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.infinispan.commons.dataconversion.internal.Json;

public final class Serialization {
   private Serialization() { }

   public static Json convert(Struct struct) {
      Json json = Json.object();
      convert(struct, json);
      return json;
   }

   private static void convert(Struct struct, Json json) {
      for (Field field : struct.schema().fields()) {
         Object value = struct.get(field);
         if (value != null) {
            if (value instanceof Struct) json.set(field.name(), convert((Struct) value));
            else json.set(field.name(), value);
         }
      }
   }
}
