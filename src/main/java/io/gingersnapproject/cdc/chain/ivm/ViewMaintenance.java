package io.gingersnapproject.cdc.chain.ivm;

import io.debezium.annotation.NotThreadSafe;
import org.infinispan.commons.dataconversion.internal.Json;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.gingersnapproject.cdc.chain.Event;

/**
 * Base class for view maintenance.
 *
 * <p>Implementors must identify which events they handle and the proper processing. This base class
 * holds the {@link #connection} of the in-memory database and the named {@link #query}, i. e.,
 * the view representation.</p>
 *
 * <p><h2>Thread Safety</h2></p>
 *
 * <p>This class and all its inheritors are <b>not</b> thread-safe. View management is inherently deterministic,
 * meaning we must have a single-threaded execution of all events. Assuming a single-threaded execution, we
 * don't include any synchronization mechanism.</p>
 *
 * <p><h2>Approach</h2></p>
 *
 * <p>This implementation assumes that each event causes only a single change. Meaning that after a multi-row
 * update, we receive an event per row, but since we use {@link io.debezium.engine.DebeziumEngine}, this seems
 * reasonable. The method {@link #difference(ResultSet, ResultSet)} relies on this assumption to work correctly.
 * The overall approach is pretty simple, following these steps:</p>
 *
 * <ol>
 *    <li>Load current view;</li>
 *    <li>Apply the event, a create, update, or delete;</li>
 *    <li>Load view after handling the event;</li>
 *    <li>Compare results and proceed with the operation.</li>
 * </ol>
 *
 * <p>Because of the simplicity, we can already see points of improvement in this approach. Since we assume that
 * each event represents a single change event, we can more easily identify the differences between before and after.</p>
 *
 * <p>An insert means the after may have more items; a delete means the before may have more items; an update
 * means something may change in the view. We split the handling of each operation type into multiple
 * specialized implementations.</p>
 *
 * <p><h3>Query result ordering</h3></p>
 *
 * <p>Another assumption is that of {@link ResultSet}'s ordering. The current approach also assumes that for a
 * query `X` and the set of executions `E` of `X`. For all `e_i` and `e_j` in `E`, `e_i` and `e_j` have the elements
 * in the same order.</p>
 *
 * <p>We have this assumption only to ease the way to find the {@link #difference(ResultSet, ResultSet)} between
 * the two results. We can iterate both simultaneously and break when finding a difference. The ordering with
 * the single change per event assumptions makes everything easier to handle.</p>
 *
 * @see io.gingersnapproject.cdc.chain.ViewMaintenanceLink
 * @author Jose Bolina
 */
@NotThreadSafe
public abstract class ViewMaintenance {
   protected final Connection connection;
   private final String query;

   protected ViewMaintenance(Connection connection, String query) {
      this.connection = connection;
      this.query = query;
   }

   /**
    * Handle the given {@link Event}.
    *
    * <p>Each implementation applies a different strategy. Every event changes the in-memory database
    * accordingly with the original database, creating a replica.</p>
    *
    * <p>The implementation applies the change to the database and verifies if the event should extend
    * to the {@link io.gingersnapproject.cdc.CacheBackend}. In such a case, the returned {@link Event}
    * must represent the view.</p>
    *
    * @param event: Represents the direct change from the database.
    * @return null to abort the processing. In case of an {@link Event} return, it represents the
    * view that should apply to the {@link io.gingersnapproject.cdc.CacheBackend}.
    */
   public abstract Event handle(Event event);

   /**
    * Identify if it can handle the current event. Each concrete implementation accepts only one kind of event.
    *
    * @param event: The change event.
    * @return true if accepted, false otherwise.
    */
   public abstract boolean isSupported(Event event);

   protected ResultSet load() {
      try {
         Statement statement = connection.createStatement();
         return statement.executeQuery(query);
      } catch (Exception ignore) {
         return null;
      }
   }

   protected static String tableName(Event event) {
      return String.format("`%s`", event.value().at("source").at("table").asString());
   }

   protected static Json parse(ResultSet rs) throws SQLException {
      ResultSetMetaData rsm = rs.getMetaData();
      List<String> columns = new ArrayList<>();

      for (int i = 1; i <= rsm.getColumnCount(); i++) {
         // Label return an alias or the original name.
         columns.add(rsm.getColumnLabel(i));
      }

      Json json = Json.object();
      for (String column : columns) {
         json.set(column, rs.getObject(column));
      }
      return json;
   }

   /**
    * Find the difference between two {@link ResultSet}.
    *
    * <p>This method relies on both assumptions that the results have the same order and differ by, at most,
    * a single element. With these assumptions, we break as soon we find the first difference.</p>
    *
    * <p>Since the result might differ, one can be longer than the other. The client is responsible for calling
    * with the longer result as the first argument. We are unable to know the size beforehand to assert
    * that without iterating.</p>
    *
    * @param longer: The supposed longer result set.
    * @param shorter: The supposed shorter result set.
    * @return A {@link Json} representing the difference, or null if no difference exists.
    * @throws SQLException: From the underlying {@link ResultSet}.
    */
   protected static Json difference(ResultSet longer, ResultSet shorter) throws SQLException {
      Json difference = null;
      while (longer.next() && shorter.next()) {
         Json left = parse(longer);
         Json right = parse(shorter);
         if (!left.equals(right)) {
            difference = left;
            break;
         }
      }

      // We iterate everything and didn't find any difference.
      // Maybe the longer result has a last element, which is the difference.
      if (difference == null && longer.last()) {
         difference = parse(longer);
      }

      return difference;
   }

   protected static String extractPrimaryKeyName(Event event) {
      Map<String, Json> keys = event.key().asJsonMap();
      // Only handling tables with a single unique identifiable columns.
      if (keys.size() != 1) {
         throw new IllegalArgumentException("Primary key for table not accepted");
      }

      return keys.keySet().stream().findFirst().get();
   }
}
