package io.gingersnapproject.cdc.cache;

import java.net.URI;
import java.util.concurrent.CompletionStage;

import io.gingersnapproject.cdc.CacheBackend;
import io.gingersnapproject.cdc.OffsetBackend;
import io.gingersnapproject.cdc.SchemaBackend;
import io.gingersnapproject.cdc.translation.JsonTranslator;

public interface CacheService {
   boolean supportsURI(URI uri);

   CompletionStage<Boolean> reconnect(URI uri);

   CompletionStage<Void> stop(URI uri);

   CacheBackend backendForURI(URI uri, JsonTranslator<?> keyTranslator, JsonTranslator<?> valueTranslator);

   OffsetBackend offsetBackendForURI(URI uri);

   SchemaBackend schemaBackendForURI(URI uri);

   void shutdown(URI uri);
}
