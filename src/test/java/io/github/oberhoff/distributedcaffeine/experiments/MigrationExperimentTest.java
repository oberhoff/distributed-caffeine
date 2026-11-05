/*
 * Copyright © 2023-2026 Dr. Andreas Oberhoff (All rights reserved)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.oberhoff.distributedcaffeine.experiments;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.StoredValue;
import io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoAdapter;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.mongodb.MongoDBContainer;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A rolling update from the current generation's release (0.7.0, as published) to this one on the same MongoDB
 * collection with {@code withSchemaReset()}: the first new instance drops the old collection while old instances
 * still run. Checks what the code of both says should happen: the old instances keep working among themselves, the
 * generations do not see each other's documents, an old instance cannot start once the new generation is there
 * (its unique index on fields new documents lack fails), and the old documents left behind are removed by one
 * command.
 * <p>
 * The release runs in a class loader of its own (both generations share packages), driven by reflection. Its
 * classpath is resolved first:
 * {@code mvn -f src/test/resources/migration/pom.xml dependency:build-classpath
 * -Dmdep.outputFile=$PWD/target/migration-0.7.0.classpath}. Not run by default, only with {@code -Dexperiments=true}:
 * {@code mvn test -Dtest=MigrationExperimentTest -Dexperiments=true}.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@EnabledIfSystemProperty(named = "experiments", matches = "true")
@SuppressWarnings("resource") // every cache and client a test opens is closed after it (@AfterEach)
class MigrationExperimentTest {

    private static final Path CLASSPATH = Path.of("target", "migration-0.7.0.classpath");
    private static final String DATABASE = "app";
    private static final String COLLECTION = "cache";
    // the documents of the old generation: a top-level discriminator, which the new one's never have
    private static final Document OLD_DOCUMENTS = new Document("discriminator", new Document("$exists", true));

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private @Nullable MongoDBContainer container;
    private @Nullable URLClassLoader oldGeneration;
    private @Nullable MongoClient client;

    @BeforeEach
    void setUp() throws Exception {
        assumeTrue(Files.exists(CLASSPATH), "the release's classpath is resolved first, see the class comment");
        List<URL> urls = new ArrayList<>();
        for (String jar : Files.readString(CLASSPATH).trim().split(Pattern.quote(File.pathSeparator), -1)) {
            urls.add(Path.of(jar).toUri().toURL());
        }
        // the platform loader as parent: nothing of this generation or its dependencies leaks into the old one
        oldGeneration = new URLClassLoader(urls.toArray(URL[]::new), ClassLoader.getPlatformClassLoader());
        container = new MongoDBContainer("mongo:8").withReplicaSet();
        container.start();
        client = MongoClients.create(container.getReplicaSetUrl());
    }

    @AfterEach
    void tearDown() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            try {
                closeables.get(i).close();
            } catch (Exception ignored) {
                // closing what may have failed already
            }
        }
        if (client != null) {
            client.close();
        }
        if (container != null) {
            container.stop();
        }
        if (oldGeneration != null) {
            oldGeneration.close();
        }
    }

    @Test
    void a_rolling_update_with_schema_reset_keeps_both_generations_working_apart() {
        OldCache oldA = oldCache();
        OldCache oldB = oldCache();
        oldA.put("k1", "old");
        await(() -> "old".equals(oldB.getIfPresent("k1"))); // the old default distributes populations
        assertThat(collection().countDocuments(OLD_DOCUMENTS)).isEqualTo(1);

        // the first new instance: drops the old collection, the old instances still running
        DistributedCache<String, String> newA = newCache();
        assertThat(collection().countDocuments(OLD_DOCUMENTS)).as("dropped").isZero();

        // the old instances go on among themselves: their change streams ended with the drop, and they resume
        oldA.put("k2", "old-2");
        await(() -> "old-2".equals(oldB.getIfPresent("k2")));
        assertThat(collection().countDocuments(OLD_DOCUMENTS)).as("written into the new collection").isEqualTo(1);

        // the new generation works, a second instance finding the marker and dropping nothing
        newA.put("k3", "new-3");
        DistributedCache<String, String> newB = newCache();
        assertThat(newB.get("k3", key -> "loaded")).isEqualTo("new-3");
        for (int i = 0; i < 10; i++) {
            newA.put("n" + i, "new");
        }
        assertThat(collection().countDocuments(OLD_DOCUMENTS)).as("the old documents survived the second new instance")
                .isEqualTo(1);

        // neither generation sees the other's writes (a 0.7.0 cache without persistence keeps only synchronization
        // records in the collection, so what an old instance sees is what it holds)
        assertThat(storedValue(newA, "k2")).as("an old document, invisible to the new generation").isNull();
        newA.put("k2", "new-2");
        oldA.put("k3", "old-3");
        await(() -> "old-3".equals(oldB.getIfPresent("k3")));
        assertThat(oldB.getIfPresent("k2")).as("not reached by the new generation's write").isEqualTo("old-2");
        assertThat(storedValue(newB, "k2")).isEqualTo("new-2");
        assertThat(storedValue(newB, "k3")).as("not reached by the old generation's write").isEqualTo("new-3");
        assertThat(newB.getIfPresent("k3")).isEqualTo("new-3");

        // after the rollout: the old documents left behind, removed by one command, the new ones untouched
        oldA.close();
        oldB.close();
        assertThat(collection().countDocuments(OLD_DOCUMENTS)).isEqualTo(2);
        long newDocuments = collection().countDocuments() - 2;
        collection().deleteMany(OLD_DOCUMENTS);
        assertThat(collection().countDocuments(OLD_DOCUMENTS)).isZero();
        assertThat(collection().countDocuments()).isEqualTo(newDocuments);
        assertThat(storedValue(newA, "k3")).isEqualTo("new-3");
        newA.put("k4", "new-4");
        assertThat(newB.get("k4", key -> "loaded")).isEqualTo("new-4");
    }

    @Test
    void an_old_instance_cannot_start_once_the_new_generation_is_there_and_breaks_nothing() {
        OldCache oldA = oldCache();
        oldA.put("k1", "old");
        DistributedCache<String, String> newA = newCache();
        System.out.println("documents of the new generation right after its first instance: "
                + collection().countDocuments(Filters.not(Filters.exists("discriminator"))));

        Throwable failure = catchThrowable(this::oldCache);
        assertThat(failure).as("an old instance starting after the reset").isNotNull();
        System.out.println("the old instance failed to start: " + failure);
        System.out.println("indexes after it: " + collection().listIndexes().into(new ArrayList<>()));

        newA.put("k2", "new-2");
        for (int i = 0; i < 10; i++) {
            newA.put("n" + i, "new");
        }
        DistributedCache<String, String> newB = newCache();
        assertThat(newB.get("k2", key -> "loaded")).isEqualTo("new-2");
        oldA.put("k3", "old-3");
        assertThat(oldA.getIfPresent("k3")).as("the running old instance is unaffected").isEqualTo("old-3");
    }

    private MongoCollection<Document> collection() {
        return requireClient().getDatabase(DATABASE).getCollection(COLLECTION);
    }

    private MongoClient requireClient() {
        if (client == null) {
            throw new IllegalStateException("not set up");
        }
        return client;
    }

    private DistributedCache<String, String> newCache() {
        DistributedCache<String, String> cache = DistributedCaffeine.<String, String>newBuilder(
                        MongoAdapter.newBuilder(requireClient(), DATABASE, COLLECTION).withSchemaReset().build())
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(1_000))
                .build();
        closeables.add(cache);
        return cache;
    }

    private static @Nullable String storedValue(DistributedCache<String, String> cache, String key) {
        StoredValue<String> stored = cache.distributedPolicy().getFromStore(key);
        return stored == null ? null : stored.value();
    }

    private OldCache oldCache() {
        if (oldGeneration == null || container == null) {
            throw new IllegalStateException("not set up");
        }
        OldCache cache = new OldCache(oldGeneration, container.getReplicaSetUrl());
        closeables.add(cache);
        return cache;
    }

    // a cache of the release, built as its README shows (a MongoDB adapter, the defaults), driven by reflection
    private static final class OldCache implements AutoCloseable {

        private final ClassLoader loader;
        private final Object client;
        private final Object cache;

        OldCache(ClassLoader loader, String uri) {
            this.loader = loader;
            this.client = requireNonNull(call(() -> {
                Class<?> clients = type("com.mongodb.client.MongoClients");
                return clients.getMethod("create", String.class).invoke(null, uri);
            }));
            Object built;
            try {
                built = call(() -> {
                    Class<?> adapterType = type("io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoAdapter");
                    Object builder = adapterType.getMethod("newBuilder", type("com.mongodb.client.MongoClient"),
                            String.class, String.class).invoke(null, client, DATABASE, COLLECTION);
                    Object adapter = invoke(builder, "build");
                    Class<?> caffeine = type("io.github.oberhoff.distributedcaffeine.DistributedCaffeine");
                    Object cacheBuilder = caffeine.getMethod("newBuilder",
                            type("io.github.oberhoff.distributedcaffeine.adapter.Adapter")).invoke(null, adapter);
                    return invoke(cacheBuilder, "build");
                });
            } catch (RuntimeException e) {
                closeClient();
                throw e;
            }
            this.cache = requireNonNull(built);
        }

        void put(String key, String value) {
            call(() -> type("com.github.benmanes.caffeine.cache.Cache").getMethod("put", Object.class, Object.class)
                    .invoke(cache, key, value));
        }

        @Nullable String getIfPresent(String key) {
            return (String) call(() -> type("com.github.benmanes.caffeine.cache.Cache")
                    .getMethod("getIfPresent", Object.class).invoke(cache, key));
        }

        @Override
        public void close() {
            try {
                call(() -> invoke(requireNonNull(invoke(cache, "distributedPolicy")), "stopSynchronization"));
            } finally {
                closeClient();
            }
        }

        private void closeClient() {
            call(() -> type("com.mongodb.client.MongoClient").getMethod("close").invoke(client));
        }

        private Class<?> type(String name) throws ClassNotFoundException {
            return Class.forName(name, true, loader);
        }

        // a public method of a public interface or class the object implements
        private static @Nullable Object invoke(Object target, String name) throws Exception {
            for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
                for (Class<?> implemented : type.getInterfaces()) {
                    try {
                        Method method = implemented.getMethod(name);
                        return method.invoke(target);
                    } catch (NoSuchMethodException ignored) {
                        // not on this interface
                    }
                }
            }
            return target.getClass().getMethod(name).invoke(target);
        }

        // with the release's loader as the thread's context loader (its serializer may load classes through it), and
        // its own exception unwrapped (checked ones wrapped, so that the calls fit into lambdas)
        private @Nullable Object call(Callable<@Nullable Object> work) {
            Thread thread = Thread.currentThread();
            ClassLoader previous = thread.getContextClassLoader();
            thread.setContextClassLoader(loader);
            try {
                return work.call();
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof RuntimeException cause) {
                    throw cause;
                }
                throw new IllegalStateException(e.getCause());
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            } finally {
                thread.setContextClassLoader(previous);
            }
        }
    }
}
