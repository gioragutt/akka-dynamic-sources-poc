package org.example;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.KillSwitches;
import akka.stream.UniqueKillSwitch;
import akka.stream.contrib.SwitchMode;
import akka.stream.contrib.Valve;
import akka.stream.contrib.ValveSwitch;
import akka.stream.javadsl.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import scala.jdk.javaapi.FutureConverters;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] args) throws InterruptedException {
        var system = ActorSystem.create();

        var entity1 = new EntityGraph(system, "=== entity1 ===");
        var entity2 = new EntityGraph(system, "=== entity2 ===");

        var source1 = PauseableSource.create(system, createRangeSource("1-%d"));
        var source2 = PauseableSource.create(system, createRangeSource("2-%d"));
        var source3 = PauseableSource.create(system, createRangeSource("3-%d"));

        entity1.addSource(source1);
        var uuid2 = entity1.addSource(source2);
        entity2.addSource(source3);

        Thread.sleep(5000);

        entity1.pauseSource(uuid2).ifPresent(f ->
                f.thenCompose(source -> {
                    try {
                        System.out.println("|||===== source 2 paused, waiting...");

                        Thread.sleep(5_000);

                        System.out.println("|||===== adding source 2 to entity 2, waiting...");
                        return entity2.addPausedSource(source);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                })
                .thenAccept((ignored) -> System.out.println("|||==== restarted source 2"))
        );
    }

    private static Source<Object, NotUsed> createRangeSource(String format) {
        return Source.range(0, 999)
                .map(format::formatted)
                .map(n -> (Object) n)
                .throttle(1, Duration.ofSeconds(4));
    }

    @Data
    @Builder(access = AccessLevel.PRIVATE)
    static class PauseableSource {
        public Source<Object, NotUsed> dataSource;
        public CompletionStage<ValveSwitch> valveSwitch;
        public UniqueKillSwitch killSwitch;
        public Source<Object, NotUsed> broadcastSource;

        static PauseableSource create(ActorSystem system, Source<Object, NotUsed> source) {
            var result = source
                    .map(v -> {
                        System.out.println(" ----- raw value " + v);
                        return v;
                    })
                    .viaMat(Valve.create(), Keep.right())
                    .viaMat(KillSwitches.single(), Keep.both())
                    .toMat(BroadcastHub.of(Object.class), Keep.both())
                    .run(system);


            var valveSwitchFuture = result.first().first();
            var killSwitch = result.first().second();
            var broadcastSource = result.second();

            return PauseableSource.builder()
                    .dataSource(source)
                    .valveSwitch(FutureConverters.asJava(valveSwitchFuture))
                    .killSwitch(killSwitch)
                    .broadcastSource(broadcastSource)
                    .build();
        }
    }

    @Data
    @Builder
    static class PauseableSourceInGraph {
        public PauseableSource source;
        public UniqueKillSwitch broadcastKillSwitch;
    }

    static class EntityGraph {
        public final Map<UUID, PauseableSourceInGraph> sources = new HashMap<>();
        private final Sink<Object, NotUsed> mergeConsumer;
        private final ActorSystem system;

        public EntityGraph(ActorSystem system, String name) {
            this.system = system;

            Sink<Object, CompletionStage<Done>> consumer =
                    Sink.foreach(obj -> System.out.println("Entity %s received: %s".formatted(name, obj)));

            mergeConsumer = MergeHub.of(Object.class).to(consumer).run(system);
        }

        public CompletionStage<UUID> addPausedSource(PauseableSource source) {
            return source.valveSwitch.thenApply(v -> {
                System.out.println("Flipping source to open");
                v.flip(new SwitchMode.Open$());
                return addSource(source);
            });
        }

        public UUID addSource(PauseableSource source) {
            UniqueKillSwitch broadcastKillSwitch = source.broadcastSource
                    .viaMat(KillSwitches.single(), Keep.both())
                    .toMat(mergeConsumer, Keep.left())
                    .run(system).second();

            var uuid = UUID.randomUUID();

            sources.put(uuid, PauseableSourceInGraph.builder()
                    .source(source)
                    .broadcastKillSwitch(broadcastKillSwitch)
                    .build());

            return uuid;
        }

        public Optional<CompletionStage<PauseableSource>> pauseSource(UUID uuid) {
            var source = sources.remove(uuid);

            if (source == null) {
                return Optional.empty();
            }

            return Optional.of(
                    source.source.valveSwitch.thenApply(v -> {
                        v.flip(new SwitchMode.Close$());
                        source.broadcastKillSwitch.shutdown();
                        return source.source;
                    }));
        }
    }
}