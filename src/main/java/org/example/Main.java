package org.example;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
	public static void main(String[] args) throws InterruptedException {
		var system = ActorSystem.create();

		var entity1 = new EntityGraph(system, createEntitySink("entity1"));
		var entity2 = new EntityGraph(system, createEntitySink("entity2"));

		var source1 = PauseableSource.create(system, createRangeSource("source(1)-update(%d)"));
		var source2 = PauseableSource.create(system, createRangeSource("source(2)-update(%d)"));
		var source3 = PauseableSource.create(system, createRangeSource("source(3)-update(%d)"));

		entity1.addSource(source1);
		var uuid2 = entity1.addSource(source2);
		var uuid3 = entity2.addSource(source3);

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

		Thread.sleep(15_000);

		System.out.println("|||===== stopping source 3");

		entity2.killSource(uuid3);
	}

	private static Sink<Object, CompletionStage<Done>> createEntitySink(String x) {
		return Sink.foreach(obj ->
			System.out.println("Entity === %s === received: %s".formatted(x, obj))
		);
	}

	private static Source<Object, NotUsed> createRangeSource(String format) {
		return Source.range(1, 999)
			.map(format::formatted)
			.map(n -> (Object) n)
			.throttle(1, Duration.ofSeconds(4))
			.map(v -> {
				System.out.println(" ----- raw value " + v);
				return v;
			});
	}
}