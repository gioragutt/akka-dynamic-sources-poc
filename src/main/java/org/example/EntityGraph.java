package org.example;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.KillSwitches;
import akka.stream.UniqueKillSwitch;
import akka.stream.contrib.SwitchMode;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.MergeHub;
import akka.stream.javadsl.Sink;
import lombok.Builder;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

class EntityGraph {
	public final Map<UUID, PauseableSourceInGraph> sources = new HashMap<>();
	private final Sink<Object, NotUsed> mergeConsumer;
	private final ActorSystem system;

	public EntityGraph(ActorSystem system, Sink<Object, CompletionStage<Done>> consumer) {
		this.system = system;
		mergeConsumer = MergeHub.of(Object.class).to(consumer).run(system);
	}

	public CompletionStage<UUID> addPausedSource(PauseableSource source) {
		return source.flipValve(new SwitchMode.Open$()).thenApply(this::addSource);
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
			source.source.flipValve(new SwitchMode.Close$())
				.whenComplete((ignored1, ignored2) -> source.broadcastKillSwitch.shutdown()));
	}

	public void killSource(UUID uuid) {
		var source = sources.remove(uuid);
		if (source == null) {
			return;
		}

		source.broadcastKillSwitch.shutdown();
		source.source.killSwitch.shutdown();
	}

	@Data
	@Builder
	static class PauseableSourceInGraph {
		/**
		 * The {@link PauseableSource} that's connected to this graph
		 */
		public PauseableSource source;

		/**
		 * The killswitch to disconnect the source from the graph.
		 * <p>
		 * Technically it is the one between the source's {@link akka.stream.javadsl.BroadcastHub} and
		 * the graph's {@link MergeHub}
		 */
		public UniqueKillSwitch broadcastKillSwitch;
	}
}
