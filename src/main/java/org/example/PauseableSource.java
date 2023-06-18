package org.example;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.KillSwitches;
import akka.stream.UniqueKillSwitch;
import akka.stream.contrib.SwitchMode;
import akka.stream.contrib.Valve;
import akka.stream.contrib.ValveSwitch;
import akka.stream.javadsl.BroadcastHub;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Source;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import scala.jdk.javaapi.FutureConverters;

import java.util.concurrent.CompletionStage;

/**
 * Represents a Source which can be attached and detached from graphs while still keeping the
 * underlying source alive.
 */
@Data()
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(access = AccessLevel.PRIVATE)
class PauseableSource {
	/**
	 * The original source of data. It is meant to be kept alive, so as it will not be "resubscribed"
	 * and therefor re-start it's internal logic
	 */
	public final Source<Object, NotUsed> dataSource;

	/**
	 * The valve enables us to stop forwarding messages to the {@link BroadcastHub}, and continue
	 * after we've finished our logic, which for example can be moving the source from one graph to
	 * another.
	 */
	public final CompletionStage<ValveSwitch> valveSwitch;

	/**
	 * This killswitch is intended for when you want to indefinitely close this source and not
	 * continue receiving data from it.
	 */
	public final UniqueKillSwitch killSwitch;

	/**
	 * This source acts as the output of the entire flow, and it is the one that will be read from,
	 * and when moving between graphs, disconnected from.
	 */
	public final Source<Object, NotUsed> broadcastSource;

	static PauseableSource create(ActorSystem system, Source<Object, NotUsed> source) {
		var result = source
			.viaMat(Valve.create(), Keep.right())
			.viaMat(KillSwitches.single(), Keep.both())
			.toMat(BroadcastHub.of(Object.class), Keep.both())
			/*
			  This is what allows the source to keep running. This basically means we run a separate graph,
			  Which keeps the data source connected to the BroadcastHub,
			  So that even if the hub is not pulled (as it is a Source), it still pulls the data source.
			 */
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

	CompletionStage<PauseableSource> flipValve(SwitchMode mode) {
		/*
			It's important to note that flipping the valve is an async operation (it returns a Future),
			So we have to wait for it before we continue operating on this source.
			Technically the valveSwitch itself could be un-futured,
			But it would require dirty sync awaiting of futures.
		 */
		return valveSwitch
			.thenCompose(v -> FutureConverters.asJava(v.flip(mode)))
			.thenApply(ignore -> this);
	}
}
