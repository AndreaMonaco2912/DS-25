package ds25.tracker

import ds25.protocol.PeerInfo
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.ddata.typed.scaladsl.{DistributedData, Replicator}
import org.apache.pekko.cluster.ddata.{ORMultiMap, ORMultiMapKey, SelfUniqueAddress}

import scala.concurrent.duration.*

/** Swarm registry shared by all trackers: infoHash -> peers in that swarm.
  *
  * The state is an `ORMultiMap` CRDT replicated by Pekko Distributed Data, so a peer that announced to T1
  * is also returned by T2. The consistency level decides what happens while trackers are disconnected:
  *  - `Local`:    reads/writes succeed on the local replica, others learn by gossip (AP, eventually consistent)
  *  - `Majority`: reads/writes need a majority of trackers (with 2 trackers: both) (CP, may fail)
  */
object SwarmRegistry:

  enum Consistency:
    case Local, Majority

  object Consistency:
    def parse(s: String): Consistency =
      if s.equalsIgnoreCase("majority") then Majority else Local

  sealed trait Command

  /** Add (or, when `stopped`, remove) `peer` in swarm `infoHash`, then answer with the current swarm. */
  final case class Announce(infoHash: String, peer: PeerInfo, stopped: Boolean, replyTo: ActorRef[Response])
      extends Command

  sealed trait Response
  final case class Swarm(peers: Set[PeerInfo]) extends Response
  final case class Unavailable(reason: String) extends Response

  private type Swarms = ORMultiMap[String, PeerInfo]
  val SwarmsKey: ORMultiMapKey[String, PeerInfo] = ORMultiMapKey("swarms")

  private final case class Updated(
      rsp: Replicator.UpdateResponse[Swarms],
      infoHash: String,
      replyTo: ActorRef[Response]
  ) extends Command

  private final case class Fetched(
      rsp: Replicator.GetResponse[Swarms],
      infoHash: String,
      replyTo: ActorRef[Response]
  ) extends Command

  def apply(consistency: Consistency, timeout: FiniteDuration = 3.seconds): Behavior[Command] =
    Behaviors.setup { context =>
      given SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress

      val (writeConsistency, readConsistency) = consistency match
        case Consistency.Local    => (Replicator.WriteLocal, Replicator.ReadLocal)
        case Consistency.Majority => (Replicator.WriteMajority(timeout), Replicator.ReadMajority(timeout))

      context.log.info("Swarm registry started with {} consistency", consistency)

      DistributedData.withReplicatorMessageAdapter[Command, Swarms] { replicator =>
        Behaviors.receiveMessage {
          case Announce(infoHash, peer, stopped, replyTo) =>
            replicator.askUpdate(
              askReplyTo =>
                Replicator.Update(SwarmsKey, ORMultiMap.empty[String, PeerInfo], writeConsistency, askReplyTo) {
                  swarms =>
                    if stopped then swarms.removeBindingBy(infoHash, peer)
                    else swarms.addBindingBy(infoHash, peer)
                },
              rsp => Updated(rsp, infoHash, replyTo)
            )
            Behaviors.same

          case Updated(_: Replicator.UpdateSuccess[?], infoHash, replyTo) =>
            replicator.askGet(
              askReplyTo => Replicator.Get(SwarmsKey, readConsistency, askReplyTo),
              rsp => Fetched(rsp, infoHash, replyTo)
            )
            Behaviors.same

          case Updated(failure, _, replyTo) =>
            context.log.warn("Swarm update failed: {}", failure)
            replyTo ! Unavailable(s"write did not reach $consistency consistency")
            Behaviors.same

          case Fetched(rsp @ Replicator.GetSuccess(SwarmsKey), infoHash, replyTo) =>
            replyTo ! Swarm(rsp.get(SwarmsKey).get(infoHash).getOrElse(Set.empty))
            Behaviors.same

          case Fetched(_: Replicator.NotFound[?], _, replyTo) =>
            replyTo ! Swarm(Set.empty)
            Behaviors.same

          case Fetched(failure, _, replyTo) =>
            context.log.warn("Swarm read failed: {}", failure)
            replyTo ! Unavailable(s"read did not reach $consistency consistency")
            Behaviors.same
        }
      }
    }
