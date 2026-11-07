package ds25.client

import ds25.protocol.{PeerAddress, PeerInfo, PeerMsg}
import ds25.protocol.PeerMsg.*
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, ActorRefResolver, Behavior}

import scala.concurrent.duration.*
import scala.util.Random

/** A peer sharing one file split into `totalPieces` pieces.
  *
  * It starts with the pieces in `initialPieces`, contacts the peers the tracker tells it about and downloads
  * the missing pieces from them, while serving its own pieces to whoever asks.
  */
object Peer:

  /** New peer list from the tracker (local message, sent by [[Announcer]]). */
  final case class PeersDiscovered(peers: List[PeerInfo])

  /** Ask for the pieces this peer currently has (local message, for tests and monitoring). */
  final case class GetStatus(replyTo: ActorRef[Status])
  final case class Status(peerId: String, have: Set[Int], complete: Boolean)

  type Command = PeerMsg | PeersDiscovered | GetStatus

  /** Default way of reaching a peer: Pekko remoting, by the actor-path naming convention. */
  def remoteResolver(context: ActorContext[?]): PeerInfo => ActorRef[PeerMsg] =
    val resolver = ActorRefResolver(context.system)
    info => resolver.resolveActorRef[PeerMsg](PeerAddress.actorPath(info.host, info.port))

  def apply(
      peerId: String,
      infoHash: String,
      totalPieces: Int,
      initialPieces: Set[Int],
      serveDelay: FiniteDuration = 300.millis,
      resolve: Option[PeerInfo => ActorRef[PeerMsg]] = None
  ): Behavior[Command] =
    Behaviors.setup { context =>
      val pieces = initialPieces.filter(i => i >= 0 && i < totalPieces).map(i => i -> pieceData(infoHash, i)).toMap
      context.log.info("Peer {} has pieces {} of {}", peerId, sorted(pieces.keySet), totalPieces)
      new Peer(context, peerId, infoHash, totalPieces, serveDelay, resolve.getOrElse(remoteResolver(context)))
        .running(pieces, known = Map.empty, requested = Set.empty)
    }

  /** Content of a piece; deterministic so that every peer agrees on the "file". */
  def pieceData(infoHash: String, index: Int): String = s"$infoHash#piece-$index"

  private def sorted(s: Set[Int]): String = s.toList.sorted.mkString("[", ",", "]")

  private final case class Remote(ref: ActorRef[PeerMsg], have: Set[Int])

private final class Peer(
    context: ActorContext[Peer.Command],
    peerId: String,
    infoHash: String,
    totalPieces: Int,
    serveDelay: FiniteDuration,
    resolve: PeerInfo => ActorRef[PeerMsg]
):
  import Peer.*

  private val self: ActorRef[PeerMsg] = context.self

  /** @param pieces    pieces we own (index -> data)
    * @param known     peers that completed a handshake with us
    * @param requested pieces asked for and not yet received
    */
  private def running(pieces: Map[Int, String], known: Map[String, Remote], requested: Set[Int]): Behavior[Command] =

    def complete = pieces.size == totalPieces

    /** Ask a random holder for every missing piece that isn't already on its way. */
    def requestMissing(known: Map[String, Remote], requested: Set[Int]): Set[Int] =
      val wanted = (0 until totalPieces).filterNot(i => pieces.contains(i) || requested.contains(i))
      val asked = wanted.flatMap { index =>
        val holders = known.collect { case (id, r) if r.have(index) => id -> r }.toVector
        Option.when(holders.nonEmpty) {
          val (holderId, holder) = holders(Random.nextInt(holders.size))
          context.log.debug("Peer {} requests piece {} from {}", peerId, index, holderId)
          holder.ref ! RequestPiece(index, self)
          index
        }
      }
      requested ++ asked

    def learned(otherId: String, ref: ActorRef[PeerMsg], have: Set[Int]): Behavior[Command] =
      if !known.contains(otherId) then context.log.info("Peer {} connected to {} (has {})", peerId, otherId, sorted(have))
      val newKnown = known.updated(otherId, Remote(ref, have))
      running(pieces, newKnown, requestMissing(newKnown, requested))

    Behaviors.receiveMessage {
      case PeersDiscovered(peers) =>
        // Handshake with every peer we're not connected to yet; repeated on each announce until it answers.
        peers.filter(p => p.peerId != peerId && !known.contains(p.peerId)).foreach { p =>
          context.log.info("Peer {} contacting {} at {}:{}", peerId, p.peerId, p.host, p.port)
          resolve(p) ! Handshake(peerId, pieces.keySet, self)
        }
        Behaviors.same

      case Handshake(otherId, have, replyTo) =>
        replyTo ! HandshakeAck(peerId, pieces.keySet, self)
        learned(otherId, replyTo, have)

      case HandshakeAck(otherId, have, replyTo) =>
        learned(otherId, replyTo, have)

      case Have(otherId, index) =>
        known.get(otherId) match
          case Some(r) =>
            val newKnown = known.updated(otherId, r.copy(have = r.have + index))
            running(pieces, newKnown, requestMissing(newKnown, requested))
          case None => Behaviors.same

      case RequestPiece(index, replyTo) =>
        // The delay only makes the exchange slow enough to follow in the logs.
        pieces.get(index).foreach(data => context.scheduleOnce(serveDelay, replyTo, Piece(peerId, index, data)))
        Behaviors.same

      case Piece(fromId, index, data) if !pieces.contains(index) =>
        val newPieces = pieces.updated(index, data)
        context.log.info("Peer {} got piece {} from {} ({}/{})", peerId, index, fromId, newPieces.size, totalPieces)
        known.values.foreach(_.ref ! Have(peerId, index))
        if newPieces.size == totalPieces then context.log.info("Peer {} DOWNLOAD COMPLETE: {}", peerId, infoHash)
        running(newPieces, known, requested - index)

      case Piece(_, _, _) =>
        Behaviors.same // duplicate

      case GetStatus(replyTo) =>
        replyTo ! Status(peerId, pieces.keySet, complete)
        Behaviors.same
    }
