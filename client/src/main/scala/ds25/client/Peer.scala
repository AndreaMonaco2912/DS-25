package ds25.client

import ds25.protocol.{PeerAddress, PeerInfo, PeerMsg}
import ds25.protocol.PeerMsg.*
import ds25.torrent.Torrent
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, ActorRefResolver, Behavior}

import scala.concurrent.duration.*
import scala.util.Random

/** This client's part in the swarm of one [[Torrent]].
  *
  * It starts with `initialPieces` (all of them when seeding, none when downloading), contacts the peers the
  * tracker tells it about and downloads the missing pieces from them, while serving its own pieces to whoever
  * asks. Received pieces are checked against the hashes in the torrent.
  */
object Peer:

  /** New peer list from the tracker (local message, sent by [[Announcer]]). */
  final case class PeersDiscovered(peers: List[PeerInfo])

  /** Progress of this torrent (local message, for the GUI and tests). */
  final case class GetStatus(replyTo: ActorRef[Status])
  final case class Status(torrent: Torrent, have: Set[Int], peers: Set[String]):
    def complete: Boolean = have.size == torrent.pieceCount

  /** The whole file, once every piece is here (local message). */
  final case class GetFile(replyTo: ActorRef[Option[Array[Byte]]])

  type Command = PeerMsg | PeersDiscovered | GetStatus | GetFile

  /** Pieces requested at the same time, so that big files are fetched gradually and from several peers. */
  val MaxInFlight = 4

  /** Default way of reaching a peer: Pekko remoting, by the actor-path naming convention. */
  def remoteResolver(context: ActorContext[?], infoHash: String): PeerInfo => ActorRef[PeerMsg] =
    val resolver = ActorRefResolver(context.system)
    info => resolver.resolveActorRef[PeerMsg](PeerAddress.actorPath(info.host, info.port, infoHash))

  def apply(
      peerId: String,
      torrent: Torrent,
      initialPieces: Map[Int, Array[Byte]],
      serveDelay: FiniteDuration = 300.millis,
      resolve: Option[PeerInfo => ActorRef[PeerMsg]] = None
  ): Behavior[Command] =
    Behaviors.setup { context =>
      context.log.info("Peer {} has {}/{} pieces of {}", peerId, initialPieces.size, torrent.pieceCount, torrent.name)
      val peer = new Peer(context, peerId, torrent, serveDelay, resolve.getOrElse(remoteResolver(context, torrent.infoHash)))
      peer.running(initialPieces, known = Map.empty, requested = Set.empty)
    }

  private final case class Remote(ref: ActorRef[PeerMsg], have: Set[Int])

private final class Peer(
    context: ActorContext[Peer.Command],
    peerId: String,
    torrent: Torrent,
    serveDelay: FiniteDuration,
    resolve: PeerInfo => ActorRef[PeerMsg]
):
  import Peer.*

  private val self: ActorRef[PeerMsg] = context.self

  /** Ask a random holder for missing pieces that aren't already on their way, up to [[MaxInFlight]]. */
  private def requestMissing(pieces: Map[Int, Array[Byte]], known: Map[String, Remote], requested: Set[Int]): Set[Int] =
    val asked = (0 until torrent.pieceCount).iterator
      .filterNot(i => pieces.contains(i) || requested.contains(i))
      .flatMap { index =>
        val holders = known.collect { case (id, r) if r.have(index) => id -> r }.toVector
        Option.when(holders.nonEmpty)(index -> holders(Random.nextInt(holders.size)))
      }
      .take(MaxInFlight - requested.size)
      .map { case (index, (holderId, holder)) =>
        context.log.debug("Peer {} requests piece {} from {}", peerId, index, holderId)
        holder.ref ! RequestPiece(index, self)
        index
      }
      .toSet
    requested ++ asked

  /** @param pieces    pieces we own (index -> data)
    * @param known     peers that completed a handshake with us
    * @param requested pieces asked for and not yet received
    */
  private def running(pieces: Map[Int, Array[Byte]], known: Map[String, Remote], requested: Set[Int]): Behavior[Command] =

    def learned(otherId: String, ref: ActorRef[PeerMsg], have: Set[Int]): Behavior[Command] =
      if !known.contains(otherId) then context.log.info("Peer {} connected to {} ({} pieces)", peerId, otherId, have.size)
      val newKnown = known.updated(otherId, Remote(ref, have))
      running(pieces, newKnown, requestMissing(pieces, newKnown, requested))

    Behaviors.receiveMessage {
      case PeersDiscovered(peers) =>
        // Handshake with every peer we're not connected to yet; repeated on each announce until it answers.
        peers.filter(p => p.peerId != peerId && !known.contains(p.peerId)).foreach { p =>
          context.log.info("Peer {} contacting {} at {}:{}", peerId, p.peerId, p.host, p.port)
          resolve(p) ! Handshake(peerId, pieces.keySet, self)
        }
        // Requests sent to a peer that has since died are never answered: ask again, duplicates are ignored.
        running(pieces, known, requestMissing(pieces, known, Set.empty))

      case Handshake(otherId, have, replyTo) =>
        replyTo ! HandshakeAck(peerId, pieces.keySet, self)
        learned(otherId, replyTo, have)

      case HandshakeAck(otherId, have, replyTo) =>
        learned(otherId, replyTo, have)

      case Have(otherId, index) =>
        known.get(otherId) match
          case Some(r) =>
            val newKnown = known.updated(otherId, r.copy(have = r.have + index))
            running(pieces, newKnown, requestMissing(pieces, newKnown, requested))
          case None => Behaviors.same

      case RequestPiece(index, replyTo) =>
        // The delay only makes the exchange slow enough to follow in the GUI and the logs.
        pieces.get(index).foreach(data => context.scheduleOnce(serveDelay, replyTo, Piece(peerId, index, data)))
        Behaviors.same

      case Piece(fromId, index, data) if !pieces.contains(index) =>
        if !torrent.isValidPiece(index, data) then
          context.log.warn("Peer {} got a corrupt piece {} from {}, dropped", peerId, index, fromId)
          running(pieces, known, requestMissing(pieces, known, requested - index))
        else
          val newPieces = pieces.updated(index, data)
          context.log.info("Peer {} got piece {} from {} ({}/{})", peerId, index, fromId, newPieces.size, torrent.pieceCount)
          known.values.foreach(_.ref ! Have(peerId, index))
          if newPieces.size == torrent.pieceCount then
            context.log.info("Peer {} DOWNLOAD COMPLETE: {}", peerId, torrent.name)
          running(newPieces, known, requestMissing(newPieces, known, requested - index))

      case Piece(_, _, _) =>
        Behaviors.same // duplicate

      case GetStatus(replyTo) =>
        replyTo ! Status(torrent, pieces.keySet, known.keySet)
        Behaviors.same

      case GetFile(replyTo) =>
        val complete = pieces.size == torrent.pieceCount
        replyTo ! Option.when(complete)((0 until torrent.pieceCount).toArray.flatMap(pieces))
        Behaviors.same
    }
