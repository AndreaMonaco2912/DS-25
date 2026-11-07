package ds25.protocol

import org.apache.pekko.actor.typed.ActorRef

/** Peer-to-peer protocol, a toy version of the BitTorrent peer wire protocol.
 * A file is split into numbered pieces; peers exchange which pieces they have and download missing ones.
 * Every message is about one torrent: the one of the actor it is sent to (see [[PeerAddress]]).
 */
sealed trait PeerMsg extends CborSerializable

object PeerMsg:
  /** First message to a newly discovered peer: who I am and which pieces I have. */
  final case class Handshake(peerId: String, have: Set[Int], replyTo: ActorRef[PeerMsg]) extends PeerMsg

  /** Answer to a [[Handshake]]. */
  final case class HandshakeAck(peerId: String, have: Set[Int], replyTo: ActorRef[PeerMsg]) extends PeerMsg

  /** Broadcast to known peers after a piece has been downloaded. */
  final case class Have(peerId: String, index: Int) extends PeerMsg

  final case class RequestPiece(index: Int, replyTo: ActorRef[PeerMsg]) extends PeerMsg

  final case class Piece(peerId: String, index: Int, data: Array[Byte]) extends PeerMsg

/** By convention every client runs an actor system called [[SystemName]], with one actor per torrent at
 * `/user/[[ActorName]]/<infoHash>`, so an address handed out by the tracker is enough to reach the peer.
 */
object PeerAddress:
  val SystemName = "client"
  val ActorName = "torrents"

  def actorPath(host: String, port: Int, infoHash: String): String =
    s"pekko://$SystemName@$host:$port/user/$ActorName/$infoHash"
