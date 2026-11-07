package ds25.protocol

import org.apache.pekko.actor.typed.ActorRef

/** Peer-to-peer protocol, a toy version of the BitTorrent peer wire protocol.
 * A file is split into numbered pieces; peers exchange which pieces they have and download missing ones.
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

  final case class Piece(peerId: String, index: Int, data: String) extends PeerMsg

/** By convention every client runs an actor system called [[SystemName]] with its peer actor at [[ActorName]],
 * so an address handed out by the tracker is enough to reach the peer.
 */
object PeerAddress:
  val SystemName = "client"
  val ActorName = "peer"

  def actorPath(host: String, port: Int): String = s"pekko://$SystemName@$host:$port/user/$ActorName"
