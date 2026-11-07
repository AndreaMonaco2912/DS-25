package ds25.client

import ds25.protocol.{PeerInfo, PeerMsg}
import ds25.torrent.Torrent
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.ActorRef
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*

class PeerSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike:

  /** Resolves peers by id among actors of this test instead of over the network. */
  private val directory                                 = TrieMap.empty[String, ActorRef[PeerMsg]]
  private val resolve: PeerInfo => ActorRef[PeerMsg]    = info => directory(info.peerId)
  private def info(id: String)                          = PeerInfo(id, "localhost", 0)

  /** A 22-byte file in 4-byte pieces: 6 pieces, the last one shorter. */
  private val data              = "hello, distributed sys".getBytes
  private val (torrent, pieces) = Torrent.create("hello.txt", data, "http://tracker", pieceLength = 4)

  private def peer(id: String, have: Set[Int]) =
    val ref = spawn(Peer(id, torrent, have.map(i => i -> pieces(i)).toMap, serveDelay = 10.millis, resolve = Some(resolve)))
    directory.put(id, ref)
    ref

  "A peer" should {
    "serve the pieces it owns" in {
      val a     = peer("A1", Set(0, 1))
      val probe = createTestProbe[PeerMsg]()
      a ! PeerMsg.RequestPiece(1, probe.ref)
      val piece = probe.expectMessageType[PeerMsg.Piece]
      assert(piece.index == 1 && piece.data.sameElements(pieces(1)))
    }

    "not answer for pieces it does not own" in {
      val a     = peer("A2", Set(0))
      val probe = createTestProbe[PeerMsg]()
      a ! PeerMsg.RequestPiece(5, probe.ref)
      probe.expectNoMessage(200.millis)
    }

    "drop pieces that don't match the torrent's hashes" in {
      val a = peer("A3", Set.empty)
      a ! PeerMsg.Piece("X", 0, "evil".getBytes)
      val probe = createTestProbe[Peer.Status]()
      a ! Peer.GetStatus(probe.ref)
      assert(probe.receiveMessage().have.isEmpty)
    }

    "download the missing pieces from discovered peers and rebuild the file" in {
      val a = peer("A4", Set(0, 1, 2))
      val b = peer("B4", Set(3, 4, 5))
      a ! Peer.PeersDiscovered(List(info("B4")))

      val probe = createTestProbe[Peer.Status]()
      probe.awaitAssert({
        a ! Peer.GetStatus(probe.ref)
        assert(probe.receiveMessage().complete)
        b ! Peer.GetStatus(probe.ref)
        assert(probe.receiveMessage().complete)
      }, 5.seconds)

      val file = createTestProbe[Option[Array[Byte]]]()
      a ! Peer.GetFile(file.ref)
      assert(file.receiveMessage().exists(_.sameElements(data)))
    }

    "seed a whole file to a new peer" in {
      peer("S5", (0 until torrent.pieceCount).toSet)
      val c = peer("C5", Set.empty)
      c ! Peer.PeersDiscovered(List(info("S5")))
      val file = createTestProbe[Option[Array[Byte]]]()
      file.awaitAssert({
        c ! Peer.GetFile(file.ref)
        assert(file.receiveMessage().exists(_.sameElements(data)))
      }, 5.seconds)
    }
  }
