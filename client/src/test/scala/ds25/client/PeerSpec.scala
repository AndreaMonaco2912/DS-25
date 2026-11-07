package ds25.client

import ds25.protocol.{PeerInfo, PeerMsg}
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

  private def peer(id: String, hash: String, pieces: Set[Int]) =
    val ref = spawn(Peer(id, hash, totalPieces = 6, pieces, serveDelay = 10.millis, resolve = Some(resolve)))
    directory.put(id, ref)
    ref

  "A peer" should {
    "serve the pieces it owns" in {
      val a     = peer("A1", "h1", Set(0, 1))
      val probe = createTestProbe[PeerMsg]()
      a ! PeerMsg.RequestPiece(1, probe.ref)
      probe.expectMessage(PeerMsg.Piece("A1", 1, Peer.pieceData("h1", 1)))
    }

    "not answer for pieces it does not own" in {
      val a     = peer("A2", "h2", Set(0))
      val probe = createTestProbe[PeerMsg]()
      a ! PeerMsg.RequestPiece(5, probe.ref)
      probe.expectNoMessage(200.millis)
    }

    "download the missing pieces from discovered peers" in {
      val a = peer("A3", "h3", Set(0, 1, 2))
      val b = peer("B3", "h3", Set(3, 4, 5))
      a ! Peer.PeersDiscovered(List(info("B3")))

      val probe = createTestProbe[Peer.Status]()
      probe.awaitAssert({
        a ! Peer.GetStatus(probe.ref)
        probe.expectMessage(Peer.Status("A3", (0 to 5).toSet, complete = true))
        b ! Peer.GetStatus(probe.ref)
        probe.expectMessage(Peer.Status("B3", (0 to 5).toSet, complete = true))
      }, 5.seconds)
    }
  }

  "parsePieces" should {
    "accept ranges and lists" in {
      assert(ClientMain.parsePieces("0-2, 5,7") == Set(0, 1, 2, 5, 7))
      assert(ClientMain.parsePieces("") == Set.empty)
    }
  }
