package ds25.tracker

import ds25.protocol.{AnnounceResponse, PeerInfo}
import ds25.protocol.TrackerJson.given
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.scaladsl.adapter.*
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

/** HTTP layer only: the registry is replaced by a stub, no cluster involved. */
class TrackerRoutesSpec extends AnyWordSpec with Matchers with ScalatestRouteTest:

  private given typedSystem: org.apache.pekko.actor.typed.ActorSystem[Nothing] = system.toTyped

  private val peerA = PeerInfo("A", "client-a", 25520)
  private val peerB = PeerInfo("B", "client-b", 25520)

  private def stubRegistry(answer: SwarmRegistry.Response) =
    system.spawnAnonymous(Behaviors.receiveMessage[SwarmRegistry.Command] {
      case SwarmRegistry.Announce(_, _, _, replyTo) => replyTo ! answer; Behaviors.same
      case _                                        => Behaviors.same
    })

  "GET /announce" should {
    "return the other peers of the swarm, not the caller" in {
      val routes = TrackerRoutes("T1", stubRegistry(SwarmRegistry.Swarm(Set(peerA, peerB))), 7.seconds).routes
      Get("/announce?info_hash=h&peer_id=A&ip=client-a&port=25520") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        responseAs[AnnounceResponse] shouldBe AnnounceResponse("T1", 7, List(peerB))
      }
    }

    "answer 503 when the registry cannot reach the required consistency" in {
      val routes = TrackerRoutes("T2", stubRegistry(SwarmRegistry.Unavailable("no majority")), 5.seconds).routes
      Get("/announce?info_hash=h&peer_id=A&ip=client-a&port=25520") ~> routes ~> check {
        status shouldBe StatusCodes.ServiceUnavailable
      }
    }

    "reject requests with missing parameters" in {
      val routes = TrackerRoutes("T1", stubRegistry(SwarmRegistry.Swarm(Set.empty)), 5.seconds).routes
      Get("/announce?info_hash=h") ~> routes ~> check {
        handled shouldBe false
      }
    }
  }

  "GET /cluster" should {
    "list the cluster members seen by this tracker" in {
      val members = () => List("t1:Up", "t2:Up")
      val routes  = TrackerRoutes("T1", stubRegistry(SwarmRegistry.Swarm(Set.empty)), 5.seconds, members).routes
      Get("/cluster") ~> routes ~> check(responseAs[String] shouldBe "T1 sees [t1:Up, t2:Up]")
    }
  }

  "GET /health" should {
    "report the tracker id" in {
      val routes = TrackerRoutes("T1", stubRegistry(SwarmRegistry.Swarm(Set.empty)), 5.seconds).routes
      Get("/health") ~> routes ~> check(responseAs[String] shouldBe "OK T1")
    }
  }
