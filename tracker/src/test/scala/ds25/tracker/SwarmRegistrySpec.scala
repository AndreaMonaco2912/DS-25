package ds25.tracker

import com.typesafe.config.ConfigFactory
import ds25.protocol.PeerInfo
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.cluster.typed.{Cluster, Join}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

/** Two tracker nodes in one cluster: an announce on one must become visible on the other. */
class SwarmRegistrySpec extends AnyWordSpec with Matchers with BeforeAndAfterAll:

  private val config = ConfigFactory.parseString(
    """
    pekko.actor.provider = cluster
    pekko.remote.artery.canonical.hostname = "127.0.0.1"
    pekko.remote.artery.canonical.port = 0
    pekko.cluster.seed-nodes = []
    pekko.cluster.jmx.multi-mbeans-in-same-jvm = on
    pekko.coordinated-shutdown.exit-jvm = off
    pekko.cluster.distributed-data.gossip-interval = 200 ms
    """
  ).withFallback(ConfigFactory.load())

  private val t1 = ActorTestKit("tracker", config)
  private val t2 = ActorTestKit("tracker", config)

  override def beforeAll(): Unit =
    val seed = Cluster(t1.system).selfMember.address
    Cluster(t1.system).manager ! Join(seed)
    Cluster(t2.system).manager ! Join(seed)
    t1.createTestProbe[Any]().awaitAssert(
      Cluster(t1.system).state.members.count(_.status.toString == "Up") shouldBe 2,
      15.seconds
    )

  override def afterAll(): Unit =
    t2.shutdownTestKit()
    t1.shutdownTestKit()

  private val peerA = PeerInfo("A", "client-a", 25520)
  private val peerB = PeerInfo("B", "client-b", 25520)

  "SwarmRegistry" should {
    "return the announcing peer in its swarm" in {
      val registry = t1.spawn(SwarmRegistry(SwarmRegistry.Consistency.Local))
      val probe    = t1.createTestProbe[SwarmRegistry.Response]()
      registry ! SwarmRegistry.Announce("hash1", peerA, stopped = false, probe.ref)
      probe.expectMessage(SwarmRegistry.Swarm(Set(peerA)))
    }

    "replicate peers between trackers" in {
      val r1 = t1.spawn(SwarmRegistry(SwarmRegistry.Consistency.Local))
      val r2 = t2.spawn(SwarmRegistry(SwarmRegistry.Consistency.Local))
      val p1 = t1.createTestProbe[SwarmRegistry.Response]()
      val p2 = t2.createTestProbe[SwarmRegistry.Response]()

      r1 ! SwarmRegistry.Announce("hash2", peerA, stopped = false, p1.ref) // A talks to T1
      p1.expectMessageType[SwarmRegistry.Swarm]

      p2.awaitAssert({
        r2 ! SwarmRegistry.Announce("hash2", peerB, stopped = false, p2.ref) // B talks to T2
        p2.expectMessageType[SwarmRegistry.Swarm].peers shouldBe Set(peerA, peerB)
      }, 10.seconds)
    }

    "work with majority consistency while both trackers are up" in {
      val r2 = t2.spawn(SwarmRegistry(SwarmRegistry.Consistency.Majority))
      val p2 = t2.createTestProbe[SwarmRegistry.Response]()
      r2 ! SwarmRegistry.Announce("hash3", peerB, stopped = false, p2.ref)
      p2.expectMessage(5.seconds, SwarmRegistry.Swarm(Set(peerB)))
    }

    "remove a peer that announces stopped" in {
      val registry = t1.spawn(SwarmRegistry(SwarmRegistry.Consistency.Majority))
      val probe    = t1.createTestProbe[SwarmRegistry.Response]()
      registry ! SwarmRegistry.Announce("hash4", peerA, stopped = false, probe.ref)
      probe.expectMessage(5.seconds, SwarmRegistry.Swarm(Set(peerA)))
      registry ! SwarmRegistry.Announce("hash4", peerA, stopped = true, probe.ref)
      probe.expectMessage(5.seconds, SwarmRegistry.Swarm(Set.empty))
    }
  }
