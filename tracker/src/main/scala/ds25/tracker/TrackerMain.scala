package ds25.tracker

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.cluster.ClusterEvent
import org.apache.pekko.cluster.typed.{Cluster, Subscribe}
import org.apache.pekko.http.scaladsl.Http

import scala.concurrent.duration.*
import scala.util.{Failure, Success}

/** Tracker node. Configuration (environment variables):
  *  - `TRACKER_ID`    name shown in announce responses (default: canonical hostname)
  *  - `HTTP_PORT`     HTTP API port (default 8080)
  *  - `CONSISTENCY`   `local` (default) or `majority`, see [[SwarmRegistry]]
  *  - `ANNOUNCE_INTERVAL` seconds clients should wait between announces (default 5)
  *  - cluster/remoting settings: see `application.conf`
  */
object TrackerMain:

  def main(args: Array[String]): Unit =
    ActorSystem[Nothing](guardian(), "tracker")

  private def guardian(): Behavior[Nothing] =
    Behaviors.setup[Nothing] { context =>
      given system: ActorSystem[Nothing] = context.system
      val config                         = system.settings.config

      val trackerId   = sys.env.getOrElse("TRACKER_ID", config.getString("pekko.remote.artery.canonical.hostname"))
      val httpPort    = sys.env.get("HTTP_PORT").map(_.toInt).getOrElse(8080)
      val consistency = SwarmRegistry.Consistency.parse(sys.env.getOrElse("CONSISTENCY", "local"))
      val interval    = sys.env.get("ANNOUNCE_INTERVAL").map(_.toInt.seconds).getOrElse(5.seconds)

      context.spawn(clusterListener(), "cluster-listener")
      val registry = context.spawn(SwarmRegistry(consistency), "swarm-registry")
      val cluster  = Cluster(system)
      val members  = () => cluster.state.members.toList.map(m => s"${m.address.host.getOrElse("?")}:${m.status}")
      val routes   = TrackerRoutes(trackerId, registry, interval, members).routes

      Http().newServerAt("0.0.0.0", httpPort).bind(routes).onComplete {
        case Success(binding) => system.log.info("Tracker {} listening on {}", trackerId, binding.localAddress)
        case Failure(ex) =>
          system.log.error("Tracker {} failed to bind HTTP port {}", trackerId, httpPort, ex)
          system.terminate()
      }(using system.executionContext)

      Behaviors.empty
    }

  /** Logs cluster membership changes, handy to watch T1/T2 find each other. */
  private def clusterListener(): Behavior[ClusterEvent.ClusterDomainEvent] =
    Behaviors.setup { context =>
      Cluster(context.system).subscriptions ! Subscribe(context.self, classOf[ClusterEvent.ClusterDomainEvent])
      Behaviors.receiveMessage {
        case ClusterEvent.MemberUp(m)              => context.log.info("Tracker joined: {}", m.address); Behaviors.same
        case ClusterEvent.MemberRemoved(m, _)      => context.log.info("Tracker removed: {}", m.address); Behaviors.same
        case ClusterEvent.UnreachableMember(m)     => context.log.warn("Tracker unreachable: {}", m.address); Behaviors.same
        case ClusterEvent.ReachableMember(m)       => context.log.info("Tracker reachable again: {}", m.address); Behaviors.same
        case _                                     => Behaviors.same
      }
    }
