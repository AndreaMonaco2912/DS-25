package ds25.tracker

import ds25.protocol.{AnnounceResponse, PeerInfo}
import ds25.protocol.TrackerJson.given
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.util.Timeout

import scala.concurrent.duration.*
import scala.util.{Failure, Success}

/** HTTP API of a tracker, modelled on the BitTorrent tracker `announce` request:
  *
  * `GET /announce?info_hash=..&peer_id=..&ip=..&port=..[&event=started|stopped|completed]`
  *
  * Unlike real trackers `ip` is required: it must be the peer's Pekko canonical hostname, otherwise remoting
  * would drop messages sent to it.
  *
  * `GET /health` and `GET /cluster` (cluster members as seen by this tracker) are for monitoring.
  */
final class TrackerRoutes(
    trackerId: String,
    registry: ActorRef[SwarmRegistry.Command],
    announceInterval: FiniteDuration,
    clusterMembers: () => List[String] = () => Nil
)(using system: ActorSystem[?]):

  private given Timeout = Timeout(5.seconds)

  val routes: Route = concat(
    path("announce") {
      get {
        parameters("info_hash", "peer_id", "ip", "port".as[Int], "event".optional) {
          (infoHash, peerId, ip, port, event) =>
            val peer    = PeerInfo(peerId, ip, port)
            val stopped = event.contains("stopped")
            system.log.info("{} announce: {} {} in swarm {}", trackerId, peerId, event.getOrElse("update"), infoHash)
            onComplete(registry.ask[SwarmRegistry.Response](SwarmRegistry.Announce(infoHash, peer, stopped, _))) {
              case Success(SwarmRegistry.Swarm(peers)) =>
                val others = peers.filterNot(_.peerId == peerId).toList.sortBy(_.peerId)
                complete(AnnounceResponse(trackerId, announceInterval.toSeconds.toInt, others))
              case Success(SwarmRegistry.Unavailable(reason)) =>
                complete(StatusCodes.ServiceUnavailable, s"$trackerId: $reason")
              case Failure(ex) =>
                complete(StatusCodes.ServiceUnavailable, s"$trackerId: ${ex.getMessage}")
            }
        }
      }
    },
    path("health") {
      get(complete(s"OK $trackerId"))
    },
    path("cluster") {
      // This tracker's view of the tracker cluster: with a split brain the two trackers disagree.
      get(complete(s"$trackerId sees ${clusterMembers().mkString("[", ", ", "]")}"))
    }
  )
