package ds25.client

import ds25.protocol.{PeerAddress, PeerInfo}
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorSystem, Behavior}

import scala.concurrent.duration.*

/** BitTorrent-like client. Configuration (environment variables):
 *  - `PEER_ID`      peer id (default: canonical hostname)
 *  - `TRACKER_URL`  the single tracker endpoint (default http://localhost:8080)
 *  - `INFO_HASH`    id of the shared file / swarm (default `demo-file`)
 *  - `TOTAL_PIECES` number of pieces of the file (default 8)
 *  - `PIECES`       pieces owned at start, e.g. `0-3` or `0,2,4` (default none)
 *  - `PIECE_DELAY_MS` artificial delay when serving a piece (default 300)
 *  - `CANONICAL_HOST`/`CANONICAL_PORT` Pekko remoting address, see `application.conf`
 */
object ClientMain:

  def main(args: Array[String]): Unit =
    ActorSystem[Nothing](guardian(), PeerAddress.SystemName)

  def parsePieces(spec: String): Set[Int] =
    spec.split(',').map(_.trim).filter(_.nonEmpty).flatMap { part =>
      part.split('-') match
        case Array(from, to) => from.trim.toInt to to.trim.toInt
        case Array(single) => Seq(single.toInt)
        case _ => throw IllegalArgumentException(s"bad piece spec: $part")
    }.toSet

  private def guardian(): Behavior[Nothing] =
    Behaviors.setup[Nothing] { context =>
      given system: ActorSystem[Nothing] = context.system

      val config = system.settings.config

      val host = config.getString("pekko.remote.artery.canonical.hostname")
      val port = config.getInt("pekko.remote.artery.canonical.port")
      val peerId = sys.env.getOrElse("PEER_ID", host)
      val trackerUrl = sys.env.getOrElse("TRACKER_URL", "http://localhost:8080")
      val infoHash = sys.env.getOrElse("INFO_HASH", "demo-file")
      val totalPieces = sys.env.get("TOTAL_PIECES").map(_.toInt).getOrElse(8)
      val owned = parsePieces(sys.env.getOrElse("PIECES", ""))
      val delay = sys.env.get("PIECE_DELAY_MS").map(_.toInt.millis).getOrElse(300.millis)

      val announcerConfig = Announcer.Config(trackerUrl, infoHash, PeerInfo(peerId, host, port))

      // Spawned under the guardian as /user/peer, the path other peers resolve (PeerAddress.actorPath).
      val peer = context.spawn(Peer(peerId, infoHash, totalPieces, owned, delay), PeerAddress.ActorName)
      context.spawn(Announcer(announcerConfig, peer), "announcer")

      CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, "announce-stopped") { () =>
        Announcer.announceStopped(announcerConfig)
      }

      Behaviors.empty
    }
