package ds25.client

import ds25.protocol.{PeerAddress, PeerInfo}
import ds25.torrent.TorrentFormat
import org.apache.pekko.Done
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.util.Timeout

import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.{Failure, Success}

/** BitTorrent-like client with a web GUI. Configuration (environment variables):
 *  - `PEER_ID`      peer id (default: canonical hostname)
 *  - `TRACKER_URL`  the tracker endpoint written in the `.tor` files this client creates (default http://localhost:8080)
 *  - `HTTP_PORT`    port of the web GUI (default 8080)
 *  - `PIECE_DELAY_MS` artificial delay when serving a piece (default 300)
 *  - `CANONICAL_HOST`/`CANONICAL_PORT` Pekko remoting address, see `application.conf`
 */
object ClientMain:

  def main(args: Array[String]): Unit =
    ActorSystem[Nothing](guardian(), PeerAddress.SystemName)

  private def guardian(): Behavior[Nothing] =
    Behaviors.setup[Nothing] { context =>
      given system: ActorSystem[Nothing] = context.system
      given scala.concurrent.ExecutionContext = system.executionContext

      val config = system.settings.config

      val host = config.getString("pekko.remote.artery.canonical.hostname")
      val port = config.getInt("pekko.remote.artery.canonical.port")
      val peerId = sys.env.getOrElse("PEER_ID", host)
      val trackerUrl = sys.env.getOrElse("TRACKER_URL", "http://localhost:8080")
      val httpPort = sys.env.get("HTTP_PORT").map(_.toInt).getOrElse(8080)
      val delay = sys.env.get("PIECE_DELAY_MS").map(_.toInt.millis).getOrElse(300.millis)

      val self = PeerInfo(peerId, host, port)
      // Spawned under the guardian as /user/torrents, the path other peers resolve (PeerAddress.actorPath).
      val client = context.spawn(Client(Client.Settings(self, delay)), PeerAddress.ActorName)
      val routes = WebUi(peerId, trackerUrl, client, TorrentFormat.default).routes

      Http().newServerAt("0.0.0.0", httpPort).bind(routes).onComplete {
        case Success(binding) => system.log.info("Client {} GUI on {}", peerId, binding.localAddress)
        case Failure(ex) =>
          system.log.error("Client {} failed to bind HTTP port {}", peerId, httpPort, ex)
          system.terminate()
      }

      CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, "announce-stopped") { () =>
        given Timeout = Timeout(2.seconds)
        client.ask[List[Client.Entry]](Client.GetTorrents(_)).flatMap { entries =>
          Future.traverse(entries) { e =>
            Announcer.announceStopped(Announcer.Config(e.torrent.announce, e.torrent.infoHash, self))
          }
        }.map(_ => Done).recover { case _ => Done }
      }

      Behaviors.empty
    }
