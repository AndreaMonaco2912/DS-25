package ds25.client

import ds25.protocol.{AnnounceResponse, PeerInfo}
import ds25.protocol.TrackerJson.given
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{HttpRequest, StatusCodes, Uri}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal

import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.{Failure, Success}

/** Periodically announces this peer to the tracker endpoint and forwards the peer list to [[Peer]].
 *
 * The client only knows one URL; behind it a load balancer picks T1 or T2, which is visible in the
 * `trackerId` of each response.
 */
object Announcer:

  //Enum?
  sealed trait Command
  private case object Tick extends Command
  private final case class Announced(response: AnnounceResponse) extends Command
  private final case class AnnounceFailed(error: Throwable) extends Command

  final case class Config(trackerUrl: String, infoHash: String, self: PeerInfo)

  private val RetryDelay = 2.seconds

  def apply(config: Config, peer: ActorRef[Peer.PeersDiscovered]): Behavior[Command] =
    Behaviors.withTimers { timers =>
      Behaviors.setup { context =>
        given system: ActorSystem[Nothing] = context.system

        def announce(event: Option[String]): Unit =
          context.pipeToSelf(Announcer.announce(config, event)) {
            case Success(r) => Announced(r)
            case Failure(ex) => AnnounceFailed(ex)
          }

        def behavior(started: Boolean): Behavior[Command] = Behaviors.receiveMessage {
          case Tick =>
            announce(if started then None else Some("started"))
            Behaviors.same

          case Announced(r) =>
            context.log.info(
              "Announced to {} -> peers {}",
              r.trackerId,
              r.peers.map(_.peerId).mkString("[", ",", "]")
            )
            peer ! Peer.PeersDiscovered(r.peers)
            timers.startSingleTimer(Tick, r.interval.seconds)
            behavior(started = true)

          case AnnounceFailed(ex) =>
            context.log.warn("Announce to {} failed: {}", config.trackerUrl, ex.getMessage)
            timers.startSingleTimer(Tick, RetryDelay)
            Behaviors.same
        }

        context.self ! Tick
        behavior(started = false)
      }
    }

  /** One `GET /announce` request. */
  def announce(config: Config, event: Option[String])(using system: ActorSystem[?]): Future[AnnounceResponse] =
    given scala.concurrent.ExecutionContext = system.executionContext

    val query = Uri.Query(
      Map(
        "info_hash" -> config.infoHash,
        "peer_id" -> config.self.peerId,
        "ip" -> config.self.host,
        "port" -> config.self.port.toString
      ) ++ event.map("event" -> _)
    )
    val uri = Uri(config.trackerUrl.stripSuffix("/") + "/announce").withQuery(query)
    Http().singleRequest(HttpRequest(uri = uri)).flatMap { response =>
      if response.status == StatusCodes.OK then Unmarshal(response.entity).to[AnnounceResponse]
      else
        Unmarshal(response.entity).to[String].flatMap { body =>
          Future.failed(RuntimeException(s"${response.status}: $body"))
        }
    }

  /** Tell the tracker we're leaving (used on shutdown). */
  def announceStopped(config: Config)(using system: ActorSystem[?]): Future[Done] =
    given scala.concurrent.ExecutionContext = system.executionContext

    announce(config, Some("stopped")).map(_ => Done).recover { case _ => Done }
