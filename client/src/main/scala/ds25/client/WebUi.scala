package ds25.client

import ds25.torrent.{Torrent, TorrentFormat}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.pekko.http.scaladsl.model.headers.{ContentDispositionTypes, `Content-Disposition`}
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.{ByteString, Timeout}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent.Future
import scala.concurrent.duration.*

/** Web GUI of a client (`GET /`), and the small HTTP API it uses:
  *  - `POST /share`    multipart `file`: split it, start seeding, `.tor` available in the list
  *  - `POST /download` multipart `torrent`: a `.tor` file, start downloading it
  *  - `GET  /torrents` JSON progress of every torrent
  *  - `GET  /torrents/<infoHash>/tor`  the `.tor` file, in `format`
  *  - `GET  /torrents/<infoHash>/file` the downloaded file, once complete
  *
  * `trackerUrl` is written in the `.tor` files created here.
  */
final class WebUi(peerId: String, trackerUrl: String, client: ActorRef[Client.Command], format: TorrentFormat)(using
    system: ActorSystem[?]
):
  import WebUi.*
  import WebUi.Json.given

  private given Timeout                         = Timeout(3.seconds)
  private given scala.concurrent.ExecutionContext = system.executionContext

  private def readAll(bytes: Source[ByteString, Any]): Future[Array[Byte]] =
    bytes.runFold(ByteString.empty)(_ ++ _).map(_.toArrayUnsafe())

  private def find(infoHash: String): Future[Option[Client.Entry]] =
    client.ask[List[Client.Entry]](Client.GetTorrents(_)).map(_.find(_.torrent.infoHash == infoHash))

  private def statuses: Future[ClientView] =
    client.ask[List[Client.Entry]](Client.GetTorrents(_)).flatMap { entries =>
      Future.traverse(entries)(_.peer.ask[Peer.Status](Peer.GetStatus(_))).map { all =>
        ClientView(peerId, all.map(s =>
          TorrentView(s.torrent.infoHash, s.torrent.name, s.torrent.length, s.torrent.pieceCount, s.have.size,
            s.complete, s.peers.toList.sorted)
        ))
      }
    }

  private def attachment(fileName: String) =
    respondWithHeader(`Content-Disposition`(ContentDispositionTypes.attachment, Map("filename" -> fileName)))

  val routes: Route = concat(
    pathSingleSlash(get(getFromResource("ui.html"))),
    path("share") {
      post {
        fileUpload("file") { (meta, bytes) =>
          onSuccess(readAll(bytes)) { data =>
            val (torrent, pieces) = Torrent.create(meta.fileName, data, trackerUrl)
            client ! Client.Seed(torrent, pieces)
            redirect("/", StatusCodes.SeeOther)
          }
        }
      }
    },
    path("download") {
      post {
        fileUpload("torrent") { (_, bytes) =>
          onSuccess(readAll(bytes)) { data =>
            format.decode(data).fold(
              ex => complete(StatusCodes.BadRequest, s"Not a valid ${format.extension} file: ${ex.getMessage}"),
              torrent =>
                client ! Client.Download(torrent)
                redirect("/", StatusCodes.SeeOther)
            )
          }
        }
      }
    },
    path("torrents")(get(onSuccess(statuses)(complete(_)))),
    path("torrents" / Segment / "tor") { infoHash =>
      get {
        onSuccess(find(infoHash)) {
          case Some(entry) =>
            attachment(entry.torrent.name + format.extension) {
              complete(HttpEntity(ContentTypes.`application/octet-stream`, format.encode(entry.torrent)))
            }
          case None => complete(StatusCodes.NotFound, "unknown torrent")
        }
      }
    },
    path("torrents" / Segment / "file") { infoHash =>
      get {
        onSuccess(find(infoHash)) {
          case Some(entry) =>
            onSuccess(entry.peer.ask[Option[Array[Byte]]](Peer.GetFile(_))) {
              case Some(data) =>
                attachment(entry.torrent.name)(complete(HttpEntity(ContentTypes.`application/octet-stream`, data)))
              case None => complete(StatusCodes.Conflict, "download not complete yet")
            }
          case None => complete(StatusCodes.NotFound, "unknown torrent")
        }
      }
    }
  )

object WebUi:
  final case class TorrentView(
      infoHash: String,
      name: String,
      length: Long,
      pieces: Int,
      have: Int,
      complete: Boolean,
      peers: List[String]
  )
  final case class ClientView(peerId: String, torrents: List[TorrentView])

  object Json extends SprayJsonSupport with DefaultJsonProtocol:
    given RootJsonFormat[TorrentView] = jsonFormat7(TorrentView.apply)
    given RootJsonFormat[ClientView]  = jsonFormat2(ClientView.apply)
