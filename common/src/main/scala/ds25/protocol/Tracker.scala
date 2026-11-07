package ds25.protocol

import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

/** A peer as the tracker knows it: an id and the address it can be reached at.
  * `host`/`port` are the peer's Pekko Artery canonical address.
  */
final case class PeerInfo(peerId: String, host: String, port: Int) extends CborSerializable

/** Body of `GET /announce`. `trackerId` shows which tracker instance answered. */
final case class AnnounceResponse(trackerId: String, interval: Int, peers: List[PeerInfo])

object TrackerJson extends SprayJsonSupport with DefaultJsonProtocol:
  given peerInfoFormat: RootJsonFormat[PeerInfo]                 = jsonFormat3(PeerInfo.apply)
  given announceResponseFormat: RootJsonFormat[AnnounceResponse] = jsonFormat3(AnnounceResponse.apply)
