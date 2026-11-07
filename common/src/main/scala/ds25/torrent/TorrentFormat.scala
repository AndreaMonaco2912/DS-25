package ds25.torrent

import spray.json.*

import java.nio.charset.StandardCharsets.UTF_8
import scala.util.Try

/** How a [[Torrent]] is written to and read from a `.tor` file.
  *
  * To change the form of the file (bencode, YAML, extra fields...), implement this trait and change
  * [[TorrentFormat.default]]. Nothing else depends on the encoding.
  */
trait TorrentFormat:
  /** File name extension, with the dot. */
  def extension: String
  def encode(torrent: Torrent): Array[Byte]
  def decode(bytes: Array[Byte]): Try[Torrent]

object TorrentFormat:
  val default: TorrentFormat = JsonTorrentFormat

/** `.tor` as readable JSON:
  * `{"name":..,"length":..,"pieceLength":..,"pieceHashes":[..],"announce":"http://tracker:8080"}`
  */
object JsonTorrentFormat extends TorrentFormat with DefaultJsonProtocol:
  private given RootJsonFormat[Torrent] =
    jsonFormat(Torrent.apply, "name", "length", "pieceLength", "pieceHashes", "announce")

  val extension = ".tor"

  def encode(torrent: Torrent): Array[Byte] = torrent.toJson.prettyPrint.getBytes(UTF_8)

  def decode(bytes: Array[Byte]): Try[Torrent] = Try(String(bytes, UTF_8).parseJson.convertTo[Torrent])
