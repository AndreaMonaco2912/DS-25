package ds25.torrent

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

/** Metadata of a shared file: what a `.tor` file contains.
  *
  * This is only the model. How it is written to disk is decided by a [[TorrentFormat]], so the file format can
  * change without touching the client.
  *
  * @param pieceHashes SHA-1 (hex) of every piece, used to verify downloaded pieces
  * @param announce    tracker endpoint the clients announce to
  */
final case class Torrent(name: String, length: Long, pieceLength: Int, pieceHashes: Vector[String], announce: String):
  require(pieceLength > 0, "pieceLength must be positive")
  require(pieceHashes.size == (length + pieceLength - 1) / pieceLength, "number of pieces doesn't match the length")

  def pieceCount: Int = pieceHashes.size

  /** Id of the swarm. Computed from the fields, not from the encoded file, so a new [[TorrentFormat]] keeps the
    * same swarms. As in BitTorrent, the tracker URL is not part of it.
    */
  lazy val infoHash: String =
    Torrent.sha1Hex(s"$name\n$length\n$pieceLength\n${pieceHashes.mkString(",")}".getBytes(UTF_8))

  def isValidPiece(index: Int, data: Array[Byte]): Boolean =
    pieceHashes.lift(index).contains(Torrent.sha1Hex(data))

object Torrent:

  /** Well below Artery's default `maximum-frame-size` (256 KiB): a piece travels in one remoting message. */
  val DefaultPieceLength: Int = 64 * 1024

  /** Split `data` into pieces and describe it. Returns the metadata and the pieces. */
  def create(
      name: String,
      data: Array[Byte],
      announce: String,
      pieceLength: Int = DefaultPieceLength
  ): (Torrent, Vector[Array[Byte]]) =
    val pieces = data.grouped(pieceLength).toVector
    (Torrent(name, data.length.toLong, pieceLength, pieces.map(sha1Hex), announce), pieces)

  def sha1Hex(data: Array[Byte]): String =
    MessageDigest.getInstance("SHA-1").digest(data).map(b => f"$b%02x").mkString
