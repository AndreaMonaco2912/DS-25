package ds25.torrent

import org.scalatest.wordspec.AnyWordSpec

class TorrentSpec extends AnyWordSpec:

  private val data              = Array.tabulate[Byte](10)(_.toByte)
  private val (torrent, pieces) = Torrent.create("ten.bin", data, "http://tracker:8080", pieceLength = 4)

  "Torrent.create" should {
    "split a file into pieces and hash them" in {
      assert(pieces.map(_.length) == Vector(4, 4, 2))
      assert(torrent.pieceCount == 3 && torrent.length == 10)
      assert(pieces.zipWithIndex.forall((p, i) => torrent.isValidPiece(i, p)))
      assert(!torrent.isValidPiece(0, pieces(1)))
      assert(!torrent.isValidPiece(7, pieces(0)))
    }

    "give the same info hash to the same content, whatever the tracker" in {
      val (other, _) = Torrent.create("ten.bin", data, "http://elsewhere", pieceLength = 4)
      assert(other.infoHash == torrent.infoHash)
      assert(Torrent.create("eleven.bin", data, "http://tracker:8080", 4)._1.infoHash != torrent.infoHash)
    }

    "reject inconsistent metadata" in {
      assertThrows[IllegalArgumentException](torrent.copy(length = 100))
    }
  }

  "JsonTorrentFormat" should {
    "round-trip a torrent" in {
      assert(JsonTorrentFormat.decode(JsonTorrentFormat.encode(torrent)).get == torrent)
    }

    "fail on garbage" in {
      assert(JsonTorrentFormat.decode("not json".getBytes).isFailure)
      assert(JsonTorrentFormat.decode("""{"name":"x"}""".getBytes).isFailure)
    }
  }
