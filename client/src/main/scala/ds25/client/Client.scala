package ds25.client

import ds25.protocol.{PeerAddress, PeerInfo}
import ds25.torrent.Torrent
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

import scala.concurrent.duration.FiniteDuration

/** A client node: the torrents it takes part in, one [[Peer]] (and one [[Announcer]]) each.
  *
  * It knows nothing about `.tor` files, only about [[Torrent]] metadata. It runs at `/user/torrents` and its
  * peers are its children named by info hash, the paths other clients resolve ([[PeerAddress.actorPath]]).
  */
object Client:

  sealed trait Command

  /** Share a file we already have. */
  final case class Seed(torrent: Torrent, pieces: Vector[Array[Byte]]) extends Command

  /** Join the swarm of `torrent` to download it. */
  final case class Download(torrent: Torrent) extends Command

  final case class GetTorrents(replyTo: ActorRef[List[Entry]]) extends Command
  final case class Entry(torrent: Torrent, peer: ActorRef[Peer.Command])

  final case class Settings(self: PeerInfo, serveDelay: FiniteDuration)

  def apply(settings: Settings): Behavior[Command] =
    Behaviors.setup { context =>

      def running(torrents: Map[String, Entry]): Behavior[Command] =

        /** Adding a torrent we already have does nothing. */
        def start(torrent: Torrent, pieces: Map[Int, Array[Byte]]): Behavior[Command] =
          if torrents.contains(torrent.infoHash) then Behaviors.same
          else
            val hash = torrent.infoHash
            val peer = context.spawn(Peer(settings.self.peerId, torrent, pieces, settings.serveDelay), hash)
            context.spawn(Announcer(Announcer.Config(torrent.announce, hash, settings.self), peer), s"announcer-$hash")
            running(torrents.updated(hash, Entry(torrent, peer)))

        Behaviors.receiveMessage {
          case Seed(torrent, pieces) => start(torrent, pieces.zipWithIndex.map(_.swap).toMap)
          case Download(torrent)     => start(torrent, Map.empty)
          case GetTorrents(replyTo)  =>
            replyTo ! torrents.values.toList.sortBy(_.torrent.name)
            Behaviors.same
        }

      running(Map.empty)
    }
