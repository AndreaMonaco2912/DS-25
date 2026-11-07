# DS-25: a simplified BitTorrent in Scala + Apache Pekko

Three **trackers** (T1, T2, T3) and two **clients** (A, B), each with a web GUI. In the GUI you share a file,
which creates a `.tor` file. Another client opens that `.tor` and downloads the file piece by piece, directly
from the clients that have it.

- Clients only know **one tracker endpoint** (`http://tracker:8080`). Behind it, nginx sends each request to
  T1, T2 or T3, so A and B usually talk to *different* trackers.
- The trackers form a **Pekko Cluster** and replicate the swarm registry (infoHash -> peers) with
  **Distributed Data**, a CRDT. Every client finds every other client, whichever tracker it asked.
- The clients exchange pieces **directly** over Pekko remoting. Trackers only introduce peers to each other.
- The default choice is **availability over consistency**. A tracker always answers from its own replica, even
  when it can't reach the others. A stricter `majority` mode exists to compare the two (see [CP vs AP](#consistency-cp-vs-ap)).

```
   browser :8081 ─► client-a ──┐                         ┌── client-b ◄─ browser :8082
                  (HTTP announce)  ┌──────────────────┐  │
                               └──►│ tracker (nginx)  │◄─┘      single endpoint
                                   │ round-robin LB   │
                                   └──┬──────┬─────┬──┘
                                      ▼      ▼     ▼
                                  ┌────┐  ┌────┐  ┌────┐
                                  │ T1 │◄►│ T2 │◄►│ T3 │   Pekko Cluster + Distributed Data
                                  └────┘  └────┘  └────┘   (ORMultiMap: infoHash -> peers, gossip)

   client-a ◄═════ Pekko Artery remoting (Handshake / Have / RequestPiece / Piece) ═════► client-b
```

Stack: Scala 3.9, Pekko 1.7.1 (typed actors, cluster, distributed data, remoting), Pekko HTTP 1.4.2, sbt 1.13,
Docker Compose, nginx.

**Contents:** [Quick start](#quick-start) · [Architecture](#architecture) · [The client](#the-client) ·
[The trackers](#the-trackers) · [CP vs AP](#consistency-cp-vs-ap) · [The swarm registry](#the-swarm-registry-and-how-it-is-replicated) ·
[Design decisions](#design-decisions-and-alternatives) · [Experiments](#experiments) ·
[Configuration](#configuration) · [Simplifications](#simplifications-and-possible-extensions)

---

## Quick start

```bash
sbt test                                                   # unit tests (incl. a 2-node replication test)
sbt tracker/Docker/publishLocal client/Docker/publishLocal # builds images ds25-tracker, ds25-client
docker compose up                                          # or: docker compose up -d && docker compose logs -f
docker compose down
```

Then:
1. Open client A at <http://localhost:8081>, pick a file under **Share a file** and press *Share*. A starts
   seeding it, and the list shows a `.tor` link. Save that file.
2. Open client B at <http://localhost:8082>, choose the `.tor` under **Download from a .tor**. The progress bar
   fills up as pieces arrive from A. Once it's complete, the *file* link gives you the downloaded copy.

It works in both directions, and with any number of files. Files are kept in memory (up to 64 MB per upload).

In the logs:
- `t1`/`t2`/`t3`: `Tracker joined: ...` once they've formed the cluster.
- `tracker` (nginx): `"GET /announce?..." -> <ip>:8080 200`, which shows which tracker served each request.
- `client-a`/`client-b`: `Announced to T1 -> peers [B]`, `Announced to T3 -> peers [B]`, ..., then
  `connected to A`, `got piece 5 from A (6/46)`, ... and finally `DOWNLOAD COMPLETE`.

The same flow from the shell (this is how it was tested):

```bash
curl -F file=@some.bin localhost:8081/share                     # A seeds it
H=$(curl -s localhost:8081/torrents | jq -r '.torrents[0].infoHash')
curl -o some.tor localhost:8081/torrents/$H/tor                 # the .tor file
curl -F torrent=@some.tor localhost:8082/download               # B downloads it
curl -s localhost:8082/torrents                                 # progress (JSON)
curl -o copy.bin localhost:8082/torrents/$H/file && cmp some.bin copy.bin
```

Talking to the tracker endpoint by hand:

```bash
# event=stopped so the probe isn't left in the swarm (clients would keep trying to contact it)
for i in 1 2 3; do curl -s "localhost:8080/announce?info_hash=$H&peer_id=probe&ip=nowhere&port=1&event=stopped"; echo; done
# {"interval":5,"peers":[...A..., ...B...],"trackerId":"T1"}
# {"interval":5,"peers":[...A..., ...B...],"trackerId":"T2"}   <- rotates, same peer list

curl localhost:8080/cluster   # T1 sees [t1:Up, t2:Up, t3:Up]   (each tracker's view of the cluster)
```

---

## Architecture

### Modules

| sbt project | What it contains |
|---|---|
| `common`  | `ds25.protocol`: what goes over the network: `PeerInfo`, `AnnounceResponse` (+ JSON), peer-wire messages `PeerMsg`, `PeerAddress`, `CborSerializable`.<br>`ds25.torrent`: the `.tor` file: `Torrent` (metadata, info hash, piece hashes) and `TorrentFormat` (how it is encoded) |
| `tracker` | `TrackerMain` (cluster node + HTTP server), `TrackerRoutes` (`GET /announce`, `GET /health`, `GET /cluster`), `SwarmRegistry` (Distributed Data) |
| `client`  | `ClientMain` (wiring), `Client` (the torrents of this node), `Peer` (one torrent's swarm: handshake, piece exchange), `Announcer` (periodic HTTP announce, one per torrent), `WebUi` + `ui.html` (GUI and its HTTP API) |

### The four kinds of links

Two of them are HTTP, the ones that involve a browser or the tracker endpoint:

| Link | Protocol | Port | Why |
|---|---|---|---|
| browser → client (GUI) | **HTTP** (page + small JSON API) | 8081, 8082 on the host (8080 in the container) | Upload/download files and `.tor`s, follow progress. |
| client → tracker (announce) | **HTTP** (`GET /announce`, JSON reply) | 8080 | Same as real BitTorrent trackers. Stateless request/response, so clients don't have to join the tracker cluster or know any actor address, and a standard HTTP load balancer can sit in front. |
| tracker ↔ tracker | Pekko Cluster + Distributed Data over Artery (TCP) | 25520 | Membership, failure detection and replication of the swarm registry. |
| client ↔ client | Pekko Artery remoting (TCP), messages in Jackson CBOR | 25520 | Piece exchange (`Handshake`, `Have`, `RequestPiece`, `Piece`). |

### nginx

**nginx** (`docker/nginx.conf`, the `tracker` service in `docker-compose.yml`) is only infrastructure for the
client → tracker link. It is not part of the system's logic and holds no state:

- it gives clients **one address** (`http://tracker:8080`) instead of a list of trackers;
- it sends each request to T1, T2 or T3 in turn (round-robin), which is how A and B end up on different trackers
  and why the replication between trackers is visible;
- if a tracker doesn't answer, or replies `502`/`503`, it retries the same request on another one
  (`proxy_next_upstream`). This is safe because an announce is idempotent: it just re-adds the peer to a set;
- it re-resolves `t1`/`t2`/`t3` through Docker's DNS, so a restarted tracker with a new IP is found again.

Making sure every tracker knows every peer is the trackers' job (Distributed Data), not nginx's. Without
nginx the system still works: set `TRACKER_URL=http://t1:8080` for a client (it goes into the `.tor` files it
creates) and it will see the peers that announced to T2 and T3 too.

In this demo nginx is a single point of failure. Real BitTorrent avoids that with DNS round-robin or several
tracker URLs in the torrent (`announce-list`, BEP 12). Here that would be a new field in `Torrent` plus a loop in
`Announcer`.

---

## The client

### `.tor` file vs. client

The `.tor` file is kept fully apart from the client logic. Only `WebUi` (the edge where files come in and go out)
sees encoded bytes. Everything else works with the `Torrent` model:

```
             bytes of a .tor                         Torrent (model)
  WebUi  ◄──────────────────►  TorrentFormat  ◄──►  name, length, pieceLength,
                               (JsonTorrentFormat)   pieceHashes, announce, infoHash
                                                          ▲
                                                          │ only the model
                                          Client ── Peer ── Announcer
```

- **`Torrent`** (`common/.../ds25/torrent/Torrent.scala`) is the model:
  - `Torrent.create(name, bytes, announce)` splits a file into 64 KiB pieces and hashes each one with SHA-1;
  - `isValidPiece(i, data)` checks a received piece;
  - the constructor rejects inconsistent metadata, e.g. a piece count that doesn't match the length.
- **`infoHash`**, the swarm id, is computed **from the fields, not from the encoded file**. Real BitTorrent hashes
  the bencoded `info` dictionary, which ties the swarm id to the file format. Here a new format keeps the same
  swarms. As in BitTorrent, the tracker URL is not part of it, so the same content announced to another tracker
  is still the same swarm.
- **`TorrentFormat`** (`TorrentFormat.scala`) is the only code that knows the file layout:

  ```scala
  trait TorrentFormat:
    def extension: String
    def encode(torrent: Torrent): Array[Byte]
    def decode(bytes: Array[Byte]): Try[Torrent]
  ```

  The current one is `JsonTorrentFormat`, readable JSON with extension `.tor`:

  ```json
  { "announce": "http://tracker:8080", "length": 3000000, "name": "some.bin",
    "pieceHashes": ["7f6bde04...", "e4b7f658...", ...], "pieceLength": 65536 }
  ```

  **To change the form of the `.tor`** (bencode, YAML, a binary layout, a signed file...), write another object
  implementing `TorrentFormat` and assign it to `TorrentFormat.default`, or pass it to `WebUi` in `ClientMain`.
  To add **fields** (e.g. `announceList`, `comment`, `createdBy`), add them to `Torrent` and to the format. Fields
  that identify the content should also go into `infoHash`.

### Inside a client

```
 ClientMain
   ├── WebUi (HTTP :8080)      ── Seed / Download / GetTorrents ──►  Client  (/user/torrents)
   │                                                                   ├── Peer       (/user/torrents/<infoHash>)
   │                                                                   ├── Announcer  (/user/torrents/announcer-<infoHash>)
   │                                                                   ├── Peer       (another torrent)
   │                                                                   └── Announcer
   └── CoordinatedShutdown: announce `stopped` for every torrent
```

- **`Client`** is the node: a map infoHash -> (`Torrent`, `Peer`). `Seed(torrent, pieces)` starts a `Peer` with
  every piece, and `Download(torrent)` starts one with none. Adding a torrent that is already there does nothing.
- **`Peer`** is this node's part in **one** swarm, and seeding and downloading are the same code. Each torrent
  has its own actor at a fixed path, `/user/torrents/<infoHash>`. The tracker only hands out `host:port`, and
  `PeerAddress.actorPath(host, port, infoHash)` turns that into the actor to talk to. So messages need no
  torrent id, and two torrents never mix.
- **`Announcer`** announces one torrent every `interval` seconds (the tracker decides, 5 s here), retries after 2 s
  on failure, and passes each peer list to its `Peer`.

### Peer wire protocol

| Message | Meaning |
|---|---|
| `Handshake(peerId, have, replyTo)` | Sent to every peer from the tracker that we aren't connected to yet (again on every announce until it answers). |
| `HandshakeAck(peerId, have, replyTo)` | Reply, with our own pieces. Both sides now know each other's `have` set. |
| `Have(peerId, index)` | Broadcast to connected peers after a new piece is verified. |
| `RequestPiece(index, replyTo)` | Ask a holder for a piece. Unknown pieces are silently ignored. |
| `Piece(peerId, index, data)` | The piece's bytes (CBOR binary). Checked against `pieceHashes` and dropped if it doesn't match. |

How a download proceeds:
- **Piece choice.** Missing pieces are requested in index order. Each one goes to a **random** holder, which
  spreads the load across seeders.
- **Request limit.** At most `MaxInFlight = 4` requests are open at a time. A new one is sent each time a piece
  arrives, so a big file is fetched gradually and the GUI shows real progress.
- **Lost requests.** A request sent to a peer that died is never answered. So on every announce the
  "requested" set is cleared and the missing pieces are asked for again. Duplicates are simply ignored.
- **Serving.** A piece is served after `PIECE_DELAY_MS` (300 ms by default), only to make the exchange visible.

### GUI and its API

`WebUi` serves one static page (`client/src/main/resources/ui.html`, vanilla JS, no CDN). The page uses plain
HTML forms and polls `GET /torrents` every second. The same endpoints work with `curl`:

| Endpoint | What it does |
|---|---|
| `GET /` | The page |
| `POST /share` (multipart `file`) | Creates the `Torrent` (announce = this client's `TRACKER_URL`), seeds it, redirects to `/` |
| `POST /download` (multipart `torrent`) | Decodes the `.tor` with `TorrentFormat` (`400` if invalid), starts downloading |
| `GET /torrents` | `{"peerId":"A","torrents":[{infoHash,name,length,pieces,have,complete,peers}]}` |
| `GET /torrents/<infoHash>/tor` | The `.tor` file, encoded by the current format |
| `GET /torrents/<infoHash>/file` | The whole file (`409` while incomplete) |

---

## The trackers

- **Cluster formation.** T1 is the first seed node, the only one that may start a new cluster. T2 and T3 join it.
- **Failure handling.** The cluster uses the **Split Brain Resolver** with `keep-majority` and `stable-after = 10s`.
  When a tracker has been unreachable for 10 s, the side with the majority downs the others. A downed tracker
  exits (`exit-jvm = on`), Docker restarts it, and it joins again.
- **What a tracker stores.** It has no state of its own. Everything is in `SwarmRegistry`, a replicated
  `ORMultiMap[infoHash, PeerInfo]`.
- **Announce handling.** An announce is one `Update` (add the peer, or remove it on `event=stopped`) followed by a
  `Get` of that swarm. The reply lists every peer in the swarm except the caller.

### Why three trackers

With two trackers, every failure is a 1-vs-1 tie: the survivor can't tell a crashed partner from a network
partition. `keep-majority` breaks the tie by address, so if **T1** crashed, T2 had to shut itself down. With three,
a single crash or a 1-vs-2 partition has a clear majority, and the two survivors keep the cluster. Three is also
the smallest size where `majority` (2 of 3) still works with one tracker down, as the next section shows.

---

## Consistency: CP vs AP

`CONSISTENCY` (env of the trackers, `local` by default) chooses the Distributed Data read/write levels:

| | `local` (default) | `majority` |
|---|---|---|
| Write (announce) | `WriteLocal`: applied to this tracker's replica, acknowledged at once | `WriteMajority(3s)`: acknowledged only when ⌊N/2⌋+1 trackers have it |
| Read (peer list) | `ReadLocal`: this tracker's replica | `ReadMajority(3s)`: merges the replicas of a majority |
| Propagation | Others learn the change by delta/gossip in a fraction of a second up to ~2 s | Already on a majority when the client gets the reply |
| When trackers are unreachable | **Always answers** (possibly with a slightly stale list) | **`503`** if a majority can't be reached within 3 s |
| CAP / PACELC | **AP / EL**: available in a partition, low latency otherwise | **CP / EC**: consistent in a partition, extra round trips otherwise |
| Cost per announce | 0 network round trips before replying | 2 round trips (update + read) to other trackers |

In `majority` mode, any majority read sees the last majority write (two majorities always overlap). In `local`
mode, two clients asking two trackers at the same moment can get different lists for a second or two. Every
replica still converges to the same state, because the ORMultiMap merge is a CRDT.

### What was measured (3 trackers, `docker compose kill`)

| Scenario | `local` | `majority` |
|---|---|---|
| All up | 200 | 200 |
| Kill t3 | 200 the whole time (some requests take up to ~2 s while nginx gives up on t3) | **200 the whole time**: 2 of 3 are still a majority. With 2 trackers this would already fail |
| Then kill t2 (cluster is now {t1, t2}, 1-vs-1 tie) | 200 | **503** for ~15 s, until the SBR resolves the tie for t1 (lowest address) and downs t2. t1 alone is then its own majority: 200 |
| Kill t2 **and** t3 at once (t1 is 1 of 3) | 200 for ~18 s from t1's replica, then t1 downs itself, one `502` while it restarts, then 200 again | 503 until t1 downs itself, then 200 again after the restart |

A subtlety seen in the last row: a restarted t1 that **hasn't joined a cluster yet** (`/cluster` shows
`T1 sees []`) answers `200` even in `majority` mode. A replica counts the majority over the nodes it knows, and
here it knows only itself. In that window `majority` is effectively `local`, with empty state. t1 forms a new
cluster on its own after its 30 s `seed-node-timeout`, and the clients' announces fill it up again.

### Why `local` is the default

- **The data is soft state.** Every client re-announces every 5 s, so any missing or lost entry comes back within
  one interval. Even a crash that loses an announce acknowledged with `WriteLocal`, before it was gossiped, is
  repaired by the next announce.
- **A stale answer is cheap, a missing one is not.** A peer missing from a list for a few seconds delays a
  handshake by one interval, and an extra (departed) peer costs a handshake that is never answered. A `503`
  means the client gets no peers at all until the retry.
- **The rest of the system is eventually consistent anyway.** A `majority` read is consistent among the trackers,
  but it is outdated as soon as it reaches the client: peers join, leave and crash without telling anyone.
  Clients also keep exchanging pieces with known peers whatever the trackers do.
- **Lower latency.** No round trips between trackers before replying.

`majority` is still useful to *see* the trade-off: `CONSISTENCY=majority docker compose up -d`, then the
experiments above.

---

## The swarm registry and how it is replicated

### Data model

One Distributed Data entry, key `"swarms"`, of type `ORMultiMap[String, PeerInfo]`: infoHash -> set of
`PeerInfo(peerId, host, port)`. Every tracker has a full replica.

Why a CRDT (ORMultiMap = an observed-remove map of observed-remove sets):
- **Merging concurrent updates.** Announces arrive concurrently at different trackers. Replicas are merged
  automatically and always reach the same state, whatever the order the updates arrive in. There are no locks
  and no leader.
- **Add wins.** If a peer is removed (`stopped`) on one tracker while it is concurrently added on another, the
  add wins. For a tracker that's the right bias: a wrong extra peer is harmless, a missing one isn't.
- **Precise removes.** A remove deletes only the additions it has seen, so it can't wipe out a newer announce.

### What travels between trackers today

- **Delta-CRDT is on** (Pekko's default, `delta-crdt.enabled = on`). After a local update, the Replicator sends
  the other trackers a **delta** of the change, not the whole map.
- **The delta is coarser than the change.** With `ORMultiMap.empty` (what `SwarmRegistry` uses), the delta of
  `addBinding` carries the changed key **with its whole value**: adding one peer sends that swarm's full peer
  set. The Pekko docs imply this by contrast with `emptyWithValueDeltas`.
- **Full state still travels**, as a safety net:
  - **Gossip.** Every 2 s (`gossip-interval`) each tracker sends a random other tracker a digest of its entries.
    If a digest differs (a delta was lost, a partition healed), the **full value of that entry** is sent and merged.
  - **Join or rejoin.** A tracker that joins or rejoins receives everything this way. In the experiments, a
    restarted t1 got the whole registry back by gossip.
  - **Large deltas.** A delta that grows beyond `max-delta-size` (50) is replaced by the full state.
- **Everything is one entry.** All swarms live under the single key `"swarms"`, so each full-state transfer
  sends **every swarm**, not just the one that changed.
- **Clients get full lists too.** The announce reply returns the full peer list of a swarm to the client.

### Is that a good idea?

**For this project, yes.** There are a few peers and a few swarms, so the whole map is a few hundred bytes. The
state is soft and rebuilt every 5 s anyway, and one entry keeps the code to a single key.

**At scale, no.** A full-state repair costs O(all peers of all swarms), and gossip runs constantly. Pekko logs a
warning when an entry passes 10 KiB (`log-data-size-exceeding`), which one shared entry would reach early.
Every entry also has to fit in one remote message. The Pekko docs recommend spreading data over several
top-level entries (e.g. 10 maps of 100 elements instead of one of 1000) and keeping fewer than ~100 000 of them.
The single key is a bigger problem than the size of each delta.

### Sending only the differences: options

1. **`ORMultiMap.emptyWithValueDeltas`** (one-line change in `SwarmRegistry`).
   - **Effect:** the value set is also replicated as deltas, so an announce sends roughly "add peer X to swarm H"
     and nothing more. The Pekko docs call it "much more efficient in terms of network bandwidth".
   - **Caveat:** its replication is **not compatible** with a plain `ORMultiMap`. All trackers must switch
     together, and old and new code must never share a key. With soft state, a full restart (or a new key name)
     is enough.
2. **One entry per swarm:** `ORSetKey[PeerInfo]("swarm-" + infoHash)`.
   - **Effect:** digests and full-state transfers are per swarm, so a repair sends only the swarms that differ.
     Each swarm can also get its own consistency level.
   - **Cost:** two swarms can't be updated atomically together, which an announce never needs. Very many swarms
     mean very many entries (the ~100 k limit).
   - **Combined with option 1:** `ORSet` deltas are already per element.
3. **Bucketed keys.** Hash the infoHash into a fixed number of keys (e.g. 16 `ORMultiMap`s). This is a middle
   ground: bounded entry size and a bounded number of keys.
4. **Bounded replies to clients.** Like real trackers' `numwant` (usually 50): return a random subset of the swarm.
   This limits what the *client* receives, independent of the tracker replication.
5. **Don't replicate at all.** Real BitTorrent trackers share no state. A torrent lists several trackers
   (`announce-list`), the client announces to them, and peers also find each other through **DHT** (BEP 5) and
   **Peer Exchange** (BEP 11). Each tracker would then hold a partial view, which is fine for soft state, and the
   cluster could go away.
6. **Partition instead of replicate.** Cluster Sharding by infoHash: each swarm lives on one tracker, so there is
   no replication traffic and each swarm is strongly consistent. But a tracker crash makes its swarms unavailable
   until they are rebuilt elsewhere (from announces), which runs against "availability first".

Options 1 and 2 keep the current design and remove most of the traffic. Option 5 is what BitTorrent does in
practice.

### Known gap: crashed peers

A peer is removed only when it announces `event=stopped` (done in `CoordinatedShutdown`). A crashed client stays
in the swarm forever, and the others keep sending it handshakes, which are cheap and never answered. A fix in
CRDT terms: store a last-seen timestamp per peer (e.g. `ORMap[infoHash, LWWMap[peerId, (PeerInfo, lastSeen)]]`),
and filter out, or periodically remove, peers not seen for ~3 intervals.

---

## Design decisions and alternatives

| Decision | Why | Alternatives considered |
|---|---|---|
| HTTP for client → tracker | What BitTorrent does. Stateless and idempotent, so it works through a load balancer and clients need no cluster membership | Clients as cluster members or remote actors talking to the tracker actors: clients would come and go in the cluster (churn), and a load balancer couldn't sit in front |
| One endpoint via nginx | Clients know one URL, and round robin makes the replication visible | `announce-list` in the torrent (no single point of failure, more client logic), DNS round robin |
| Pekko Cluster only for trackers, plain remoting for clients | Trackers are few and stable. Clients churn, and cluster membership (gossip, failure detection, downing) would be wasted on them | Everyone in one cluster |
| Distributed Data (CRDT) for the registry | Merges concurrent announces on different trackers without coordination, works during partitions | Our own replication protocol, a database, Cluster Sharding (see above) |
| `local` consistency by default | Availability over consistency. The data is soft state (see CP vs AP) | `majority`, still available via `CONSISTENCY` |
| 3 trackers + `keep-majority` | One failure has a clear majority. `majority` survives one tracker down | 2 trackers (every failure is a tie), `keep-oldest`/`static-quorum` strategies, a lease |
| t1 `seed-node-timeout = 30s` | A restarted t1 waits for the others instead of starting a second cluster (split brain at boot) | Equal timeouts (a restarted t1 that can't connect fast enough forms its own cluster) |
| `Torrent` model + `TorrentFormat` trait | The `.tor` form can change in one place. The client works on the model only | Encoding fields inside the client classes |
| infoHash from the fields, not from the encoded bytes | Swarm identity doesn't change with the file format | BitTorrent's SHA-1 of the bencoded `info` dictionary |
| JSON `.tor` | Readable, spray-json is already a dependency | bencode (compatible with real BitTorrent), YAML |
| 64 KiB pieces | A piece fits in one Artery message (default `maximum-frame-size` 256 KiB) | Bigger pieces with Artery's large-message stream, or a separate transfer channel |
| SHA-1 per piece | Detects corrupt or malicious pieces; same as BitTorrent v1 | SHA-256 Merkle tree (BitTorrent v2) |
| Random holder, in order, max 4 in flight | Simple, spreads load, progress is visible | Rarest-first (what BitTorrent uses, improves swarm health), endgame mode, choking/tit-for-tat |
| Re-request on every announce | Recovers requests lost with a dead peer, without per-request timers | A timeout per request |
| One actor per torrent at `/user/torrents/<infoHash>` | The address from the tracker plus the infoHash is enough to reach a peer, with no routing and no torrent id in messages | One actor routing by infoHash, or an infoHash field in every message |
| GUI served by the client with pekko-http | Already a dependency. Works in Docker (a Swing GUI would need a display). No build step for the frontend | Desktop GUI (Swing/JavaFX), a separate frontend app |
| Files in memory | Simplest | A data directory per client (volume), resumable downloads |
| Jackson CBOR for remote messages | Compact, binary-friendly (`Array[Byte]` is sent as raw bytes), standard in Pekko | Java serialization (unsafe, disabled by default), Protobuf |

---

## Experiments

Use `curl localhost:8080/cluster` (a few times, to hit every tracker) to see the cluster membership. A tracker
stopped with `docker compose kill`/`stop` is not restarted by Docker (it was stopped by hand). Use
`docker compose start <name>`. A tracker downed by the SBR exits by itself and *is* restarted.

- **Graceful tracker failure:** `docker compose stop t1`. T1 leaves the cluster, nginx sends everything to T2
  and T3, and they still know all the peers. `docker compose start t1` rejoins and receives the state by gossip.
- **Crash of one tracker:** `docker compose kill t1` (or t2, t3). The other two mark it unreachable, and after
  `stable-after` (10 s) the SBR downs it. 2 of 3 is a majority, so they keep running and answering.
  Tested: a download started right after the kill completed normally, and the restarted t1 rejoined
  (~11 s) with the full swarm.
- **Two crashes, one after the other:** kill t3, wait for `Tracker removed`, kill t2. The cluster is then
  {t1, t2}, a 1-vs-1 tie. `keep-majority` keeps the lowest address, T1, which goes on alone. Had you killed t1
  second, T2 would have downed itself.
- **Two crashes at once:** `docker compose kill t2 t3`. T1 is 1 of 3, a minority. It answers from its replica for
  ~18 s, then downs itself, restarts, answers with empty lists while waiting, and after 30 s forms a new cluster
  alone. The clients' announces rebuild the registry. Connected peers keep exchanging pieces throughout, since
  that doesn't go through the trackers.
- **CP vs AP:** repeat the above with `CONSISTENCY=majority docker compose up -d` and compare with the table in
  [CP vs AP](#consistency-cp-vs-ap). Watch the `503`s in nginx's log.
- **Corrupt piece:** `PeerSpec` sends a piece with wrong bytes, which is dropped (the hash doesn't match).

---

## Configuration

**Tracker env:**
- `TRACKER_ID`
- `HTTP_PORT` (8080)
- `CONSISTENCY` (`local`, the default, or `majority`)
- `ANNOUNCE_INTERVAL` (5 s)
- `CANONICAL_HOST`/`CANONICAL_PORT` (Artery address)
- seed nodes via `-Dpekko.cluster.seed-nodes.N=...` in `JAVA_OPTS`

**Client env:**
- `PEER_ID`
- `TRACKER_URL`: written into the `.tor` files the client creates
- `HTTP_PORT`: the GUI, 8080 in the container
- `PIECE_DELAY_MS` (300): delay when serving a piece, so progress is visible
- `CANONICAL_HOST`/`CANONICAL_PORT`

**Code constants:**
- `Torrent.DefaultPieceLength` (64 KiB)
- `Peer.MaxInFlight` (4)
- `pekko.http.server.parsing.max-content-length` (64m, in `client/.../application.conf`)

### Docker notes

- t1 runs with `seed-node-timeout=30s`, so a restarted t1 waits for t2/t3 to answer instead of forming a second
  cluster. t2 and t3 keep the 5 s default, which is also their retry interval for joining.
- `dns_search: .` keeps the host's DNS search domain out of the containers. Some home routers answer
  `127.0.0.1` for unknown names, which made a tracker resolve its stopped peer to itself.
- The images set a 2 s JVM DNS cache, so a restarted container's new IP is picked up quickly.

---

## Simplifications and possible extensions

What is simpler than real BitTorrent:
- **Announce protocol.** Responses are JSON, not bencode, and `ip` is required. It must be the peer's Pekko
  canonical hostname, because remoting drops messages addressed to any other name.
- **`.tor` files** are JSON, not bencode (swap the `TorrentFormat`). They have one tracker URL, no `announce-list`.
- **Peers are never expired.** They leave the swarm only with `event=stopped` (see
  [crashed peers](#known-gap-crashed-peers)).
- **Piece exchange.** No choking, no rarest-first, no bandwidth limits. At most 4 requests are in flight.
- **Storage.** Files live in memory only. A client that restarts forgets its torrents.
- **Tracker endpoint.** nginx is a single point of failure for the client → tracker link.

Natural next steps:
- `emptyWithValueDeltas` and/or one key per swarm;
- peer expiry with last-seen timestamps;
- `announce-list` in `Torrent` with failover in `Announcer`;
- rarest-first piece selection;
- persistent storage per client;
- a bencode `TorrentFormat` to read real `.torrent` metadata.
