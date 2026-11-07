# DS-25: a simplified BitTorrent in Scala + Apache Pekko

Two **trackers** (T1, T2) and two **clients** (A, B) share a file split into pieces.
Clients only know **one tracker endpoint** (`http://tracker:8080`). Behind it, nginx sends each request to
T1 or T2, so A and B usually talk to *different* trackers. The trackers form a **Pekko Cluster** and replicate
the swarm registry with **Distributed Data** (a CRDT), so every client still finds every other client.
The clients then exchange pieces **directly**, over Pekko remoting.

```
            client-a ──┐                         ┌── client-b
   (HTTP announce)     │   ┌──────────────────┐  │
                       └──►│ tracker (nginx)  │◄─┘      single endpoint
                           │ round-robin LB   │
                           └──┬────────────┬──┘
                              ▼            ▼
                        ┌────────┐  gossip  ┌────────┐
                        │   T1   │◄────────►│   T2   │   Pekko Cluster + Distributed Data
                        └────────┘   CRDT   └────────┘   (ORMultiMap: infoHash -> peers)

   client-a ◄═════ Pekko Artery remoting (Handshake / Have / RequestPiece / Piece) ═════► client-b
```

## Where HTTP and nginx are used (and where they aren't)

There are three kinds of links, and only one of them is HTTP:

| Link | Protocol | Port | Why |
|---|---|---|---|
| client → tracker (announce) | **HTTP** (`GET /announce`, JSON reply) | 8080 | Same as real BitTorrent trackers. Stateless request/response, so clients don't have to join the tracker cluster or know any actor address, and a standard HTTP load balancer can sit in front. |
| tracker ↔ tracker | Pekko Cluster + Distributed Data over Artery (TCP) | 25520 | Membership, failure detection and replication of the swarm registry. |
| client ↔ client | Pekko Artery remoting (TCP) | 25520 | Piece exchange (`Handshake`, `Have`, `RequestPiece`, `Piece`). |

**nginx** (`docker/nginx.conf`, the `tracker` service in `docker-compose.yml`) is only infrastructure for the
client → tracker link. It is not part of the system's logic and holds no state:

- it gives clients **one address** (`http://tracker:8080`) instead of a list of trackers;
- it sends each request to T1 or T2 in turn (round-robin), which is how A and B end up on different trackers
  and why the replication between trackers is visible;
- if a tracker doesn't answer, or replies `502`/`503`, it retries the same request on the other one
  (`proxy_next_upstream`). This is safe because an announce can be repeated: it just re-adds the peer to a set;
- it re-resolves `t1`/`t2` through Docker's DNS, so a restarted tracker with a new IP is found again.

Making sure every tracker knows every peer is the trackers' job (Distributed Data), not nginx's. Without
nginx the system still works: set `TRACKER_URL=http://t1:8080` for a client and it will see the peers that
announced to T2 too. In this demo nginx is a single point of failure. Real BitTorrent avoids that with
DNS round-robin or several tracker URLs in the torrent (`announce-list`).

## Modules

| sbt project | What it contains |
|---|---|
| `common`  | Protocol shared by everyone: `PeerInfo`, `AnnounceResponse` (+ JSON), peer-wire messages `PeerMsg`, `CborSerializable` |
| `tracker` | `TrackerMain` (cluster node + HTTP server), `TrackerRoutes` (`GET /announce`, `GET /health`), `SwarmRegistry` (Distributed Data) |
| `client`  | `ClientMain`, `Announcer` (periodic HTTP announce), `Peer` (handshake, piece exchange) |

Stack: Scala 3.9, Pekko 1.7.1 (typed actors, cluster, distributed data, remoting), Pekko HTTP 1.4.2, sbt 1.13.

## Build & test

```bash
sbt test                                                   # unit tests (incl. a 2-node replication test)
sbt tracker/Docker/publishLocal client/Docker/publishLocal # builds images ds25-tracker, ds25-client
```

## Run

```bash
docker compose up             # Ctrl+C to stop; or: docker compose up -d && docker compose logs -f
docker compose down
```

What you should see:
- `t1`/`t2`: `Tracker joined: ...` once they've formed the cluster.
- `tracker` (nginx): `"GET /announce?..." -> <ip>:8080 200`, which shows which tracker served each request.
- `client-a`/`client-b`: `Announced to T1 -> peers [B]`, then `Announced to T2 -> peers [B]`, and so on,
  followed by `connected to B`, `got piece 5 from B (5/8)`, ... and finally `DOWNLOAD COMPLETE`.

A starts with pieces 0–3 and B with 4–7 (`PIECES` in `docker-compose.yml`), so each must download the other half.

### Talking to the endpoint by hand

```bash
# event=stopped so the probe isn't left in the swarm (clients would keep trying to contact it)
for i in 1 2 3 4; do curl -s 'localhost:8080/announce?info_hash=demo-file&peer_id=probe&ip=nowhere&port=1&event=stopped'; echo; done
# {"interval":5,"peers":[...A..., ...B...],"trackerId":"T1"}
# {"interval":5,"peers":[...A..., ...B...],"trackerId":"T2"}   <- alternates, same peer list

curl localhost:8080/cluster   # T1 sees [t1:Up, t2:Up]   (each tracker's view of the cluster)
```

## Experiments

Use `curl localhost:8080/cluster` (twice, to hit both trackers) to see the cluster membership.

- **Graceful tracker failure:** `docker compose stop t1`. T1 leaves the cluster, nginx sends everything to T2,
  and T2 still knows all the peers. `docker compose start t1` makes it rejoin and receive the state by gossip.
- **Crash of T2:** `docker compose kill t2`. T1 marks it unreachable, and after `stable-after` (10 s) the
  split-brain resolver downs it. T1 keeps serving with the full state. `docker compose start t2` rejoins.
- **Crash of T1, the limit of 2 nodes:** `docker compose kill t1`. T2 can't tell a crashed T1 from a network
  partition. `keep-majority` breaks the 1-vs-1 tie in favor of the lowest address (T1), so **T2 downs
  itself** to avoid a split brain. It restarts, waits for T1 (the first seed node) and answers with empty peer
  lists meanwhile. When T1 is back, the clients' periodic announces rebuild the registry (it's soft state).
  A third tracker would give the survivors a real majority.
- **Consistency:** `CONSISTENCY=majority docker compose up -d`. Writes and reads now need a majority of the
  trackers (with 2 trackers: both). While a tracker is unreachable but not yet removed from the cluster,
  announces fail with `503` instead of returning possibly stale data (CP vs AP).

## Configuration

Tracker env: `TRACKER_ID`, `HTTP_PORT` (8080), `CONSISTENCY` (`local`|`majority`), `ANNOUNCE_INTERVAL` (5 s),
`CANONICAL_HOST`/`CANONICAL_PORT` (Artery address), seed nodes via `-Dpekko.cluster.seed-nodes.N=...` in `JAVA_OPTS`.

Client env: `PEER_ID`, `TRACKER_URL`, `INFO_HASH`, `TOTAL_PIECES` (8), `PIECES` (e.g. `0-3` or `0,2,4`),
`PIECE_DELAY_MS` (300), `CANONICAL_HOST`/`CANONICAL_PORT`.

## Docker notes

- t1 is the first seed node, the only one allowed to start a new cluster. It runs with
  `seed-node-timeout=30s`, so a restarted t1 waits for t2 to answer instead of forming a second cluster.
  t2 keeps the 5 s default, which is also its retry interval for joining.
- `dns_search: .` keeps the host's DNS search domain out of the containers. Some home routers answer
  `127.0.0.1` for unknown names, which made a tracker resolve its stopped peer to itself.
- The images set a 2 s JVM DNS cache, so a restarted container's new IP is picked up quickly.

## Simplifications compared to real BitTorrent

- Announce responses are JSON, not bencode, and `ip` is required. It must be the peer's Pekko canonical
  hostname, because remoting drops messages addressed to any other name.
- Peers are removed only when they announce `event=stopped` (done on shutdown). There's no expiry yet for
  peers that crash.
- Pieces are tiny strings, with no hashing or verification and no choking.
