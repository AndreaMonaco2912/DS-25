package ds25.protocol

/** Marker for every message that crosses the network over Pekko remoting.
 * Bound to Jackson CBOR in `reference.conf` of this module.
 */
trait CborSerializable
