package it.ds;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.event.Logging;
import org.apache.pekko.event.LoggingAdapter;

import java.io.Serializable;

/** A minimal process in the distributed system: replies to a Ping with a Pong. */
public class Node extends AbstractActor {

  // --- Messages (immutable, serializable) ---
  public record Ping(int seq) implements Serializable {}
  public record Pong(int seq) implements Serializable {}
  public record Start(ActorRef peer) implements Serializable {}

  private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
  private final int id;

  public Node(int id) {
    this.id = id;
  }

  public static Props props(int id) {
    return Props.create(Node.class, () -> new Node(id));
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(Start.class, m -> m.peer().tell(new Ping(0), getSelf()))
        .match(Ping.class, m -> {
          log.info("node {} got Ping({}) from {}", id, m.seq(), getSender().path().name());
          getSender().tell(new Pong(m.seq()), getSelf());
        })
        .match(Pong.class, m -> log.info("node {} got Pong({}) from {}", id, m.seq(), getSender().path().name()))
        .build();
  }
}
