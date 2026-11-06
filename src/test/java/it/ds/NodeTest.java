package it.ds;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class NodeTest {
  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("test");
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
  }

  @Test
  public void repliesToPingWithPong() {
    new TestKit(system) {{
      ActorRef node = system.actorOf(Node.props(42));
      node.tell(new Node.Ping(7), getRef());
      expectMsg(new Node.Pong(7));
    }};
  }
}
