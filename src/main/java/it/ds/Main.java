package it.ds;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;

import java.io.IOException;

public class Main {
  public static void main(String[] args) throws IOException {
    final ActorSystem system = ActorSystem.create("ds");

    ActorRef a = system.actorOf(Node.props(0), "node0");
    ActorRef b = system.actorOf(Node.props(1), "node1");

    a.tell(new Node.Start(b), ActorRef.noSender());

    System.out.println(">>> Press ENTER to exit <<<");
    System.in.read();
    system.terminate();
  }
}
