package org.example.akka.actor

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

// Actor 自己跟自己玩
class HelloActor extends Actor{
  // 查看源码 type Receive = scala.PartialFunction[scala.Any, scala.Unit]
  override def receive: Receive = { // PartialFunction 简写
    case "hello" => println("hello, too (respond hello)")
    case "ok" => println("ok, too (respond ok)")
    case "exit" => {
      println("Exiting...(respond exit)")
      context.stop(self)  // to stop ActorRef
      context.system.terminate()  // to terminate ActorSystem
    }
    case _ => println("match nothing")
  }
}

object HelloActorDemo {

  private val actorFactory = ActorSystem("actorFactory")
  // Props[HelloActor] 反射一个 HelloActor，也可 Props(new HelloActor)
  private val helloActorRef: ActorRef = actorFactory.actorOf(Props[HelloActor], "helloActor")

  def main(args: Array[String]): Unit = {
    helloActorRef ! "hello"
    helloActorRef ! "ok"
    helloActorRef ! "hah"
    helloActorRef ! "exit"
  }
}
