package org.example.akka.sparkmasterworker.worker

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.example.akka.sparkmasterworker.common.{HeartBeat, RegisterWorkerInfo, RegisteredWorkerInfo, SendHeartBeat}

import scala.concurrent.duration.FiniteDuration

class SparkWorker(masterIP: String, masterPort: Int, masterActorName: String) extends Actor{

  // sparkMasterPorxy 是 SparkMaster 的代理/引用 ref
  var sparkMasterPorxy :ActorSelection = _
  val id = UUID.randomUUID().toString // SparkWorkder ID

  override def preStart(): Unit = {
    println("调用 preStart(): Unit，初始化 sparkMasterPorxy...")
    sparkMasterPorxy = context.actorSelection(s"akka.tcp://SparkMaster@${masterIP}:${masterPort}/user/${masterActorName}")
    println("sparkMasterPorxy=" + sparkMasterPorxy)
  }

  override def receive: Receive = {
    case "start" => {
      println("Worker 启动了...")
      // SparkWorker 启动后，就注册到 SparkMaster
      sparkMasterPorxy ! RegisterWorkerInfo(id, 16, 16*1024)
    }
    case RegisteredWorkerInfo => {
      println(s"Worker $id 注册成功!")
      // 当注册成功后，就定义一个定时器，每隔一定时间，发送 SendHeartBeat 给自己
      import context.dispatcher
      //说明
      //1. 0 不延时，立即执行定时器
      //2. 3000 表示每隔 3 秒执行一次
      //3. self: 表示发给自己
      //4. SendHeartBeat  发送的内容
      context.system.scheduler.schedule(new FiniteDuration(0, TimeUnit.MILLISECONDS), new FiniteDuration(3000, TimeUnit.MILLISECONDS), self, SendHeartBeat)
    }
    case SendHeartBeat =>{
      println("Worker = " + id + "给 Master 发送心跳...")
      sparkMasterPorxy ! HeartBeat(id)
    }
  }
}

object SparkWorker {
  def main(args: Array[String]): Unit = {

    if(args.length != 6) {
      println("Please input the arguments in the form of \"workerIP workerPort workerActorName masterIP masterPort masterActorName\"!")
      System.exit(-1)
    }
    val workerIP = args(0)
    val workerPort = args(1)
    val workerActorName = args(2)
    val masterIP = args(3)
    val masterPort = args(4)
    val masterActorName = args(5)

    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=$workerIP
         |akka.remote.netty.tcp.port=$workerPort
      """.stripMargin)
    val sparkWorkerSystem = ActorSystem("SparkWorker", config)
    val sparkWorkerRef = sparkWorkerSystem.actorOf(Props(new SparkWorker(masterIP, masterPort.toInt, masterActorName)), workerActorName)
    sparkWorkerRef ! "start"
  }
}
