package org.example.akka.sparkmasterworker.master

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.example.akka.sparkmasterworker.common.{HeartBeat, RegisterWorkerInfo, RegisteredWorkerInfo, RemoveTimeOutWorker, SendHeartBeat, StartTimeOutWorker, WorkerInfo}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

class SparkMaster extends Actor{

  val workers = mutable.Map[String, WorkerInfo]()

  override def receive: Receive = {
    case "start" => {
      println("Master 启动了...")
      self ! StartTimeOutWorker
    }
    case StartTimeOutWorker => {
      println("master 开始定时检测 worker 心跳，看看有没有 worker 死了...")
      import context.dispatcher
      //说明
      //1. 0 不延时，立即执行定时器
      //2. 9000 表示每隔 9 秒执行一次
      //3. self: 表示发给自己
      //4. SendHeartBeat  发送的内容
      context.system.scheduler.schedule(new FiniteDuration(0, TimeUnit.MILLISECONDS), new FiniteDuration(9000, TimeUnit.MILLISECONDS), self, RemoveTimeOutWorker)
    }
    //检测哪些 worker 心跳超时（now - lastHeartBeat > 6000），并从 workers 中删除
    case RemoveTimeOutWorker => {
      val workerInfos = workers.values
      val nowTime = System.currentTimeMillis()
      workerInfos.filter(workerInfo => (nowTime - workerInfo.lastHeartBeat) > 6000)
                 .foreach(workerInfo => workers.remove(workerInfo.id))
      println(s"SparkMaster has ${workers.size} workers alive.")
    }
    case RegisterWorkerInfo(id, cpu, ram) => {
      // 接到 SparkWorker 的注册信息，注册其信息 WorkerInfo 到 hashMap 并给其返回注册成功信息 RegisteredWorkerInfo
      if(!workers.contains(id)) {
        val workerInfo = new WorkerInfo(id, cpu, ram)
        workers += ((id, workerInfo))
        println(s"SparkMaster has ${workers.size} workers.")
        sender ! RegisteredWorkerInfo
      }
    }
    case HeartBeat(id) => {
      // 更新对应的 Worker 的心跳时间
      val workerInfo = workers(id)
      workerInfo.lastHeartBeat = System.currentTimeMillis()
      println("Master 更新了 Worker " + id + " 的心跳时间...")
    }
  }
}

object SparkMaster {
  def main(args: Array[String]): Unit = {

    if(args.length != 3) {
      println("Please input the arguments in the form of \"masterIP masterPort masterActorName\"!")
      System.exit(-1)
    }

    val masterIP = args(0)
    val masterPort = args(1)
    val masterActorName = args(2)

    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=$masterIP
         |akka.remote.netty.tcp.port=$masterPort
      """.stripMargin)

    val sparkMasterSystem = ActorSystem("SparkMaster", config)
    val sparkMasterRef = sparkMasterSystem.actorOf(Props[SparkMaster], masterActorName)
    sparkMasterRef ! "start"
  }
}