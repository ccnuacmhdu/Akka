package org.example.akka.sparkmasterworker.common

// worker 注册信息
case class RegisterWorkerInfo(id: String, cpu: Int, ram: Int)
// 这个是 WorkerInfo, 这个信息将来是保存到 master 的 hm(该 hashmap 是用于管理 worker)
// 将来这个 WorkerInfo 会扩展（比如增加 worker 上一次的心跳时间）
class WorkerInfo(val id: String, val cpu: Int, val ram: Int) {
  var lastHeartBeat: Long = System.currentTimeMillis
}
// 当 worker 注册成功，服务器返回一个 RegisteredWorkerInfo 对象
case object RegisteredWorkerInfo


// worker 每隔一定时间由定时器发给自己的一个消息
case object SendHeartBeat
// worker 每隔一定时间收到定时器发的 SendHeartBeat 后，向 master 发送 HeartBeat 心跳
case class HeartBeat(id: String)


// master 给自己发消息，触发检查超时 worker 的任务
case object StartTimeOutWorker
// master 定时检查 worker 超时的任务给 master 自己发消息 RemoveTimeOutWorker，开始检测超时 worker 并删除
case object RemoveTimeOutWorker