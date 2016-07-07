/*
 * Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.example.vertx

import java.io.File
import java.nio.file.NoSuchFileException
import java.util.UUID

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.pattern.ask
import akka.util.Timeout
import com.rbmhtechnology.eventuate.adapter.vertx._
import com.rbmhtechnology.eventuate.adapter.vertx.api._
import com.rbmhtechnology.eventuate.log.EventLogWriter
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog
import com.rbmhtechnology.eventuate.{ EventsourcedView, ReplicationConnection, ReplicationEndpoint }
import io.vertx.core._
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
import io.vertx.core.file.FileSystemException
import io.vertx.core.json.JsonObject

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Random, Success }

object LogNames {
  val logA = "log_S_A"
  val logB = "log_S_B"
}

object Endpoints {
  val Processor = "eb-address:logA-processor"
  val PublishReceiver = "eb-address:logB-publish-receiver"
  val Writer = "eb-address:logB-writer"
}

case class Event(id: String)

object VertxAdapterExample extends App {

  import ExampleVertxExtensions._

  implicit val timeout = Timeout(5.minutes)
  implicit val system = ActorSystem(ReplicationConnection.DefaultRemoteSystemName)
  val vertx = Vertx.vertx()

  import system.dispatcher

  val endpoint = new ReplicationEndpoint(id = "id1", logNames = Set(LogNames.logA, LogNames.logB),
    logFactory = logId => LeveldbEventLog.props(logId), connections = Set())

  val logA = endpoint.logs(LogNames.logA)
  val logB = endpoint.logs(LogNames.logB)

  val adapterSystemConfig =
    VertxAdapterSystemConfig()
      .addAdapter(
        VertxAdapterConfig.fromLog(logA)
          .sendTo { case _ => Endpoints.Processor }
          .atLeastOnce(confirmationType = Batch(2), confirmationTimeout = 2.seconds)
          .as("logA-processor"))
      .addAdapter(
        VertxAdapterConfig.fromEndpoints(Endpoints.Writer)
          .writeTo(logB)
          .as("logB-writer"))
      .addAdapter(VertxAdapterConfig.fromLog(logB)
        .publishTo { case _ => Endpoints.PublishReceiver }
        .as("logB-publisher"))
      .registerDefaultCodecFor(classOf[Event])

  val adapterSystem: VertxAdapterSystem = VertxAdapterSystem(adapterSystemConfig, vertx, new DiskStorageProvider("target/progress/vertx-scala", vertx))

  (for {
    _ <- vertx.deploy[ProcessorVerticle]()
    _ <- vertx.deploy[ReaderVerticle](new DeploymentOptions().setConfig(new JsonObject().put("name", "v_reader-1")))
    i <- vertx.deploy[ReaderVerticle](new DeploymentOptions().setConfig(new JsonObject().put("name", "v_reader-2")))
  } yield i).onComplete {
    case Success(res) =>
      endpoint.activate()
      adapterSystem.start()

    case Failure(err) =>
      println(s"Vert.x startup failed with $err")
  }

  val eventCount = 10
  val writer = new EventLogWriter("writer", logA)
  val reader = system.actorOf(Props(new EventLogReader("reader", logB, eventCount)))

  val runId = UUID.randomUUID().toString.take(5)
  (1 to eventCount) map (i => Event(s"[$runId]-$i")) foreach { event =>
    writer.write(List(event))
    Thread.sleep(100)
  }

  reader.ask("notifyOnComplete").onComplete { _ =>
    Thread.sleep(500)
    vertx.close()
    system.terminate()
    println("--- finished ---")
  }
}

class ProcessorVerticle extends AbstractVerticle {

  import VertxHandlerConverters._

  val r = Random
  var confirmedEvents = Set[Event]()

  override def start(): Unit = {
    val messageWriter = vertx.eventBus().sender[Event](Endpoints.Writer)

    vertx.eventBus().consumer[Event](Endpoints.Processor, new Handler[Message[Event]] {
      override def handle(msg: Message[Event]): Unit = {
        val ev = msg.body()

        if (confirmedEvents.contains(ev)) {
          msg.reply(null)
        } else if (r.nextFloat() < 0.4) {
          println(s"[v_processor] dropped   [$ev]")
        } else {
          println(s"[v_processor] processed [$ev]")

          messageWriter.send[Event](ev.copy(id = s"*processed*${ev.id}"), { (ar: AsyncResult[Message[Event]]) =>
            if (ar.succeeded()) {
              confirmedEvents = confirmedEvents + ev
              msg.reply(null)
            } else {
              println(s"[verticle] persist failed with: ${ar.cause().getMessage}")
            }
          }.asVertxHandler)
        }
      }
    })
  }
}

class ReaderVerticle extends AbstractVerticle {

  import VertxHandlerConverters._

  override def start(): Unit = {
    vertx.eventBus().consumer[Event](Endpoints.PublishReceiver).handler({ (msg: Message[Event]) =>
      println(s"[${config.getString("name")}]  received  [${msg.body}]")
    }.asVertxHandler)
  }
}

class EventLogReader(val id: String, val eventLog: ActorRef, val eventCount: Int) extends EventsourcedView {

  var subscribers: List[ActorRef] = List.empty
  var eventsRead = 0

  override def onCommand: Receive = {
    case "notifyOnComplete" =>
      subscribers = sender() +: subscribers
    case "eventRead" =>
      eventsRead = eventsRead + 1
      if (eventsRead == eventCount) {
        subscribers.foreach(_ ! "finished")
      }
  }

  override def onEvent: Receive = {
    case ev =>
      println(s"[e_reader]    received  [$ev]")
      if (!recovering) {
        self ! "eventRead"
      }
  }
}

class DiskStorageProvider(path: String, vertx: Vertx)(implicit system: ActorSystem) extends StorageProvider {

  import ExampleVertxExtensions._

  new File(path).mkdirs()

  override def readProgress(logName: String)(implicit executionContext: ExecutionContext): Future[Long] = {
    val promise = Promise[Buffer]()
    vertx.fileSystem().readFile(path(logName), promise.asVertxHandler)
    promise.future
      .map(_.toString().toLong)
      .recover {
        case err: FileSystemException if err.getCause.isInstanceOf[NoSuchFileException] => 0L
      }
  }

  override def writeProgress(logName: String, sequenceNr: Long)(implicit executionContext: ExecutionContext): Future[Long] = {
    val promise = Promise[Void]()
    vertx.fileSystem().writeFile(path(logName), Buffer.buffer(sequenceNr.toString), promise.asVertxHandler)
    promise.future
      .map(_ => sequenceNr)
  }

  def path(logName: String): String =
    s"$path/progress-$logName.txt"
}
