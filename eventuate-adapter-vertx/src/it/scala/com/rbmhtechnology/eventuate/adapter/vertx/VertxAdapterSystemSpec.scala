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

package com.rbmhtechnology.eventuate.adapter.vertx

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.rbmhtechnology.eventuate.adapter.vertx.api.{VertxAdapterConfig, VertxAdapterSystemConfig}
import com.rbmhtechnology.eventuate.log.EventLogWriter
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog
import com.rbmhtechnology.eventuate.utilities._
import com.rbmhtechnology.eventuate.{LocationCleanupLeveldb, ReplicationEndpoint}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}

import scala.collection.immutable.Seq

object VertxAdapterSystemSpec {
  val Config = ConfigFactory.defaultApplication()
    .withFallback(ConfigFactory.parseString(
      """
        |eventuate.log.replay-batch-size = 10
      """.stripMargin))

  case class Event(id: String)
}

class VertxAdapterSystemSpec extends TestKit(ActorSystem("test", VertxAdapterSystemSpec.Config))
  with WordSpecLike with MustMatchers with BeforeAndAfterAll with ActorStorage with StopSystemAfterAll with LocationCleanupLeveldb
  with VertxEnvironment with VertxEventBusProbes {

  import utilities._
  import VertxAdapterSystemSpec._

  val logName = "logA"
  val adapterId = "adapter1"
  var endpoint: ReplicationEndpoint = _

  override def config: Config = VertxAdapterSystemSpec.Config

  override def beforeAll(): Unit = {
    super.beforeAll()
    endpoint = new ReplicationEndpoint(id = "1", logNames = Set(logName), logFactory = logId => LeveldbEventLog.props(logId), connections = Set())
  }

  "A VertxAdapterSystem" must {
    "read events from an inbound log and deliver them to the Vert.x eventbus" in {
      val log = endpoint.logs(logName)
      val adapterSystemConfig = VertxAdapterSystemConfig()
        .addAdapter(VertxAdapterConfig.fromLog(log)
          .publishTo {
            case _ => endpoint1
          }
          .as("adapter1"))
        .registerDefaultCodecFor(classOf[Event])

      val vertxAdapterSystem = VertxAdapterSystem(adapterSystemConfig, vertx, actorStorageProvider())
      val logWriter = new EventLogWriter("w1", endpoint.logs(logName))
      val storageName = adapterId

      endpoint.activate()
      vertxAdapterSystem.start()

      val write1 = logWriter.write(Seq(Event("1"))).await.head

      storageProbe.expectMsg(read(storageName))
      storageProbe.reply(0L)

      storageProbe.expectMsg(write(storageName)(1))
      storageProbe.reply(1L)

      endpoint1Probe.expectVertxMsg(body = Event("1"))

      val write2 = logWriter.write(Seq(Event("2"), Event("3"), Event("4"))).await

      storageProbe.expectMsg(write(storageName)(2))
      storageProbe.reply(2L)

      endpoint1Probe.receiveNVertxMsg[Event](3).map(_.body) must be(write2.map(_.payload))

      storageProbe.expectMsg(write(storageName)(4))
      storageProbe.reply(4L)
    }
  }
}
