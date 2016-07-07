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

import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.adapter.vertx.api.{VertxAdapterConfig, VertxWriteAdapterConfig}
import com.rbmhtechnology.eventuate.utilities._
import com.rbmhtechnology.eventuate.{EventsourcedView, SingleLocationSpecLeveldb}
import io.vertx.core.eventbus.{Message, ReplyException}
import org.scalatest.{MustMatchers, WordSpecLike}

import scala.collection.immutable.Seq
import scala.concurrent.{Future, Promise}

object VertxWriteRouterSpec {

  case class ReadEvent(emitterId: String, event: Any)

  class LogReader(val id: String, val eventLog: ActorRef, receiver: ActorRef) extends EventsourcedView {
    override def onCommand: Receive = {
      case _ =>
    }

    override def onEvent: Receive = {
      case event => receiver ! ReadEvent(lastEmitterId, event)
    }
  }

  class FailingWriteLog(eventLog: ActorRef, failureEvents: Seq[Any]) extends Actor {
    override def receive: Receive = {
      case Write(events, _, replyTo, correlationId, instanceId) if failureEvents.intersect(events.map(_.payload)).nonEmpty =>
        replyTo ! WriteFailure(events, new RuntimeException("error"), correlationId, instanceId)

      case cmd =>
        eventLog forward cmd
    }
  }
}

class VertxWriteRouterSpec extends TestKit(ActorSystem("test", VertxPublisherSpec.Config))
  with WordSpecLike with MustMatchers with SingleLocationSpecLeveldb with StopSystemAfterAll with VertxEnvironment {

  import utilities._
  import VertxHandlerConverters._
  import VertxWriteRouterSpec._
  import system.dispatcher

  var logA: ActorRef = _
  var logB: ActorRef = _

  var logAProbe: TestProbe = _
  var logBProbe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    logA = system.actorOf(logProps(logId("logA")))
    logB = system.actorOf(logProps(logId("logB")))

    logAProbe = TestProbe()
    logBProbe = TestProbe()

    logReader("logAReader", logA, logAProbe.ref)
    logReader("logBReader", logB, logBProbe.ref)
  }

  def logId(id: String): String =
    s"$id-${UUID.randomUUID().toString}"

  def waitForStartup(): Unit = {
    Thread.sleep(500)
  }

  def logReader(id: String, log: ActorRef, receiver: ActorRef): ActorRef =
    system.actorOf(Props(new LogReader(id, log, receiver)))

  def writeRouter(configs: VertxWriteAdapterConfig*): ActorRef = {
    val actor = system.actorOf(VertxWriteRouter.props(configs.toVector, vertx))
    waitForStartup()
    actor
  }

  def failingWriteLog(log: ActorRef, failureEvents: Seq[Any] = Seq()): ActorRef = {
    system.actorOf(Props(new FailingWriteLog(log, failureEvents)))
  }

  def persist(endpoint: String, event: Any): Future[Any] = {
    val promise = Promise[Message[Any]]()
    vertx.eventBus().send[Any](endpoint, event, promise.asVertxHandler)
    promise.future.map(_.body)
  }

  "A VertxWriteRouter" when {
    "receiving events from the eventbus" must {
      "route events from a single source-endpoint to a single target-log" in {
        writeRouter(
          VertxAdapterConfig.fromEndpoints(endpoint1).writeTo(logA).as("id1")
        )

        persist(endpoint1, "ev-1")
        persist(endpoint1, "ev-2")

        logAProbe.receiveInAnyOrder(
          ReadEvent(emitterId = "id1", event = "ev-1"),
          ReadEvent(emitterId = "id1", event = "ev-2"))
      }
      "route events from multiple source-endpoints to a single target-log" in {
        writeRouter(
          VertxAdapterConfig.fromEndpoints(endpoint1, endpoint2).writeTo(logA).as("id1")
        )

        persist(endpoint1, "ev-1")
        persist(endpoint2, "ev-2")

        logAProbe.receiveInAnyOrder(
          ReadEvent(emitterId = "id1", event = "ev-1"),
          ReadEvent(emitterId = "id1", event = "ev-2"))
      }
      "route events from multiple source-endpoints to multiple target-logs" in {
        writeRouter(
          VertxAdapterConfig.fromEndpoints(endpoint1).writeTo(logA).as("id-a"),
          VertxAdapterConfig.fromEndpoints(endpoint2).writeTo(logB).as("id-b")
        )

        persist(endpoint1, "ev-a1")
        persist(endpoint1, "ev-a2")

        persist(endpoint2, "ev-b1")
        persist(endpoint2, "ev-b2")

        logAProbe.receiveInAnyOrder(
          ReadEvent(emitterId = "id-a", event = "ev-a1"),
          ReadEvent(emitterId = "id-a", event = "ev-a2"))

        logBProbe.receiveInAnyOrder(
          ReadEvent(emitterId = "id-b", event = "ev-b1"),
          ReadEvent(emitterId = "id-b", event = "ev-b2"))
      }
      "route events from a single source-endpoint to multiple target-logs" in {
        writeRouter(
          VertxAdapterConfig.fromEndpoints(endpoint1).writeTo(logA).as("id-a"),
          VertxAdapterConfig.fromEndpoints(endpoint1).writeTo(logB).as("id-b")
        )

        persist(endpoint1, "ev-1")

        logAProbe.expectMsg(ReadEvent(emitterId = "id-a", event = "ev-1"))
        logBProbe.expectMsg(ReadEvent(emitterId = "id-b", event = "ev-1"))
      }
      "route events from a single source-endpoint to a single target-log with multiple ids" in {
        writeRouter(
          VertxAdapterConfig.fromEndpoints(endpoint1).writeTo(logA).as("id-a1"),
          VertxAdapterConfig.fromEndpoints(endpoint1).writeTo(logA).as("id-a2")
        )

        persist(endpoint1, "ev-1")

        logAProbe.receiveInAnyOrder(
          ReadEvent(emitterId = "id-a1", event = "ev-1"),
          ReadEvent(emitterId = "id-a2", event = "ev-1"))
      }
      "filter events from a single source-endpoint to a single target-log" in {
        writeRouter(
          VertxAdapterConfig.fromEndpoints(endpoint1).writeTo(logA, { case s: String if !s.contains("filter") => true }).as("id-a1")
        )

        persist(endpoint1, "ev-1")
        persist(endpoint1, "ev-2-filter")
        persist(endpoint1, "ev-3")
        persist(endpoint1, "ev-4-filter")

        logAProbe.receiveInAnyOrder(
          ReadEvent(emitterId = "id-a1", event = "ev-1"),
          ReadEvent(emitterId = "id-a1", event = "ev-3"))
      }
      "filter events from a single source-endpoint to multiple target-logs" in {
        writeRouter(
          VertxAdapterConfig.fromEndpoints(endpoint1).writeTo(logA, { case s: String if s.contains("a") => true }).as("id-a"),
          VertxAdapterConfig.fromEndpoints(endpoint1).writeTo(logB, { case s: String if s.contains("b") => true }).as("id-b")
        )

        persist(endpoint1, "ev-a1")
        persist(endpoint1, "ev-b1")

        logAProbe.expectMsg(ReadEvent(emitterId = "id-a", event = "ev-a1"))
        logBProbe.expectMsg(ReadEvent(emitterId = "id-b", event = "ev-b1"))
      }
      "filter events from a single source-endpoint to a single target-logs with multiple ids" in {
        writeRouter(
          VertxAdapterConfig.fromEndpoints(endpoint1).writeTo(logA, { case s: String if s.contains("a") => true }).as("id-a"),
          VertxAdapterConfig.fromEndpoints(endpoint1).writeTo(logA, { case s: String if s.contains("b") => true }).as("id-b")
        )

        persist(endpoint1, "ev-a1")
        persist(endpoint1, "ev-b1")

        logAProbe.receiveInAnyOrder(
          ReadEvent(emitterId = "id-a", event = "ev-a1"),
          ReadEvent(emitterId = "id-b", event = "ev-b1"))
      }
      "respond with the event in case of success" in {
        writeRouter(
          VertxAdapterConfig.fromEndpoints(endpoint1).writeTo(logA).as("id1")
        )

        persist(endpoint1, "ev-1").await must be("ev-1")
        persist(endpoint1, "ev-2").await must be("ev-2")
      }
      "respond with a success for filtered events" in {
        writeRouter(
          VertxAdapterConfig.fromEndpoints(endpoint1).writeTo(logA, { case s: String if !s.contains("filter") => true }).as("id-a1")
        )

        persist(endpoint1, "ev-1-filter").await must be("ev-1-filter")
        persist(endpoint1, "ev-2").await must be("ev-2")

        logAProbe.expectMsg(ReadEvent(emitterId = "id-a1", event = "ev-2"))
      }
      "respond with the failure in case of an error" in {
        writeRouter(
          VertxAdapterConfig.fromEndpoints(endpoint1).writeTo(logA).as("id1")
        )

        persist("invalid-endpoint", "ev-1").failed.await mustBe a[ReplyException]
      }
    }
    "encountering an error while persisting events" must {
      "return a failure for a failed event" in {
        writeRouter(
          VertxAdapterConfig.fromEndpoints(endpoint1).writeTo(failingWriteLog(logA, Seq("ev-fail"))).as("id1")
        )

        persist(endpoint1, "ev-1").await must be("ev-1")
        persist(endpoint1, "ev-fail").failed.await mustBe a[ReplyException]
        persist(endpoint1, "ev-2").await must be("ev-2")

        logAProbe.receiveInAnyOrder(
          ReadEvent(emitterId = "id1", event = "ev-1"),
          ReadEvent(emitterId = "id1", event = "ev-2"))
      }
    }
  }
}
