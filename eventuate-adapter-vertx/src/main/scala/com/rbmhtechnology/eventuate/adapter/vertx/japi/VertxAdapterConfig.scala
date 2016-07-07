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

package com.rbmhtechnology.eventuate.adapter.vertx.japi

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.function.{ Predicate, Function => JFunction }
import java.util.{ Optional => JOption }

import akka.actor.ActorRef
import com.rbmhtechnology.eventuate.adapter.vertx.api._
import com.rbmhtechnology.eventuate.adapter.vertx.api.{ VertxAdapterConfig => SVertxAdapterConfig }

import scala.annotation.varargs
import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration

object VertxAdapterConfig {

  def fromLog(log: ActorRef): VertxReadAdapterConfigFactory =
    new VertxReadAdapterConfigFactory(log)

  @varargs
  def fromEndpoints(endpoints: String*): VertxWriteAdapterConfigFactory =
    new VertxWriteAdapterConfigFactory(endpoints.toVector)
}

class VertxAdapterConfig(private[vertx] val underlying: SVertxAdapterConfig)

abstract class CompletableVertxAdapterConfigFactory {
  def as(id: String): VertxAdapterConfig
}

class VertxReadAdapterConfigFactory(log: ActorRef) {
  import JavaConfigConverters._

  def publishTo(endpoint: String): VertxPublishAdapterConfigFactory =
    new VertxPublishAdapterConfigFactory(log, new VertxEndpointRouter(endpoint.asPF))

  def publishTo(routes: JFunction[Any, JOption[String]]): VertxPublishAdapterConfigFactory =
    new VertxPublishAdapterConfigFactory(log, new VertxEndpointRouter(routes.asScala))

  def sendTo(endpoint: String): VertxSendAdapterConfigFactory =
    new VertxSendAdapterConfigFactory(log, new VertxEndpointRouter(endpoint.asPF))

  def sendTo(routes: JFunction[Any, JOption[String]]): VertxSendAdapterConfigFactory =
    new VertxSendAdapterConfigFactory(log, new VertxEndpointRouter(routes.asScala))
}

class VertxPublishAdapterConfigFactory(log: ActorRef, endpoints: VertxEndpointRouter)
  extends CompletableVertxAdapterConfigFactory {
  import JavaConfigConverters._

  override def as(id: String): VertxAdapterConfig =
    VertxPublishAdapterConfig(id, log, endpoints).asJava
}

class VertxSendAdapterConfigFactory(log: ActorRef, endpointRouter: VertxEndpointRouter, deliveryMode: DeliveryMode = AtMostOnce)
  extends CompletableVertxAdapterConfigFactory {
  import JavaConfigConverters._

  def atLeastOnce(confirmationType: ConfirmationType, confirmationTimeout: Duration): VertxSendAdapterConfigFactory =
    new VertxSendAdapterConfigFactory(log, endpointRouter, AtLeastOnce(confirmationType.asScala, confirmationTimeout.asScala))

  override def as(id: String): VertxAdapterConfig =
    VertxSendAdapterConfig(id, log, endpointRouter, deliveryMode).asJava
}

class VertxWriteAdapterConfigFactory(endpoints: Seq[String]) {
  import JavaConfigConverters._

  def writeTo(log: ActorRef) = new CompletableVertxAdapterConfigFactory {
    override def as(id: String): VertxAdapterConfig =
      VertxWriteAdapterConfig(id, log, endpoints, { case _ => true }).asJava
  }

  def writeTo(log: ActorRef, filter: Predicate[Any]) = new CompletableVertxAdapterConfigFactory {
    override def as(id: String): VertxAdapterConfig =
      VertxWriteAdapterConfig(id, log, endpoints, toPF(filter)).asJava
  }

  private def toPF(p: Predicate[Any]): PartialFunction[Any, Boolean] = {
    case v => p.test(v)
  }
}

object JavaConfigConverters {
  import com.rbmhtechnology.eventuate.adapter.vertx.api.{ ConfirmationType => SConfirmationType }

  implicit class AsScalaConfirmationType(ct: ConfirmationType) {
    def asScala: SConfirmationType = ct match {
      case c: SingleConfirmation => Single
      case c: BatchConfirmation  => Batch(c.size)
    }
  }

  implicit class AsScalaDuration(d: Duration) {
    def asScala: FiniteDuration =
      FiniteDuration(d.toNanos, TimeUnit.NANOSECONDS)
  }

  implicit class AsScalaPF[-A, +B](f: JFunction[A, JOption[B]]) {
    def asScala: PartialFunction[A, B] = new PartialFunction[A, B] {
      override def isDefinedAt(v: A): Boolean = f.apply(v).isPresent
      override def apply(v: A): B = f.apply(v).get()
    }
  }

  implicit class StringAsScalaPF[-A](s: String) {
    def asPF: PartialFunction[A, String] = new PartialFunction[A, String] {
      override def isDefinedAt(v: A): Boolean = true
      override def apply(v: A): String = s
    }
  }

  implicit class AsJavaAdapterConfig(c: SVertxAdapterConfig) {
    def asJava: VertxAdapterConfig =
      new VertxAdapterConfig(c)
  }
}
