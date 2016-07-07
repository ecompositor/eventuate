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

package com.rbmhtechnology.eventuate.adapter.vertx.api

import akka.actor.ActorRef

import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration

sealed trait VertxAdapterConfig {
  def id: String
  def log: ActorRef
}
sealed trait VertxReadAdapterConfig extends VertxAdapterConfig {
  def endpointRouter: VertxEndpointRouter
}

case class VertxPublishAdapterConfig(id: String, log: ActorRef, endpointRouter: VertxEndpointRouter) extends VertxReadAdapterConfig
case class VertxSendAdapterConfig(id: String, log: ActorRef, endpointRouter: VertxEndpointRouter, deliveryMode: DeliveryMode) extends VertxReadAdapterConfig
case class VertxWriteAdapterConfig(id: String, log: ActorRef, endpoints: Seq[String], filter: PartialFunction[Any, Boolean]) extends VertxAdapterConfig

sealed trait ConfirmationType {}
case object Single extends ConfirmationType
case class Batch(size: Int) extends ConfirmationType

sealed trait DeliveryMode {}
case object AtMostOnce extends DeliveryMode
case class AtLeastOnce(confirmationType: ConfirmationType, confirmationTimeout: FiniteDuration) extends DeliveryMode

object VertxEndpointRouter {

  def route(f: PartialFunction[Any, String]): VertxEndpointRouter =
    new VertxEndpointRouter(f)

  def routeAllTo(s: String): VertxEndpointRouter =
    new VertxEndpointRouter({ case _ => s })
}

class VertxEndpointRouter(f: PartialFunction[Any, String]) {
  val endpoint: Any => Option[String] = f.lift
}

object VertxAdapterConfig {

  def fromLog(log: ActorRef): VertxReadAdapterConfigFactory =
    new VertxReadAdapterConfigFactory(log)

  def fromEndpoints(endpoints: String*): VertxWriteAdapterConfigFactory =
    new VertxWriteAdapterConfigFactory(endpoints.toVector)
}

trait CompletableVertxAdapterConfigFactory {
  def as(id: String): VertxAdapterConfig
}

class VertxReadAdapterConfigFactory(log: ActorRef) {

  def publishTo(routes: PartialFunction[Any, String]): VertxPublishAdapterConfigFactory =
    new VertxPublishAdapterConfigFactory(log, VertxEndpointRouter.route(routes))

  def sendTo(routes: PartialFunction[Any, String]): VertxSendAdapterConfigFactory =
    new VertxSendAdapterConfigFactory(log, VertxEndpointRouter.route(routes))
}

class VertxPublishAdapterConfigFactory(log: ActorRef, endpoints: VertxEndpointRouter)
  extends CompletableVertxAdapterConfigFactory {

  override def as(id: String): VertxAdapterConfig =
    VertxPublishAdapterConfig(id, log, endpoints)
}

class VertxSendAdapterConfigFactory(log: ActorRef, endpointRouter: VertxEndpointRouter, deliveryMode: DeliveryMode = AtMostOnce)
  extends CompletableVertxAdapterConfigFactory {

  def atLeastOnce(confirmationType: ConfirmationType, confirmationTimeout: FiniteDuration): VertxSendAdapterConfigFactory =
    new VertxSendAdapterConfigFactory(log, endpointRouter, AtLeastOnce(confirmationType, confirmationTimeout))

  override def as(id: String): VertxAdapterConfig =
    VertxSendAdapterConfig(id, log, endpointRouter, deliveryMode)
}

class VertxWriteAdapterConfigFactory(endpoints: Seq[String]) {

  def writeTo(log: ActorRef, filter: PartialFunction[Any, Boolean] = { case _ => true }) = new CompletableVertxAdapterConfigFactory {
    override def as(id: String): VertxWriteAdapterConfig =
      VertxWriteAdapterConfig(id, log, endpoints, filter)
  }
}
