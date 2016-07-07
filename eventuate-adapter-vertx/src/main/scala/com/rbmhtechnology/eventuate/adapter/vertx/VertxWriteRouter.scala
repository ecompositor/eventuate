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

import akka.actor.{ Actor, ActorRef, Props }
import com.rbmhtechnology.eventuate.adapter.vertx.VertxWriter.PersistMessage
import com.rbmhtechnology.eventuate.adapter.vertx.api.VertxWriteAdapterConfig
import io.vertx.core.Vertx
import io.vertx.core.eventbus.{ Message, MessageConsumer }

object VertxWriteRouter {

  case class Route(sourceEndpoint: String, destinationLog: ActorRef, filter: PartialFunction[Any, Boolean])

  def props(config: Seq[VertxWriteAdapterConfig], vertx: Vertx): Props =
    Props(new VertxWriteRouter(config, vertx))
}

class VertxWriteRouter(configs: Seq[VertxWriteAdapterConfig], vertx: Vertx) extends Actor {
  import VertxHandlerConverters._
  import VertxWriteRouter._

  val writers = configs
    .map { c => c.id -> context.actorOf(VertxWriter.props(c.id, c.log)) }
    .toMap

  val consumers = configs
    .flatMap { c => c.endpoints.distinct.map(e => Route(e, writers(c.id), c.filter)) }
    .groupBy(_.sourceEndpoint)
    .map { case (endpoint, routes) => installMessageConsumer(endpoint, routes) }

  private def installMessageConsumer(endpoint: String, routes: Seq[Route]): MessageConsumer[Any] = {
    val handler = (msg: Message[Any]) => {
      routes.foreach { route =>
        if (route.filter.applyOrElse(msg.body(), (_: Any) => false)) {
          route.destinationLog ! PersistMessage(msg.body(), msg)
        } else {
          msg.reply(msg.body)
        }
      }
    }
    vertx.eventBus().consumer[Any](endpoint, handler.asVertxHandler)
  }

  override def receive: Receive = Actor.emptyBehavior

  override def postStop(): Unit = {
    consumers.foreach(_.unregister())
  }
}