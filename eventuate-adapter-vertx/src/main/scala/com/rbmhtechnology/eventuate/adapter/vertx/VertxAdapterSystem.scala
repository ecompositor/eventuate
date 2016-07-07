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

import akka.actor.{ ActorSystem, Props }
import com.rbmhtechnology.eventuate.adapter.vertx.api._
import com.rbmhtechnology.eventuate.adapter.vertx.japi.rx.{ StorageProvider => RxStorageProvider }
import com.rbmhtechnology.eventuate.adapter.vertx.japi.{ StorageProvider => JStorageProvider, VertxAdapterSystemConfig => JVertxAdapterSystemConfig }
import io.vertx.core.Vertx
import io.vertx.rxjava.core.{ Vertx => RxVertx }

import scala.collection.immutable.Seq

object VertxAdapterSystem {

  import VertxConverters._

  def apply(config: VertxAdapterSystemConfig, vertx: Vertx, storageProvider: StorageProvider)(implicit system: ActorSystem): VertxAdapterSystem =
    new VertxAdapterSystem(config, vertx, storageProvider)

  def create(config: JVertxAdapterSystemConfig,
    vertx: Vertx,
    storageProvider: JStorageProvider,
    system: ActorSystem): VertxAdapterSystem =
    new VertxAdapterSystem(config.underlying, vertx, storageProvider.asScala)(system)

  def create(config: JVertxAdapterSystemConfig,
    vertx: RxVertx,
    storageProvider: RxStorageProvider,
    system: ActorSystem): VertxAdapterSystem =
    new VertxAdapterSystem(config.underlying, vertx, storageProvider.asScala)(system)
}

class VertxAdapterSystem private[vertx] (config: VertxAdapterSystemConfig, vertx: Vertx, storageProvider: StorageProvider)(implicit system: ActorSystem) {

  private def registerEventBusCodecs(): Unit = {
    config.codecClasses.foreach(c =>
      try {
        vertx.eventBus().registerDefaultCodec(c.asInstanceOf[Class[AnyRef]], AkkaSerializationMessageCodec(c))
      } catch {
        case e: IllegalStateException =>
          system.log.warning(s"An adapter codec for class ${c.getName} was configured, even though a default codec was already registered for this class.")
      })
  }

  def start(): Unit = {
    registerEventBusCodecs()
    val supervisor = system.actorOf(VertxSupervisor.props(adapters))
  }

  private def adapters: Seq[Props] =
    writeAdapters ++ readAdapters

  private def readAdapters: Seq[Props] = config.readAdapters.map {
    case VertxPublishAdapterConfig(id, log, endpointRouter) =>
      VertxPublisher.props(id, log, endpointRouter, vertx, storageProvider)

    case VertxSendAdapterConfig(id, log, endpointRouter, AtMostOnce) =>
      VertxSender.props(id, log, endpointRouter, vertx, storageProvider)

    case VertxSendAdapterConfig(id, log, endpointRouter, AtLeastOnce(Single, timeout)) =>
      VertxSingleConfirmationSender.props(id, log, endpointRouter, vertx, timeout)

    case VertxSendAdapterConfig(id, log, endpointRouter, AtLeastOnce(Batch(size), timeout)) =>
      VertxBatchConfirmationSender.props(id, log, endpointRouter, vertx, storageProvider, size, timeout)
  }

  private def writeAdapters: Seq[Props] = {
    if (config.writeAdapters.nonEmpty)
      Seq(VertxWriteRouter.props(config.writeAdapters, vertx))
    else
      Seq.empty
  }
}