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

import akka.actor.{ ActorRef, Props }
import com.rbmhtechnology.eventuate.adapter.vertx.api.{ StorageProvider, VertxEndpointRouter }
import io.vertx.core.Vertx

private[eventuate] object VertxSender {
  def props(id: String, eventLog: ActorRef, endpointRouter: VertxEndpointRouter, vertx: Vertx, storageProvider: StorageProvider): Props =
    Props(new VertxSender(id, eventLog, endpointRouter, vertx, storageProvider))
      .withDispatcher("eventuate.log.dispatchers.write-dispatcher")
}

private[eventuate] class VertxSender(val id: String, val eventLog: ActorRef, val endpointRouter: VertxEndpointRouter, val vertx: Vertx, val storageProvider: StorageProvider)
  extends VertxReader[Long, Long] with AtMostOnceDelivery with MessageSender with SequenceNumberProgressStore {
}
