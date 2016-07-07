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

import com.rbmhtechnology.eventuate.adapter.vertx.api.{ VertxAdapterSystemConfig => SVertxAdapterSystemConfig }
import scala.annotation.varargs
import java.util.{ Collection => JCollection }

object VertxAdapterSystemConfig {

  def create(): VertxAdapterSystemConfig =
    new VertxAdapterSystemConfig(SVertxAdapterSystemConfig())
}

class VertxAdapterSystemConfig(val underlying: SVertxAdapterSystemConfig) {
  import scala.collection.JavaConverters._

  @varargs
  def addAdapter(first: VertxAdapterConfig, rest: VertxAdapterConfig*): VertxAdapterSystemConfig =
    new VertxAdapterSystemConfig(underlying.addAdapter(first.underlying, rest.map(_.underlying): _*))

  def addAdapters(adapterConfigurations: JCollection[VertxAdapterConfig]): VertxAdapterSystemConfig =
    new VertxAdapterSystemConfig(underlying.addAdapters(adapterConfigurations.asScala.toVector.map(_.underlying)))

  @varargs
  def registerDefaultCodecFor(first: Class[_], rest: Class[_]*): VertxAdapterSystemConfig =
    new VertxAdapterSystemConfig(underlying.registerDefaultCodecFor(first, rest: _*))

  def registerDefaultCodecForAll(classes: JCollection[Class[_]]): VertxAdapterSystemConfig =
    new VertxAdapterSystemConfig(underlying.registerDefaultCodecForAll(classes.asScala.toVector))
}
