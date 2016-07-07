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

import scala.collection.immutable.Seq
import scalaz.Scalaz._
import scalaz._

object VertxAdapterSystemConfig {

  def apply(): VertxAdapterSystemConfig =
    VertxAdapterSystemConfig(Seq.empty, Seq.empty)

  private def apply(adapterConfigurations: Seq[VertxAdapterConfig], codecClasses: Seq[Class[_]]): VertxAdapterSystemConfig = {
    validateConfigurations(adapterConfigurations) match {
      case \/-(cs) =>
        new VertxAdapterSystemConfig(cs, codecClasses)
      case -\/(errs) =>
        throw new IllegalArgumentException(s"Invalid configuration given. Cause:\n${errs.mkString("\n")}")
    }
  }

  private def validateConfigurations(configs: Seq[VertxAdapterConfig]): \/[Seq[String], Seq[VertxAdapterConfig]] = {
    val invalid = configs.groupBy(_.id).filter(c => c._2.size > 1)

    if (invalid.isEmpty)
      configs.right
    else
      invalid.map(c => s"Ambigious definition for adapter with id '${c._1}' given. An id may only be used once.")
        .toVector
        .left
  }
}

class VertxAdapterSystemConfig(private[vertx] val configurations: Seq[VertxAdapterConfig],
  private[vertx] val codecClasses: Seq[Class[_]]) {
  val readAdapters: Seq[VertxReadAdapterConfig] =
    configurations.collect({ case c: VertxReadAdapterConfig => c })

  val writeAdapters: Seq[VertxWriteAdapterConfig] =
    configurations.collect({ case c: VertxWriteAdapterConfig => c })

  def addAdapter(first: VertxAdapterConfig, rest: VertxAdapterConfig*): VertxAdapterSystemConfig =
    addAdapters(first +: rest.toVector)

  def addAdapters(adapterConfigurations: Seq[VertxAdapterConfig]): VertxAdapterSystemConfig =
    VertxAdapterSystemConfig(configurations ++ adapterConfigurations, codecClasses)

  def registerDefaultCodecFor(first: Class[_], rest: Class[_]*): VertxAdapterSystemConfig =
    registerDefaultCodecForAll(first +: rest.toVector)

  def registerDefaultCodecForAll(classes: Seq[Class[_]]): VertxAdapterSystemConfig =
    VertxAdapterSystemConfig(configurations, codecClasses ++ classes)
}
