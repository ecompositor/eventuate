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

package com.rbmhtechnology.eventuate.adapter.stream

import akka.actor._
import akka.stream._
import akka.stream.testkit.scaladsl._
import akka.testkit._

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.log.EventLogWriter
import com.rbmhtechnology.eventuate.utilities._

import org.scalatest._

class DurableEventReaderIntegrationSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with SingleLocationSpecLeveldb {
  implicit val materializer: Materializer = ActorMaterializer()

  "A DurableEventReader" must {
    "emit events from an event log" in {
      val writer = new EventLogWriter("writer", log)
      val source = DurableEventSource(log, 16)
      val probe = source.take(4).runWith(TestSink.probe)

      val abc = writer.write(List("a", "b", "c")).await
      probe.request(3).expectNextN(abc)

      val xyz = writer.write(List("x", "y", "z")).await
      probe.request(3).expectNext(xyz.head).expectComplete()
    }
  }
}
