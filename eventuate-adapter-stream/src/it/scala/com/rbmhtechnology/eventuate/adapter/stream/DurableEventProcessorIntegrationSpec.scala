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
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl._
import akka.testkit.TestKit

import com.rbmhtechnology.eventuate._

import org.scalatest._

import scala.collection.immutable.Seq

class DurableEventProcessorIntegrationSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with SingleLocationSpecLeveldb {
  implicit val materializer: Materializer = ActorMaterializer()

  val SrcEmitterId = "src-emitter"
  val SrcLogId = "src-log"
  val ProcessorId = "processor"


  def durableEvent(payload: String, sequenceNr: Long): DurableEvent =
    DurableEvent(payload, SrcEmitterId, processId = SrcLogId, localLogId = SrcLogId, localSequenceNr = sequenceNr, vectorTimestamp = VectorTime(SrcLogId -> sequenceNr))

  "A DurableEventProcessor" must {
    "support stateless stream processing" in {
      def logic = Flow[Any].mapConcat(p => Seq(p.toString + "1", p.toString + "2"))
      val logicAB = logic.filterNot(_ == "b1")
      val logicC = logic.filter(_.startsWith("a"))

      val (src, snk) = TestSource.probe[DurableEvent]
        .via(DurableEventProcessor.statelessProcessor(ProcessorId, log, 2) {
          case "c" => logicC
          case _   => logicAB
        })
        .toMat(TestSink.probe[DurableEvent])(Keep.both)
        .run()

      snk.request(3)

      src.sendNext(durableEvent("a", 11))
      src.sendNext(durableEvent("b", 12))
      src.sendNext(durableEvent("c", 13))

      snk.expectNextN(3).map(_.payload) should be(Seq("a1", "a2", "b2"))
      snk.cancel()
    }
    "support stateful stream processing" in {
      def logic(s: Int, p: Any): (Int, Seq[String]) = {
        val ctr = if (p.toString.contains("b")) s + 1 else s
        (ctr, Seq(s"${p}1($ctr)", s"${p}2($ctr)"))
      }

      val (src, snk) = TestSource.probe[DurableEvent]
        .via(DurableEventProcessor.statefulProcessor(ProcessorId, log, 2, 0)(logic))
        .toMat(TestSink.probe[DurableEvent])(Keep.both)
        .run()

      snk.request(6)

      src.sendNext(durableEvent("a", 11))
      src.sendNext(durableEvent("b", 12))
      src.sendNext(durableEvent("c", 13))

      snk.expectNextN(6).map(_.payload) should be(Seq("a1(0)", "a2(0)", "b1(1)", "b2(1)", "c1(1)", "c2(1)"))
      snk.cancel()
    }
  }
}
