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

import akka.NotUsed
import akka.actor.ActorRef
import akka.stream._
import akka.stream.scaladsl._

import com.rbmhtechnology.eventuate.DurableEvent

import scala.collection.immutable.Seq

object DurableEventProcessor {
  import DurableEventWriter._

  def statelessProcessor[M](id: String, eventLog: ActorRef, maxBatchSize: Int)(logic: Any => Graph[FlowShape[Any, Any], M]): Flow[DurableEvent, DurableEvent, NotUsed] =
    Flow[DurableEvent].flatMapConcat { event =>
      Source.single(event.payload).via(logic(event.payload)).fold(Seq.empty[DurableEvent])(_ :+ event.copy(_))
    }.via(batchWriterN(id, eventLog, maxBatchSize))

  def statefulProcessor[S, O](id: String, eventLog: ActorRef, maxBatchSize: Int, zero: S)(logic: (S, Any) => (S, Seq[O])): Flow[DurableEvent, DurableEvent, NotUsed] =
    Flow[DurableEvent].via(statefulTransformer(zero)(logic)).via(batchWriterN(id, eventLog, maxBatchSize))

  def statefulTransformer[S, O](zero: S)(logic: (S, Any) => (S, Seq[O])): Flow[DurableEvent, Seq[DurableEvent], NotUsed] =
    Flow.fromGraph[DurableEvent, Seq[DurableEvent], NotUsed](GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val unzip = builder.add(UnzipWith[DurableEvent, Any, DurableEvent](
        event => (event.payload, event)))

      val zip = builder.add(ZipWith[Seq[Any], DurableEvent, Seq[DurableEvent]] {
        case (payloads, event) => payloads.map(p => event.copy(payload = p))
      })

      val transform = Flow[Any].scan((zero, Seq.empty[O])) {
        case ((s, _), p) => logic(s, p)
      }.map(_._2).drop(1)

      unzip.out0 ~> transform ~> zip.in0
      unzip.out1 ~> zip.in1

      FlowShape(unzip.in, zip.out)
    })
}
