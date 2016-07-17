/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import java.lang.IllegalStateException

//迭代器状态
class State
object DONE extends State //已经迭代完了
object READY extends State //已经准备好了,可以进行迭代了
object NOT_READY extends State //还没有准备好进行迭代,属于初始状态,让迭代器暂时没准备好,没准备好的意思就是下次还是会走makeNext方法
object FAILED extends State //迭代失败

/**
 * Transliteration of the iterator template in google collections. To implement an iterator
 * override makeNext and call allDone() when there is no more items
 * 迭代器模版,每一次返回元素T
 */
abstract class IteratorTemplate[T] extends Iterator[T] with java.util.Iterator[T] {
  
  private var state: State = NOT_READY //初始化迭代状态
  private var nextItem = null.asInstanceOf[T] //下一个元素内容,默认是null

  //直接返回下一个元素
  def next(): T = {
    if(!hasNext()) //判断是否还有下一个元素
      throw new NoSuchElementException() //如果没有下一个元素了,则抛异常
    state = NOT_READY
    if(nextItem == null)
      throw new IllegalStateException("Expected item but none found.")
    nextItem
  }
  
  //仅仅拿到下一个元素,并不需要进一步迭代
  def peek(): T = {
    if(!hasNext())
      throw new NoSuchElementException()
    nextItem
  }
  
  //true表示还有元素,false表示没有元素了
  def hasNext(): Boolean = {
    if(state == FAILED)
      throw new IllegalStateException("Iterator is in failed state")
    state match {
      case DONE => false //说明完成了,则返回false
      case READY => true
      case _ => maybeComputeNext()
    }
  }
  
  protected def makeNext(): T
  
  def maybeComputeNext(): Boolean = {
    state = FAILED
    nextItem = makeNext()
    if(state == DONE) {
      false
    } else {
      state = READY
      true
    }
  }
  
  //设置迭代完成
  protected def allDone(): T = {
    state = DONE
    null.asInstanceOf[T]
  }
  
  //不支持移除数据元素
  def remove = 
    throw new UnsupportedOperationException("Removal not supported")

  //重新设置迭代状态
  protected def resetState() {
    state = NOT_READY//让迭代器暂时没准备好,没准备好的意思就是下次还是会走makeNext方法
  }
}

