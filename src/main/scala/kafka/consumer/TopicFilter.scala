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

package kafka.consumer


import kafka.utils.Logging
import java.util.regex.{PatternSyntaxException, Pattern}
import kafka.common.Topic

/**
 * topic过滤器
 */
sealed abstract class TopicFilter(rawRegex: String) extends Logging {

  //正则表达式,replace参数是字符串或者字节,即有这些字符串的才被替换,而replaceAll参数是正则表达式,表示符合这个正则表达式的都会被替换
  //例如String rawRegex = "' ind ex,'indd ,xx\""; 转换成结果就是index|'indd|xx
  val regex = rawRegex
          .trim
          .replace(',', '|')//替换逗号字符
          .replace(" ", "")//替换空格字符
          .replaceAll("""^["']+""","")//替换以'或者"开头的内容,替换成""
          .replaceAll("""["']+$""","") // property files may bring quotes替换以'或者"结尾的内容,替换成""

  try {
    Pattern.compile(regex)
  }
  catch {
    case e: PatternSyntaxException =>
      throw new RuntimeException(regex + " is an invalid regex.")
  }

  override def toString = regex//返回正则表达式

  /**
   * @topic 表示待校验的topic字符串
   * @excludeInternalTopics true表示要确保topic不能是kafka内部topic名称,例如__consumer_offsets
   * 返回值 true表示topic是允许的
   */
  def isTopicAllowed(topic: String, excludeInternalTopics: Boolean): Boolean
}

//参数是白名单的正则表达式
case class Whitelist(rawRegex: String) extends TopicFilter(rawRegex) {
  
  override def isTopicAllowed(topic: String, excludeInternalTopics: Boolean) = {
    
    /**
     * 1.topic必须匹配正则表达式
     * 2.如果excludeInternalTopics=true,表示要过滤kafka内部的topic
     *   topic不能是kafka内部topic名称,例如__consumer_offsets
     */
    val allowed = topic.matches(regex) && !(Topic.InternalTopics.contains(topic) && excludeInternalTopics)

    //打印该topic是否允许
    debug("%s %s".format(
      topic, if (allowed) "allowed" else "filtered"))

    allowed
  }
}

//参数是黑名单的正则表达式
case class Blacklist(rawRegex: String) extends TopicFilter(rawRegex) {
  override def isTopicAllowed(topic: String, excludeInternalTopics: Boolean) = {
    
   /**
   * @topic 表示待校验的topic字符串
   * @excludeInternalTopics true表示要确保topic不能是kafka内部topic名称,例如__consumer_offsets
   * true表示topic是允许的
   * 
   * 即不符合正则表达式的都是允许的
   */
    val allowed = (!topic.matches(regex)) && !(Topic.InternalTopics.contains(topic) && excludeInternalTopics)

    //打印该topic是否允许
    debug("%s %s".format(
      topic, if (allowed) "allowed" else "filtered"))

    allowed
  }
}

