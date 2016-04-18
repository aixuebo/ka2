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

import joptsimple.OptionParser

object ToolsUtils {

  /**
   * 校验参数hostPort是否都是正确合法的host:port形式字符串集合
   */
  def validatePortOrDie(parser: OptionParser, hostPort: String) = {
    //返回host and port数组,端口参数使用逗号拆分
    val hostPorts: Array[String] = if(hostPort.contains(','))
      hostPort.split(",")
    else
      Array(hostPort)
      
      //返回参数是true的结果集
    val validHostPort = hostPorts.filter {
      hostPortData =>
        org.apache.kafka.common.utils.Utils.getPort(hostPortData) != null //每一个host:port调用getPort方法,仅要返回true的信息
    }
    
    //校验过滤后,结果是不是还跟参数一样,true表示一样
    val isValid = !(validHostPort.isEmpty) && validHostPort.size == hostPorts.length
    if(!isValid) //false说明校验后发现非法
      CommandLineUtils.printUsageAndDie(parser, "Please provide valid host:port like host1:9091,host2:9092\n ")//打印信息,退出程序
  }
}
