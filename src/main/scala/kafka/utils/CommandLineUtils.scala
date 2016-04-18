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

import joptsimple.{OptionSpec, OptionSet, OptionParser}
import scala.collection.Set
 import java.util.Properties

 /**
 * Helper functions for dealing with command line utilities
 * cli命令行工具
 */
object CommandLineUtils extends Logging {

  /**
   * Check that all the listed options are present
   * @required 表示必须要有的命令
   * @options 表示总共命令信息
   * 该函数校验@required中所有命令必须存在,如果不存在,则打印日志,并且停止程序运行
   */
  def checkRequiredArgs(parser: OptionParser, options: OptionSet, required: OptionSpec[_]*) {
    for(arg <- required) {
      if(!options.has(arg))
        printUsageAndDie(parser, "Missing required argument \"" + arg + "\"")
    }
  }
  
  /**
   * Check that none of the listed options are present
   * @usedOption 
   * @options 表示总共命令信息
   * @invalidOptions 表示无效的命令集合
   * 循环所有的usedOption命令,如果该命令存在,则不允许有invalidOptions命令存在
   * 比如 当-r存在的时候,不允许有-p、-s命令
   * 则usedOption为-r
   * invalidOptions为-p和-s
   */
  def checkInvalidArgs(parser: OptionParser, options: OptionSet, usedOption: OptionSpec[_], invalidOptions: Set[OptionSpec[_]]) {
    if(options.has(usedOption)) {
      for(arg <- invalidOptions) {
        if(options.has(arg))
          printUsageAndDie(parser, "Option \"" + usedOption + "\" can't be used with option\"" + arg + "\"")
      }
    }
  }
  
  /**
   * Print usage and exit
   * 打印完要程序退出
   */
  def printUsageAndDie(parser: OptionParser, message: String) {
    System.err.println(message) //打印message
    parser.printHelpOn(System.err)
    System.exit(1)
  }

  /**
   * Parse key-value pairs in the form key=value
   * 循环解析key=value,设置到Properties中,如果key没有value,则Properties中设置key=""
   */
  def parseKeyValueArgs(args: Iterable[String]): Properties = {
    val splits = args.map(_ split "=").filterNot(_.length == 0) //转换成Iterable[Array[String]]形式,即将字符串参数key=value,转换成数组,arr[key,value]

    val props = new Properties
    for(a <- splits) {
      if (a.length == 1) props.put(a(0), "")
      else if (a.length == 2) props.put(a(0), a(1))
      else {
        System.err.println("Invalid command line properties: " + args.mkString(" "))
        System.exit(1)
      }
    }
    props
  }
}