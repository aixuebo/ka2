/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.common

import java.net.URL
import java.util.jar.{Attributes, Manifest}

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup

//获取指定AppInfo.class所在的jar包的/META-INF/MANIFEST.MF文件中version对应的值,并且将其isRegistered属性设置为true
object AppInfo extends KafkaMetricsGroup {
  private var isRegistered = false
  private val lock = new Object()

  def registerInfo(): Unit = {
    lock.synchronized {
      if (isRegistered) {
        return
      }
    }

    try {
      val clazz = AppInfo.getClass
      val className = clazz.getSimpleName + ".class"
      val classPath = clazz.getResource(className).toString
      if (!classPath.startsWith("jar")) {//确保class是在jar包内
        // Class not from JAR 进入该if,说明class不是在jar包内
        return
      }
      
      //如果classPath是在jar内,则格式demo为:jar:file:/D:/workspaceHive/hiveJdbc/lib/commons-cli-1.2.jar!/org/apache/commons/cli/BasicParser.class
      val manifestPath = classPath.substring(0, classPath.lastIndexOf("!") + 1) + "/META-INF/MANIFEST.MF"//读取该jar包下的/META-INF/MANIFEST.MF文件

      //获取Version的值
      val mf = new Manifest
      mf.read(new URL(manifestPath).openStream())
      val version = mf.getMainAttributes.get(new Attributes.Name("Version")).toString

      newGauge("Version",
        new Gauge[String] {
          def value = {
            version
          }
        })

      lock.synchronized {
        isRegistered = true
      }
    } catch {
      case e: Exception =>
        warn("Can't read Kafka version from MANIFEST.MF. Possible cause: %s".format(e))
    }
  }
}
