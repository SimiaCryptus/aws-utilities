/*
 * Copyright (c) 2018 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.simiacryptus.aws.exe;

import com.simiacryptus.util.Util;
import com.simiacryptus.util.io.MarkdownNotebookOutput;
import com.simiacryptus.util.io.NotebookOutput;
import com.simiacryptus.util.lang.SerializableConsumer;
import com.simiacryptus.util.test.SysOutInterceptor;

import java.io.File;
import java.util.function.Consumer;

/**
 * The type Local runner.
 */
public class LocalNotebookRunner {
  
  static {
    SysOutInterceptor.INSTANCE.init();
  }
  
  /**
   * Gets task.
   *
   * @param <T>          the type parameter
   * @param defaultClass the default class
   * @param args         the args
   * @return the task
   * @throws InstantiationException the instantiation exception
   * @throws IllegalAccessException the illegal access exception
   * @throws ClassNotFoundException the class not found exception
   */
  public static <T extends SerializableConsumer> SerializableConsumer<NotebookOutput> getTask(
    final Class<T> defaultClass,
    final String... args
  ) throws InstantiationException, IllegalAccessException, ClassNotFoundException
  {
    return (SerializableConsumer<NotebookOutput>) (args.length == 0 ? defaultClass : Class.forName(args[0])).newInstance();
  }
  
  /**
   * Run.
   *
   * @param fns the fns
   * @throws Exception the exception
   */
  public static void run(Consumer<NotebookOutput>... fns) throws Exception {
    for (final Consumer<NotebookOutput> fn : fns) {
      try (NotebookOutput log = new MarkdownNotebookOutput(
        new File("report/" + Util.dateStr("yyyyMMddHHmmss") + "/index"),
          Util.AUTO_BROWSE
      ))
      {
        fn.accept(log);
        log.setFrontMatterProperty("status", "OK");
      } finally {
        System.exit(0);
      }
    }
  }
  
}
