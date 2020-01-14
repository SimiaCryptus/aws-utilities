/*
 * Copyright (c) 2019 by Andrew Charneski.
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

import com.simiacryptus.lang.SerializableConsumer;
import com.simiacryptus.notebook.MarkdownNotebookOutput;
import com.simiacryptus.notebook.NotebookOutput;
import com.simiacryptus.ref.wrappers.RefConsumer;
import com.simiacryptus.ref.wrappers.RefSystem;
import com.simiacryptus.util.Util;
import com.simiacryptus.util.test.SysOutInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;

public class LocalNotebookRunner {
  private static final Logger logger = LoggerFactory.getLogger(LocalNotebookRunner.class);

  static {
    SysOutInterceptor.INSTANCE.init();
  }

  @Nonnull
  public static <T extends SerializableConsumer> SerializableConsumer<NotebookOutput> getTask(
      final Class<T> defaultClass, @Nonnull final String... args)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    return (SerializableConsumer<NotebookOutput>) (args.length == 0 ? defaultClass : Class.forName(args[0]))
        .newInstance();
  }

  public static void run(@Nonnull RefConsumer<NotebookOutput>... fns) throws Exception {
    for (final RefConsumer<NotebookOutput> fn : fns) {
      try (NotebookOutput log = new MarkdownNotebookOutput(
          new File("report/" + Util.dateStr("yyyyMMddHHmmss") + "/index"), true)) {
        fn.accept(log);
        log.setFrontMatterProperty("status", "OK");
      } finally {
        logger.warn("Exiting notebook", new RuntimeException("Stack Trace"));
        RefSystem.exit(0);
      }
    }
  }

}
