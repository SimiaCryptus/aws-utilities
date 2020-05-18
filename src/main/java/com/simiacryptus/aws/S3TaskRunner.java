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

package com.simiacryptus.aws;

import com.esotericsoftware.minlog.Log;
import com.simiacryptus.lang.UncheckedSupplier;
import com.simiacryptus.ref.lang.RefUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class S3TaskRunner {

  private static final Logger logger = LoggerFactory.getLogger(S3TaskRunner.class);
  private static final int BUFFER_SIZE = 8 * 1024 * 1024;

  static {
    Log.WARN();
  }

  public static void main(String... args) {
    try {
      if (Boolean.parseBoolean(System.getProperty("SHUTDOWN_ON_EXIT", "true")))
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          String command = "sudo shutdown -h 0";
          System.err.println("Terminating system via command: " + command);
          try {
            int i = Runtime.getRuntime().exec(command).waitFor();
            System.err.printf("Result %s for %s%n", i, command);
          } catch (IOException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }));
      long max_age_ms = Long.parseLong(System.getProperty("MAX_AGE_MS", "0"));
      if (max_age_ms > 0) {
        Thread thread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              Thread.sleep(max_age_ms);
              System.out.println("Max Age Reached! Shutting down...");
              Thread.getAllStackTraces().forEach(((thread, stackTraceElements) -> {
                if (thread != Thread.currentThread()) {
                  System.out.println(String.format("Thread %s (daemon=%s): \n\t%s", thread.getName(), thread.isDaemon(),
                      Arrays.stream(stackTraceElements).map(StackTraceElement::toString).reduce((a, b) -> a + "\n\t" + b)));
                }
              }));
              System.exit(1);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        });
        thread.setDaemon(true);
        thread.setName("MAX_AGE=" + max_age_ms);
        thread.start();
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }
    UncheckedSupplier<?> appTask = TendrilSettings.INSTANCE.getAppTask();
    if (null != appTask) {
      try {
        new Thread(() -> {
          try {
            RefUtil.freeRef(appTask.get());
          } catch (Exception e) {
            e.printStackTrace();
          }
        }).run();
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      logger.warn("No Task Found");
      System.exit(1);
    }
  }

}
