/*
 * Copyright (c) 2020 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.simiacryptus.util.test;

import com.google.common.reflect.ClassPath;
import com.simiacryptus.aws.Tendril;
import com.simiacryptus.aws.TendrilControl;
import com.simiacryptus.notebook.NotebookOutput;
import com.simiacryptus.util.ReportingUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;

import javax.annotation.Nonnull;
import java.io.*;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MacroTestRunner {

  private static int MAX_TEST_CLASSES = Integer.MAX_VALUE;
  private static int MAX_TEST_METHODS = Integer.MAX_VALUE;
  private final Random RANDOM = new Random();
  private long maxChildAge = TimeUnit.MINUTES.toMillis(15);
  private String childJvmOptions = "-Xmx16g -ea";
  private Isolation isolation = Isolation.Class;

  public Map<String, String> getChildEnvironment() {
    return System.getenv();
  }

  public String getChildJvmOptions() {
    return childJvmOptions;
  }

  public MacroTestRunner setChildJvmOptions(String childJvmOptions) {
    this.childJvmOptions = childJvmOptions.trim();
    return this;
  }

  public Isolation getIsolation() {
    return isolation;
  }

  public MacroTestRunner setIsolation(Isolation isolation) {
    this.isolation = isolation;
    return this;
  }

  public long getMaxChildAge() {
    return maxChildAge;
  }

  public static Map<File, String> getReports() {
    Map<File, String> reports = new HashMap<>(NotebookTestBase.reports);
    NotebookTestBase.reports.clear();
    return reports;
  }

  public static Map<File, String> runTest(PrintStream out, String testName) throws ClassNotFoundException {
    StringWriter stringWriter = new StringWriter();
    try (PrintWriter printWriter = new PrintWriter(stringWriter)) {
      RunNotifier notifier = new RunNotifier();
      notifier.addListener(new LoggingRunListener(printWriter, out));
      new JUnitPlatform(Class.forName(testName)).run(notifier);
    }
    return getReports();
  }

  public static Class<?> getClass(String testName) {
    try {
      return Class.forName(testName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static Map<File, String> runTest(PrintStream out, String testName, String methodName) throws NoTestsRemainException, ClassNotFoundException {
    Class<?> testClass = Class.forName(testName);
    StringWriter stringWriter = new StringWriter();
    try (PrintWriter printWriter = new PrintWriter(stringWriter)) {
      RunNotifier notifier = new RunNotifier();
      notifier.addListener(new LoggingRunListener(printWriter, out));
      JUnitPlatform platform = new JUnitPlatform(testClass);
      platform.filter(new MethodFilter(methodName));
      platform.run(notifier);
    }
    return getReports();
  }

  public static String toString(TestExecutionSummary summary) {
    StringWriter stringWriter = new StringWriter();
    summary.printTo(new PrintWriter(stringWriter));
    try {
      stringWriter.close();
    } catch (IOException e) {
      return e.getMessage();
    }
    return stringWriter.toString();
  }

  public MacroTestRunner setMaxChildAge(long value, TimeUnit unit) {
    this.maxChildAge = unit.toMillis(value);
    return this;
  }

  public void runAll(NotebookOutput log, String packageName) {
    URI testArchive = TestSettings.INSTANCE.testArchive;
    String startDateId = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    if (null != testArchive) {
      log.setArchiveHome(testArchive.resolve("runner/" + startDateId + "/"));
    }
    String logRoot = log.getRoot().getAbsolutePath()
        .replaceAll(" ", "\\ ");
    String extraJvmOpts = String.format("-DSHUTDOWN_ON_EXIT=false -DTEST_REPO=%s -DMAX_AGE_MS=%s",
        logRoot, getMaxChildAge());
    getTestClasses(log, packageName)
        .forEach((declaringClass, testClasses) -> {
          log.h1(declaringClass.getSimpleName());
          testClasses.forEach(testClass -> {
            if (testClass != declaringClass) log.h2(testClass.getSimpleName());
            String testClassName = testClass.getName();
            switch (getIsolation()) {
              case None:
                getTestMethods(getClass(testClassName)).forEach(method -> {
                  String methodName = method.getName();
                  log.h3("Test Method: " + methodName);
                  try {
                    Map<File, String> files = log.eval(() -> {
                      return runTest(System.out, testClassName, methodName);
                    });
                    files.forEach((file, name) -> {
                      log.p(log.link(file, "Report " + name));
                    });
                  } catch (Throwable e) {
                    // Ignore. It was logged by the eval
                  }
                });
                break;
              case Class:
                try {
                  Map<File, String> files = log.eval(() -> {
                    System.out.println("Test: " + testClassName);
                    System.out.println("Working Directory: " + logRoot);
                    try (TendrilControl localJvm = startLocalJvm(extraJvmOpts)) {
                      return localJvm.start(() -> {
                        ReportingUtil.AUTO_BROWSE = false;
                        return MacroTestRunner.runTest(System.out, testClassName);
                      }).get(30, TimeUnit.MINUTES);
                    }
                  });
                  files.forEach((file, name) -> {
                    log.p(log.link(file, "Report " + name));
                  });
                } catch (Throwable e) {
                  // Ignore. It was logged by the eval
                }
                break;
              case Method:
                getTestMethods(getClass(testClassName)).forEach(method -> {
                  String methodName = method.getName();
                  log.h3("Test Method: " + methodName);
                  try {
                    Map<File, String> files = log.eval(() -> {
                      System.out.println("Test: " + testClassName);
                      System.out.println("Working Directory: " + logRoot);
                      try (TendrilControl localJvm = startLocalJvm(extraJvmOpts)) {
                        return localJvm.start(() -> {
                          ReportingUtil.AUTO_BROWSE = false;
                          return runTest(System.out, testClassName, methodName);
                        }).get(30, TimeUnit.MINUTES);
                      }
                    });
                    files.forEach((file, name) -> {
                      log.p(log.link(file, "Report " + name));
                    });
                  } catch (Throwable e) {
                    // Ignore. It was logged by the eval
                  }
                });
                break;
            }
          });
        });
  }

  public @Nonnull TendrilControl startLocalJvm(String extraJvmOpts) {
    return Tendril.startLocalJvm(
        16000 + RANDOM.nextInt(4 * 1024),
        getChildJvmOptions() + " " + extraJvmOpts,
        getChildEnvironment(),
        workingDirectory()
    );
  }

  @NotNull
  public File workingDirectory() {
    return new File("").getAbsoluteFile();
  }

  @NotNull
  public List<Method> getTestMethods(Class<?> testClass) {
    return Arrays.stream(testClass.getMethods())
        .filter(method -> method.getAnnotation(org.junit.jupiter.api.Test.class) != null)
        .sorted(Comparator.comparing(Method::getName))
        .limit(MAX_TEST_METHODS)
        .collect(Collectors.toList());
  }

  public Map<Class<?>, List<Class<?>>> getTestClasses(NotebookOutput log, String packageName) {
    assert null != log;
    return log.eval(() -> {
      ClassLoader classLoader = ClassLoader.getSystemClassLoader();
      return ClassPath.from(classLoader).getAllClasses().stream()
          .filter(c -> c.getPackageName().startsWith(packageName))
          .map(c -> {
            try {
              return Class.forName(c.getName(), true, classLoader);
            } catch (ClassNotFoundException e) {
              throw new RuntimeException(e);
            }
          })
          .filter(c -> (c.getModifiers() & Modifier.ABSTRACT) == 0)
          .filter(NotebookTestBase.class::isAssignableFrom)
          .sorted(Comparator.comparing(c -> c.getCanonicalName()))
          .limit(MAX_TEST_CLASSES)
          .map(aClass -> {
            System.out.println(aClass.getCanonicalName());
            return aClass;
          })
          .collect(Collectors.groupingBy(x -> {
            Class<?> declaringClass = x.getDeclaringClass();
            return declaringClass == null ? x : declaringClass;
          }));
    });
  }

  protected Map<File, String> runEachTest(PrintStream out, String testName) throws NoTestsRemainException, ClassNotFoundException {
    for (Method method : getTestMethods(getClass(testName))) {
      String methodName = method.getName();
      out.println("Test Method: " + methodName);
      runTest(out, testName, methodName);
    }
    return getReports();
  }

  public enum Isolation {
    None,
    Class,
    Method
  }

  private static class LoggingRunListener extends RunListener {
    private final PrintWriter printWriter;
    private final PrintStream out;

    public LoggingRunListener(PrintWriter printWriter, PrintStream out) {
      this.printWriter = printWriter;
      this.out = out;
    }

    @Override
    public void testRunStarted(Description description) {
      String msg = "Run Start: " + description.toString();
      printWriter.println(msg);
      out.println(msg);
    }

    @Override
    public void testRunFinished(Result result) {
      String msg = "Run Finish: " + result.toString();
      printWriter.println(msg);
      out.println(msg);
    }

    @Override
    public void testStarted(Description description) {
      String msg = "Test Start: " + description.toString();
      printWriter.println(msg);
      out.println(msg);
    }

    @Override
    public void testFinished(Description description) {
      String msg = "Test Finish: " + description.toString();
      printWriter.println(msg);
      out.println(msg);
    }

    @Override
    public void testFailure(Failure failure) {
      String msg = "Failure: " + failure.toString();
      printWriter.println(msg);
      out.println(msg);
    }

    @Override
    public void testIgnored(Description description) {
      String msg = "Ignored: " + description.toString();
      printWriter.println(msg);
      out.println(msg);
    }
  }

  private static class MethodFilter extends Filter {
    private final String methodName;

    public MethodFilter(String methodName) {
      this.methodName = methodName;
    }

    @Override
    public boolean shouldRun(Description description) {
      String name = description.getMethodName();
      if (null == name) {
        return true;
      }
      return name.startsWith(methodName + "(");
    }

    @Override
    public String describe() {
      return "Select " + methodName;
    }
  }
}
