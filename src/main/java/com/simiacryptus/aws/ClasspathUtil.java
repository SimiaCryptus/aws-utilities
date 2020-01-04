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

import com.simiacryptus.util.CodeUtil;
import com.simiacryptus.util.JsonUtil;
import com.simiacryptus.util.test.SysOutInterceptor;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public @com.simiacryptus.ref.lang.RefAware
class ClasspathUtil {

  private static final Logger logger = LoggerFactory.getLogger(Tendril.class);
  private static final AtomicReference<File> localClasspath = new AtomicReference<>();

  @NotNull
  public static File summarizeLocalClasspath() {
    synchronized (localClasspath) {
      return localClasspath.updateAndGet(f -> {
        if (f != null)
          return f;
        String localClasspath = System.getProperty("java.class.path");
        logger.info("Java Local Classpath: " + localClasspath);
        File lib = new File("lib");
        lib.mkdirs();
        String remoteClasspath = stageLocalClasspath(localClasspath, entry -> true,
            lib.getAbsolutePath() + File.separator, true);
        logger.info("Java Remote Classpath: " + remoteClasspath);
        File file = new File(lib, new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) + "_"
            + Integer.toHexString(new Random().nextInt(0xFFFF)) + ".jar");
        summarize(remoteClasspath.split(":(?!\\\\)"), file);
        return file;
      });
    }
  }

  public static String stageLocalClasspath(final String localClasspath, final Predicate<String> classpathFilter,
                                           final String libPrefix, final boolean parallel) {
    new File(libPrefix).mkdirs();
    com.simiacryptus.ref.wrappers.RefStream<String> stream = com.simiacryptus.ref.wrappers.RefArrays
        .stream(localClasspath.split(File.pathSeparator)).filter(classpathFilter);
    PrintStream out = SysOutInterceptor.INSTANCE.currentHandler();
    if (parallel)
      stream = stream.parallel();
    return stream.flatMap(entryPath -> {
      PrintStream prev = SysOutInterceptor.INSTANCE.setCurrentHandler(out);
      com.simiacryptus.ref.wrappers.RefList<String> classpathEntry = (new File(entryPath).isDirectory())
          ? stageClasspathEntry(libPrefix, entryPath)
          : com.simiacryptus.ref.wrappers.RefArrays.asList(entryPath);
      SysOutInterceptor.INSTANCE.setCurrentHandler(prev);
      return classpathEntry.stream();
    }).reduce((a, b) -> a + ":" + b).get();
  }

  @Nonnull
  public static com.simiacryptus.ref.wrappers.RefList<String> stageClasspathEntry(final String libPrefix,
                                                                                  final String entryPath) {
    final File entryFile = new File(entryPath);
    try {
      if (entryFile.isFile()) {
        String remote = libPrefix + hash(entryFile) + ".jar";
        logger.info(String.format("Staging %s via %s", entryPath, remote));
        try {
          stage(entryFile, remote);
        } catch (Throwable e) {
          logger.warn(String.format("Error staging %s to %s", entryFile, remote), e);
        }
        return com.simiacryptus.ref.wrappers.RefArrays.asList(remote);
      } else {
        logger.info(String.format("Processing %s", entryPath));
        com.simiacryptus.ref.wrappers.RefArrayList<String> list = new com.simiacryptus.ref.wrappers.RefArrayList<>();
        File parentFile = entryFile.getParentFile().getParentFile();
        if (entryFile.getName().equals("classes") && entryFile.getParentFile().getName().equals("target")) {
          File javaSrc = new File(new File(new File(parentFile, "src"), "main"), "java");
          if (javaSrc.exists())
            list.add(addDir(libPrefix, javaSrc));
          File scalaSrc = new File(new File(new File(parentFile, "src"), "main"), "scala");
          if (scalaSrc.exists())
            list.add(addDir(libPrefix, scalaSrc));
        }
        if (entryFile.getName().equals("test-classes") && entryFile.getParentFile().getName().equals("target")) {
          File javaSrc = new File(new File(new File(parentFile, "src"), "test"), "java");
          if (javaSrc.exists())
            list.add(addDir(libPrefix, javaSrc));
          File scalaSrc = new File(new File(new File(parentFile, "src"), "test"), "scala");
          if (scalaSrc.exists())
            list.add(addDir(libPrefix, scalaSrc));
        }
        list.add(addDir(libPrefix, entryFile));
        return list;
      }
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static String addDir(final String libPrefix, final File entryFile)
      throws IOException, NoSuchAlgorithmException {
    File tempJar = toJar(entryFile);
    try {
      String remote = libPrefix + hash(tempJar) + ".jar";
      logger.info(String.format("Uploading %s to %s", tempJar, remote));
      try {
        stage(tempJar, remote);
      } catch (Throwable e) {
        throw new RuntimeException(String.format("Error staging %s to %s", entryFile, remote), e);
      }
      return remote;
    } finally {
      tempJar.delete();
    }
  }

  public static void stage(final File entryFile, final String remote) {
    Tendril.stage(entryFile, remote, 10);
  }

  @Nonnull
  public static File toJar(@Nonnull final File entry) throws IOException {
    File tempJar = File.createTempFile(UUID.randomUUID().toString(), ".jar").getAbsoluteFile();
    logger.info(String.format("Archiving %s to %s", entry, tempJar));
    try (ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(tempJar))) {
      for (final File file : com.simiacryptus.ref.wrappers.RefArrays.stream(entry.listFiles()).sorted()
          .collect(com.simiacryptus.ref.wrappers.RefCollectors.toList())) {
        write(zip, "", file);
      }
      zip.putNextEntry(new ZipEntry("META-INF/CodeUtil/classSourceInfo.json"));
      try (InputStream input = new ByteArrayInputStream(
          JsonUtil.toJson(CodeUtil.classSourceInfo).toString().getBytes("UTF-8"))) {
        IOUtils.copy(input, zip);
      }
      zip.closeEntry();
    } catch (Throwable e) {
      if (tempJar.exists())
        tempJar.delete();
      throw new RuntimeException(e);
    }
    return tempJar;
  }

  public static String hash(final File classpath) throws NoSuchAlgorithmException, IOException {
    MessageDigest digest = MessageDigest.getInstance("MD5");
    InputStream fis = new FileInputStream(classpath);
    int n = 0;
    byte[] buffer = new byte[8192];
    while (n != -1) {
      n = fis.read(buffer);
      if (n > 0) {
        digest.update(buffer, 0, n);
      }
    }
    return new String(Hex.encodeHex(digest.digest()));
  }

  private static void summarize(String[] jarFiles, File summary) {
    try {
      JarOutputStream jarOutputStream = new JarOutputStream(new FileOutputStream(summary));
      com.simiacryptus.ref.wrappers.RefList<JarFile> files = com.simiacryptus.ref.wrappers.RefArrays.stream(jarFiles)
          .map(x -> {
            try {
              return new JarFile(new File(x));
            } catch (IOException e) {
              logger.warn("Error processing " + x, e);
              return null;
            }
          }).filter(x -> x != null).collect(com.simiacryptus.ref.wrappers.RefCollectors.toList());
      com.simiacryptus.ref.wrappers.RefArrayList<String[]> conflicts = new com.simiacryptus.ref.wrappers.RefArrayList<>();
      files.stream().flatMap(file -> {
        return file.stream()
            //.filter(x -> !x.isDirectory())
            .map(jarEntry -> new ClasspathEntry(file, jarEntry));
      }).sorted(
          com.simiacryptus.ref.wrappers.RefComparator.comparing(x -> x.jarEntry.getName() + ":" + x.file.getName()))
          .collect(com.simiacryptus.ref.wrappers.RefCollectors.groupingBy(x -> x.jarEntry.getName())).values().stream()
          .map(x -> {
            if (x.size() > 1 && !x.get(0).jarEntry.isDirectory()) {
              conflicts.add(new String[]{x.stream().map(y -> new File(y.file.getName()).getName()).sorted()
                  .reduce((a, b) -> a + ", " + b).get(), x.get(0).jarEntry.getName()});
            }
            return x.get(0);
          }).filter(x -> {
        String name = x.jarEntry.getName();
        return !name.startsWith("java/") && !name.startsWith("sun/") && !name.toUpperCase().endsWith(".DSA")
            && !name.toUpperCase().endsWith(".RSA");
      }).forEach(entry -> {
        java.util.jar.JarEntry jarEntry = entry.getJarEntry();
        String jarEntryName = jarEntry.getName();
        try {
          if (jarEntry.isDirectory()) {
            jarOutputStream.putNextEntry(jarEntry);
            //logger.info(String.format("Wrote directory %s from %s", jarEntryName, entry.getFile().getName()));
          } else {
            InputStream inputStream = entry.getFile().getInputStream(jarEntry);
            byte[] bytes = IOUtils.toByteArray(inputStream);
            inputStream.close();
            if (jarEntry.getSize() != (long) bytes.length)
              logger.warn(String.format("Size wrong for %s: %s != %s", new File(jarEntryName).getName(),
                  jarEntry.getSize(), bytes.length));
            jarOutputStream.putNextEntry(jarEntry);
            //logger.info(String.format("Wrote file %s from %s", jarEntryName, entry.getFile().getName()));
            IOUtils.write(bytes, jarOutputStream);
          }
        } catch (Throwable e) {
          logger.info(String.format("Error putting class %s with length %s", jarEntryName, jarEntry.getSize()), e);
        }
      });
      conflicts.stream().collect(com.simiacryptus.ref.wrappers.RefCollectors.groupingBy(y -> y[0])).entrySet().stream()
          .forEach(e -> {
            logger.info("Conflict between " + e.getKey() + " for "
                + e.getValue().stream().map(y -> y[1]).sorted().reduce((a, b) -> a + ", " + b).get());
          });
      jarOutputStream.close();
      files.forEach(x -> {
        try {
          x.close();
        } catch (IOException e) {
          logger.info("Error closing " + x.getName(), e);
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void write(final ZipOutputStream zip, final String base, final File entry) throws IOException {
    if (entry.isFile()) {
      zip.putNextEntry(new ZipEntry(base + entry.getName()));
      try (FileInputStream input = new FileInputStream(entry)) {
        IOUtils.copy(input, zip);
      }
      zip.closeEntry();
    } else {
      for (final File file : com.simiacryptus.ref.wrappers.RefArrays.stream(entry.listFiles()).sorted()
          .collect(com.simiacryptus.ref.wrappers.RefCollectors.toList())) {
        write(zip, base + entry.getName() + "/", file);
      }
    }

  }

  private static @com.simiacryptus.ref.lang.RefAware
  class ClasspathEntry {
    private final JarFile file;
    private final java.util.jar.JarEntry jarEntry;

    private ClasspathEntry(JarFile file, java.util.jar.JarEntry jarEntry) {
      this.file = file;
      this.jarEntry = jarEntry;
    }

    public JarFile getFile() {
      return file;
    }

    public java.util.jar.JarEntry getJarEntry() {
      return jarEntry;
    }
  }
}
