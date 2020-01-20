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

import com.simiacryptus.ref.lang.RefUtil;
import com.simiacryptus.ref.wrappers.*;
import com.simiacryptus.util.CodeUtil;
import com.simiacryptus.util.JsonUtil;
import com.simiacryptus.util.test.SysOutInterceptor;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ClasspathUtil {

  private static final Logger logger = LoggerFactory.getLogger(Tendril.class);
  private static final AtomicReference<File> localClasspath = new AtomicReference<>();

  @Nonnull
  public static File summarizeLocalClasspath() {
    synchronized (localClasspath) {
      return localClasspath.updateAndGet(f -> {
        if (f != null)
          return f;
        String localClasspath = RefSystem.getProperty("java.class.path");
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

  @Nonnull
  public static String stageLocalClasspath(@Nonnull final String localClasspath, @Nonnull final Predicate<String> classpathFilter,
                                           @Nonnull final String libPrefix, final boolean parallel) {
    new File(libPrefix).mkdirs();
    RefStream<String> stream = RefArrays.stream(localClasspath.split(File.pathSeparator)).filter(classpathFilter);
    PrintStream out = SysOutInterceptor.INSTANCE.currentHandler();
    if (parallel)
      stream = stream.parallel();
    return RefUtil.get(stream.flatMap(entryPath -> {
      PrintStream prev = SysOutInterceptor.INSTANCE.setCurrentHandler(out);
      RefList<String> temp_04_0003 = RefArrays.asList(entryPath);
      RefList<String> classpathEntry = (new File(entryPath).isDirectory()) ? stageClasspathEntry(libPrefix, entryPath)
          : temp_04_0003.addRef();
      temp_04_0003.freeRef();
      SysOutInterceptor.INSTANCE.setCurrentHandler(prev);
      RefStream<String> temp_04_0001 = classpathEntry.stream();
      classpathEntry.freeRef();
      return temp_04_0001;
    }).reduce((a, b) -> a + ":" + b));
  }

  @Nonnull
  public static RefList<String> stageClasspathEntry(final String libPrefix, @Nonnull final String entryPath) {
    final File entryFile = new File(entryPath);
    try {
      if (entryFile.isFile()) {
        String remote = libPrefix + hash(entryFile) + ".jar";
        logger.info(RefString.format("Staging %s via %s", entryPath, remote));
        try {
          stage(entryFile, remote);
        } catch (Throwable e) {
          logger.warn(RefString.format("Error staging %s to %s", entryFile, remote), e);
        }
        return RefArrays.asList(remote);
      } else {
        logger.info(RefString.format("Processing %s", entryPath));
        RefArrayList<String> list = new RefArrayList<>();
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
  public static String addDir(final String libPrefix, @Nonnull final File entryFile)
      throws IOException, NoSuchAlgorithmException {
    File tempJar = toJar(entryFile);
    try {
      String remote = libPrefix + hash(tempJar) + ".jar";
      logger.info(RefString.format("Uploading %s to %s", tempJar, remote));
      try {
        stage(tempJar, remote);
      } catch (Throwable e) {
        throw new RuntimeException(RefString.format("Error staging %s to %s", entryFile, remote), e);
      }
      return remote;
    } finally {
      tempJar.delete();
    }
  }

  public static void stage(@Nonnull final File entryFile, @Nonnull final String remote) {
    Tendril.stage(entryFile, remote, 10);
  }

  @Nonnull
  public static File toJar(@Nonnull final File entry) throws IOException {
    File tempJar = File.createTempFile(UUID.randomUUID().toString(), ".jar").getAbsoluteFile();
    logger.info(RefString.format("Archiving %s to %s", entry, tempJar));
    try (ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(tempJar))) {
      RefList<File> files = RefArrays.stream(entry.listFiles()).sorted().collect(RefCollectors.toList());
      files.forEach(file -> {
        try {
          write(zip, "", file);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      files.freeRef();
      zip.putNextEntry(new ZipEntry("META-INF/CodeUtil/classSourceInfo.json"));
      try (InputStream input = new ByteArrayInputStream(
          JsonUtil.toJson(RefUtil.addRef(CodeUtil.classSourceInfo)).toString().getBytes("UTF-8"))) {
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

  @Nonnull
  public static String hash(@Nonnull final File classpath) throws NoSuchAlgorithmException, IOException {
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

  private static void summarize(@Nonnull String[] jarFiles, @Nonnull File summary) {
    try {
      JarOutputStream jarOutputStream = new JarOutputStream(new FileOutputStream(summary));
      RefList<JarFile> files = RefArrays.stream(jarFiles).map(x -> {
        try {
          return new JarFile(new File(x));
        } catch (IOException e) {
          logger.warn("Error processing " + x, e);
          return null;
        }
      }).filter(x -> x != null).collect(RefCollectors.toList());
      RefArrayList<String[]> conflicts = new RefArrayList<>();
      RefMap<String, RefList<ClasspathUtil.ClasspathEntry>> temp_04_0004 = files.stream().flatMap(file -> {
        return file.stream()
            //.filter(x -> !x.isDirectory())
            .map(jarEntry -> new ClasspathEntry(file, jarEntry));
      }).sorted(RefComparator.comparing(x -> x.jarEntry.getName() + ":" + x.file.getName()))
          .collect(RefCollectors.groupingBy(x -> x.jarEntry.getName()));
      RefCollection<RefList<ClasspathUtil.ClasspathEntry>> temp_04_0005 = temp_04_0004.values();
      temp_04_0005.stream().map(
          RefUtil.wrapInterface((Function<RefList<ClasspathUtil.ClasspathEntry>, ClasspathUtil.ClasspathEntry>) x -> {
            if (x.size() > 1 && !x.get(0).jarEntry.isDirectory()) {
              conflicts.add(new String[]{RefUtil.get(
                  x.stream().map(y -> new File(y.file.getName()).getName()).sorted()
                      .reduce((a, b) -> a + ", " + b)
              ), x.get(0).jarEntry.getName()});
            }
            ClasspathUtil.ClasspathEntry temp_04_0002 = x.get(0);
            x.freeRef();
            return temp_04_0002;
          }, conflicts.addRef())).filter(x -> {
        String name = x.jarEntry.getName();
        return !name.startsWith("java/") && !name.startsWith("sun/") && !name.toUpperCase().endsWith(".DSA")
            && !name.toUpperCase().endsWith(".RSA");
      }).forEach(entry -> {
        JarEntry jarEntry = entry.getJarEntry();
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
              logger.warn(RefString.format("Size wrong for %s: %s != %s", new File(jarEntryName).getName(),
                  jarEntry.getSize(), bytes.length));
            jarOutputStream.putNextEntry(jarEntry);
            //logger.info(String.format("Wrote file %s from %s", jarEntryName, entry.getFile().getName()));
            IOUtils.write(bytes, jarOutputStream);
          }
        } catch (Throwable e) {
          logger.info(RefString.format("Error putting class %s with length %s", jarEntryName, jarEntry.getSize()),
              e);
        }
      });
      temp_04_0005.freeRef();
      temp_04_0004.freeRef();
      RefMap<String, RefList<String[]>> temp_04_0006 = conflicts.stream().collect(RefCollectors.groupingBy(y -> y[0]));
      RefSet<Map.Entry<String, RefList<String[]>>> temp_04_0007 = temp_04_0006.entrySet();
      temp_04_0007.stream().forEach(e -> {
        RefList<String[]> temp_04_0008 = e.getValue();
        logger.info("Conflict between " + e.getKey() + " for "
            + RefUtil.get(temp_04_0008.stream().map(y -> y[1]).sorted().reduce((a, b) -> a + ", " + b)));
        temp_04_0008.freeRef();
        RefUtil.freeRef(e);
      });
      temp_04_0007.freeRef();
      temp_04_0006.freeRef();
      conflicts.freeRef();
      jarOutputStream.close();
      files.forEach(x -> {
        try {
          x.close();
        } catch (IOException e) {
          logger.info("Error closing " + x.getName(), e);
        }
      });
      files.freeRef();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void write(@Nonnull final ZipOutputStream zip, final String base, @Nonnull final File entry) throws IOException {
    if (entry.isFile()) {
      zip.putNextEntry(new ZipEntry(base + entry.getName()));
      try (FileInputStream input = new FileInputStream(entry)) {
        IOUtils.copy(input, zip);
      }
      zip.closeEntry();
    } else {
      RefList<File> files = RefArrays.stream(entry.listFiles()).sorted().collect(RefCollectors.toList());
      files.forEach(file -> {
        try {
          write(zip, base + entry.getName() + "/", file);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      files.freeRef();
    }
  }

  private static class ClasspathEntry {
    private final JarFile file;
    private final JarEntry jarEntry;

    private ClasspathEntry(JarFile file, JarEntry jarEntry) {
      this.file = file;
      this.jarEntry = jarEntry;
    }

    public JarFile getFile() {
      return file;
    }

    public JarEntry getJarEntry() {
      return jarEntry;
    }
  }
}
