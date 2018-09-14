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

package com.simiacryptus.aws;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.simiacryptus.util.Util;
import com.simiacryptus.util.io.NotebookOutput;
import com.simiacryptus.util.io.TeeInputStream;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * The type S3 util.
 */
public class S3Util {

  private final static Logger logger = LoggerFactory.getLogger(S3Util.class);

  public static Map<File, URL> upload(NotebookOutput log) {
    return upload(log, AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build());
  }

  public static Map<File, URL> upload(NotebookOutput log, AmazonS3 s3) {
    try {
      log.write();
      File root = log.getRoot();
      logFiles(root);
      URI archiveHome = log.getArchiveHome();
      HashMap<File, URL> map = new HashMap<>();
      if (null == archiveHome || (archiveHome.getScheme().startsWith("s3") && (null == archiveHome.getHost() || archiveHome.getHost().isEmpty()))) {
        logger.info("No archive destination to publish to");
        return map;
      }

      logger.info(String.format("Resolved %s / %s = %s", archiveHome, root.getName(), archiveHome));
      for (File file : root.listFiles()) {
        map.putAll(S3Util.upload(s3, archiveHome, file));
      }
      return map;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void logFiles(File f) {
    if (f.isDirectory()) {
      for (File child : f.listFiles()) {
        logFiles(child);
      }
    } else logger.info(String.format("File %s length %s", f.getAbsolutePath(), f.length()));
  }

  public static Map<File, URL> upload(final AmazonS3 s3, final URI path, final File file) {
    try {
      HashMap<File, URL> map = new HashMap<>();
      if (!file.exists()) throw new RuntimeException(file.toString());
      String bucket = path.getHost();
      String scheme = path.getScheme();
      if (file.isFile()) {
        String reportPath = path.resolve(file.getName()).getPath().replaceAll("//", "/").replaceAll("^/", "");
        if (scheme.startsWith("s3")) {
          logger.info(String.format("Uploading file %s to s3 %s/%s", file.getAbsolutePath(), bucket, reportPath));
          s3.putObject(new PutObjectRequest(bucket, reportPath, file).withCannedAcl(CannedAccessControlList.PublicRead));
          map.put(file.getAbsoluteFile(), s3.getUrl(bucket, reportPath));
        } else {
          try {
            logger.info(String.format("Copy file %s to %s", file.getAbsolutePath(), reportPath));
            FileUtils.copyFile(file, new File(reportPath));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      } else {
        URI filePath = path.resolve(file.getName() + "/");
        if (scheme.startsWith("s3")) {
          String reportPath = filePath.getPath().replaceAll("//", "/").replaceAll("^/", "");
          logger.info(String.format("Scanning peer uploads to %s at s3 %s/%s", file.getAbsolutePath(), bucket, reportPath));
          List<S3ObjectSummary> preexistingFiles = s3.listObjects(new ListObjectsRequest().withBucketName(bucket).withPrefix(reportPath))
              .getObjectSummaries().stream().collect(Collectors.toList());
          for (S3ObjectSummary preexistingFile : preexistingFiles) {
            logger.info(String.format("Preexisting File: '%s' + '%s'", reportPath, preexistingFile.getKey()));
            map.put(new File(file, preexistingFile.getKey()).getAbsoluteFile(), s3.getUrl(bucket, reportPath + preexistingFile.getKey()));
          }
        }
        logger.info(String.format("Uploading folder %s to %s", file.getAbsolutePath(), filePath.toString()));
        for (File subfile : file.listFiles()) {
          map.putAll(upload(s3, filePath, subfile));
        }
      }
      return map;
    } catch (Throwable e) {
      throw new RuntimeException("Error uploading " + file + " to " + path, e);
    }
  }

  public static InputStream cache(final AmazonS3 s3, final URI cacheLocation, URI resourceLocation) {
    return cache(s3, cacheLocation, tempFile(cacheLocation), Util.getStreamSupplier(resourceLocation));
  }

  public static InputStream cache(AmazonS3 s3, URI s3File, File localFile, Supplier<InputStream> streamSupplier) {
    final String path = s3File.getPath().replaceAll("^/", "");
    if (s3.doesObjectExist(s3File.getHost(), path)) {
      return s3.getObject(s3File.getHost(), path).getObjectContent();
    } else {
      try {
        localFile.deleteOnExit();
        return new TeeInputStream(streamSupplier.get(), new FileOutputStream(localFile)) {
          @Override
          public void close() throws IOException {
            super.close();
            s3.putObject(s3File.getHost(), path, localFile);
          }
        };
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @NotNull
  public static File tempFile(URI file) {
    File localFile;
    try {
      localFile = File.createTempFile(file.getPath(), "data");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return localFile;
  }

}
