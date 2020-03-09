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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.simiacryptus.notebook.NotebookOutput;
import com.simiacryptus.ref.lang.RefUtil;
import com.simiacryptus.ref.wrappers.RefArrays;
import com.simiacryptus.ref.wrappers.RefString;
import com.simiacryptus.util.Util;
import com.simiacryptus.util.io.TeeInputStream;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

public class S3Util {

  private final static Logger logger = LoggerFactory.getLogger(S3Util.class);

  @Nonnull
  public static Map<File, URL> upload(@Nonnull NotebookOutput log) {
    synchronized (log) {
      return upload(log, AmazonS3ClientBuilder.standard().withRegion(EC2Util.REGION).build());
    }
  }

  @Nonnull
  public static Map<File, URL> upload(@Nonnull NotebookOutput log, @Nonnull AmazonS3 s3) {
    try {
      log.write();
      File root = log.getRoot();
      URI archiveHome = log.getArchiveHome();
      logger.info(RefString.format("Files in %s to be archived in %s", root.getAbsolutePath(), archiveHome));
      HashMap<File, URL> map = new HashMap<>();
      if (null != archiveHome) {
        logFiles(root);
        if (archiveHome.getScheme().startsWith("s3") && (null == archiveHome.getHost() || archiveHome.getHost().isEmpty() || "null".equals(archiveHome.getHost()))) {
          logger.info(RefString.format("No archive destination to publish to: %s", archiveHome));
          return map;
        }
        logger.info(RefString.format("Resolved %s / %s", archiveHome, log.getName()));
        for (File file : root.listFiles()) {
          map.putAll(S3Util.upload(s3, archiveHome, file));
        }
      }
      return map;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void logFiles(@Nonnull File f) {
    if (f.isDirectory()) {
      for (File child : f.listFiles()) {
        logFiles(child);
      }
    } else
      logger.info(RefString.format("File %s length %s", f.getAbsolutePath(), f.length()));
  }

  @Nonnull
  public static Map<File, URL> upload(@Nonnull final AmazonS3 s3, final URI path, @Nonnull final File file) {
    return upload(s3, path, file, 3);
  }

  @Nonnull
  public static Map<File, URL> upload(@Nonnull final AmazonS3 s3, @Nullable final URI path, @Nonnull final File file, int retries) {
    try {
      HashMap<File, URL> map = new HashMap<>();
      if (!file.exists()) {
        throw new RuntimeException(file.toString());
      }
      if (null == path) {
        return map;
      }
      String bucket = path.getHost();
      String scheme = path.getScheme();
      if (file.isFile()) {
        String reportPath = path.resolve(file.getName()).getPath().replaceAll("//", "/").replaceAll("^/", "");
        if (scheme.startsWith("s3")) {
          logger.info(RefString.format("Uploading file %s to s3 %s/%s", file.getAbsolutePath(), bucket, reportPath));
          boolean upload;
          try {
            ObjectMetadata existingMetadata;
            if (s3.doesObjectExist(bucket, reportPath))
              existingMetadata = s3.getObjectMetadata(bucket, reportPath);
            else
              existingMetadata = null;
            if (null != existingMetadata) {
              if (existingMetadata.getContentLength() != file.length()) {
                logger.info(RefString.format("Removing outdated file %s/%s", bucket, reportPath));
                s3.deleteObject(bucket, reportPath);
                upload = true;
              } else {
                logger.info(RefString.format("Existing file %s/%s", bucket, reportPath));
                upload = false;
              }
            } else {
              logger.info(RefString.format("Not found file %s/%s", bucket, reportPath));
              upload = true;
            }
          } catch (AmazonS3Exception e) {
            logger.info(RefString.format("Error listing %s/%s", bucket, reportPath), e);
            upload = true;
          }
          if (upload) {
            s3.putObject(
                new PutObjectRequest(bucket, reportPath, file).withCannedAcl(CannedAccessControlList.PublicRead));
          }
          RefUtil.freeRef(map.put(file.getAbsoluteFile(), s3.getUrl(bucket, reportPath)));
        } else {
          try {
            logger.info(RefString.format("Copy file %s to %s", file.getAbsolutePath(), reportPath));
            FileUtils.copyFile(file, new File(reportPath));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      } else {
        URI filePath = path.resolve(file.getName() + "/");
        if (scheme.startsWith("s3")) {
          String reportPath = filePath.getPath().replaceAll("//", "/").replaceAll("^/", "");
          logger.info(
              RefString.format("Scanning peer uploads to %s at s3 %s/%s", file.getAbsolutePath(), bucket, reportPath));
          List<S3ObjectSummary> preexistingFiles = s3
              .listObjects(new ListObjectsRequest().withBucketName(bucket).withPrefix(reportPath)).getObjectSummaries()
              .stream().collect(Collectors.toList());
          for (S3ObjectSummary preexistingFile : preexistingFiles) {
            logger.info(RefString.format("Preexisting File: '%s' + '%s'", reportPath, preexistingFile.getKey()));
            RefUtil.freeRef(map.put(
                new File(file, preexistingFile.getKey()).getAbsoluteFile(),
                s3.getUrl(bucket, reportPath + preexistingFile.getKey())
            ));
          }
        }
        logger.info(RefString.format("Uploading folder %s to %s", file.getAbsolutePath(), filePath.toString()));
        for (File subfile : file.listFiles()) {
          map.putAll(upload(s3, filePath, subfile));
        }
      }
      return map;
    } catch (Throwable e) {
      if (retries > 0) {
        return upload(s3, path, file, retries - 1);
      }
      throw new RuntimeException("Error uploading " + file + " to " + path, e);
    }
  }

  public static InputStream cache(@Nonnull final AmazonS3 s3, @Nonnull final URI cacheLocation, @Nonnull URI resourceLocation) {
    return cache(s3, cacheLocation, tempFile(cacheLocation), Util.getStreamSupplier(resourceLocation));
  }

  public static InputStream cache(@Nonnull AmazonS3 s3, @Nonnull URI s3File, @Nonnull File localFile, @Nonnull Supplier<InputStream> streamSupplier) {
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

  @Nonnull
  public static File tempFile(@Nonnull URI file) {
    File localFile;
    try {
      localFile = File.createTempFile(file.getPath(), "data");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return localFile;
  }

  @Nonnull
  public static String defaultPolicy(@Nonnull final String... bucket) {
    String bucketGrant = RefUtil.get(RefArrays.stream(bucket).map(b -> "{\n" + "      \"Action\": \"s3:*\",\n"
        + "      \"Effect\": \"Allow\",\n" + "      \"Resource\": \"arn:aws:s3:::" + b + "*\"\n" + "    }")
        .reduce((a, b) -> a + "," + b));
    return "{\n" + "  \"Version\": \"2012-10-17\",\n" + "  \"Statement\": [\n" + "    " + bucketGrant + ",\n"
        + "    {\n" + "      \"Action\": \"s3:ListBucket*\",\n" + "      \"Effect\": \"Allow\",\n"
        + "      \"Resource\": \"arn:aws:s3:::*\"\n" + "    },\n" + "    {\n"
        + "      \"Action\": [\"ses:SendEmail\",\"ses:SendRawEmail\"],\n" + "      \"Effect\": \"Allow\",\n"
        + "      \"Resource\": \"*\"\n" + "    }\n" + "  ]\n" + "}";
  }
}
