package com.simiacryptus.aws;

import jnr.posix.util.Chmod;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class LinuxUtil {
  public static void installCurrentAppAsStartupDaemon() {
    String homeDir = "/home/ec2-user";
    String mainClass = "com.simiacryptus.mindseye.art.libs.StandardLibraryNotebook";
    String email = "your@email.com";
    String exeName = "mindseye";
    String runAs = "ec2-user";
    Map<String, String> defines = new HashMap<>(System.getProperties().entrySet().stream()
        .filter(t -> !t.getKey().toString().startsWith("awt."))
        .filter(t -> !t.getKey().toString().startsWith("com."))
        .filter(t -> !t.getKey().toString().startsWith("file."))
        .filter(t -> !t.getKey().toString().startsWith("java."))
        .filter(t -> !t.getKey().toString().startsWith("line."))
        .filter(t -> !t.getKey().toString().startsWith("log4j"))
        .filter(t -> !t.getKey().toString().startsWith("os."))
        .filter(t -> !t.getKey().toString().startsWith("path."))
        .filter(t -> !t.getKey().toString().startsWith("sun."))
        .filter(t -> !t.getKey().toString().startsWith("user."))
        .filter(t -> !t.getKey().toString().startsWith("tendril."))
        .filter(t -> !t.getValue().toString().contains(" "))
        .collect(Collectors.toMap(t->t.getKey().toString(), t->t.getValue().toString())));
    String[] classPath = System.getProperty("java.class.path", "").split(":");
    AppLaunchConfig appLaunchConfig = new AppLaunchConfig(
        classPath,
        runAs,
        defines.entrySet().stream().map(t-> String.format("-D%s=%s", t.getKey(), t.getValue())).toArray(String[]::new),
        mainClass,
        homeDir,
        email,
        exeName);
    writeLauncher(appLaunchConfig);
  }

  public static void run(String command) {
    try {
      System.out.println("Running " + command);
      Process process = new ProcessBuilder("/bin/bash", "-c", command.replaceAll("\n","\\n").replaceAll("\"","\\\"")).redirectErrorStream(true).start();
      InputStream processOut = process.getInputStream();
      int exitCode = process.waitFor();
      String output = IOUtils.toString(processOut, "UTF-8");
      System.out.println("Output: " + output);
      if (0 != exitCode) {
        throw new IllegalStateException("Error code " + exitCode);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void sudo(String command) {
    try {
      System.out.println("Running " + command);
      String quoted = command.replaceAll("\"", "\\\"").replaceAll("\n", "\\\\n");
      System.out.println("Sudo " + quoted);
      Process process = new ProcessBuilder("sudo", "/bin/bash", "-c", quoted).redirectErrorStream(true).start();
      InputStream processOut = process.getInputStream();
      int exitCode = process.waitFor();
      String output = IOUtils.toString(processOut, "UTF-8");
      System.out.println("Output: " + output);
      System.out.println("Error code " + exitCode);
      if (0 != exitCode) {
        //throw new IllegalStateException("Error code " + exitCode);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
      //throw new RuntimeException(e);
    } catch (IOException e) {
      e.printStackTrace();
      //throw new RuntimeException(e);
    }
  }

  public static void writeLauncher(AppLaunchConfig appLaunchConfig) {
    File home = new File(appLaunchConfig.homeDir);
    File binDir = new File(home, "bin");
    binDir.mkdirs();
    File exeFile = new File(binDir, appLaunchConfig.exeName);
    if(exeFile.exists()) {
      System.out.printf("Launcher already installed: %s%n", exeFile.getAbsolutePath());
      return;
    }
    File confFile = new File(binDir, appLaunchConfig.exeName + ".conf");
    FileOutputStream exeFileOut = null;
    FileOutputStream confFileOut = null;
    try {
      exeFileOut = new FileOutputStream(exeFile);
      confFileOut = new FileOutputStream(confFile);
      IOUtils.write(
          "#!/bin/bash\n" +
              "export EMAIL=\"" + appLaunchConfig.email + "\";\n" +
              "export S3_BUCKET=\"\";\n" +
              "\n", confFileOut, Charset.forName("UTF-8"));
      IOUtils.write(
          "#!/bin/bash\n" +
              "source "+appLaunchConfig.homeDir+"/bin/"+appLaunchConfig.exeName+".conf\n" +
              "cd "+ appLaunchConfig.homeDir +";\n" +
              "sudo -H -u "+ appLaunchConfig.runAs + " nohup java " +
              Arrays.stream(appLaunchConfig.jvmOptions).reduce((a,b)->a+" "+b).get() +
              " -cp " + Arrays.stream(appLaunchConfig.classPath).reduce((a,b)->a+":"+b).get() +
              " " + appLaunchConfig.mainClass + " &\n" +
              "\n", exeFileOut, Charset.forName("UTF-8"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if(null != confFileOut) {
        try {
          confFileOut.close();
        } catch (IOException e) {
          // Ignore
        }
      }
      if(null != exeFileOut) {
        try {
          exeFileOut.close();
        } catch (IOException e) {
          // Ignore
        }
      }
    }
    run("chmod 500 "+exeFile.getAbsolutePath());

    try {
      File sh = File.createTempFile(appLaunchConfig.exeName, "sh");
      FileUtils.write(sh, "#!/bin/bash\nsudo -H -u ec2-user /home/ec2-user/bin/mindseye","UTF-8");
      sudo("mv "+sh.getAbsolutePath()+" /sbin/"+appLaunchConfig.exeName);
      sudo("chown root /sbin/"+appLaunchConfig.exeName);
      sudo("chmod 555 /sbin/"+appLaunchConfig.exeName);
    } catch (IOException e ) {
      throw new RuntimeException(e);
    }

    sudo("sed 's|exit 0|/sbin/"+appLaunchConfig.exeName+"\\nexit 0|' -i /etc/rc.d/rc.local"); // Some systems terminate this file with an exit
    sudo("echo '/sbin/mindseye' >> /etc/rc.d/rc.local"); // Just in case it didn't
  }

  private static class AppLaunchConfig {
    private final String[] classPath;
    private final String runAs;
    private final String[] jvmOptions;
    private final String mainClass;
    private final String homeDir;
    private final String email;
    private final String exeName;

    private AppLaunchConfig(String[] classPath, String runAs, String[] jvmOptions, String mainClass, String homeDir, String email, String exeName) {
      this.classPath = classPath;
      this.runAs = runAs;
      this.jvmOptions = jvmOptions;
      this.mainClass = mainClass;
      this.homeDir = homeDir;
      this.email = email;
      this.exeName = exeName;
    }

  }
}
