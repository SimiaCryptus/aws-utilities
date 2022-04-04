import org.junit.Test;

public class QuickTest {

  @Test
  public void test() {
    String command = "cat >/sbin/mindseye <<EOF\n" +
        "#!/bin/bash\n" +
        "sudo -H -u ec2-user /home/ec2-user/bin/mindseye\n" +
        "EOF";
    String quoted = command.replaceAll("\"", "\\\"").replaceAll("\n", "\\\\n");
    System.out.printf("sudo -i \"%s\"%n", quoted);
  }
}
