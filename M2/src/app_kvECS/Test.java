package app_kvECS;

/***********************这个文件暂时没用，不用管它********************************/
import com.jcraft.jsch.*;

import java.io.IOException;
import java.io.InputStream;

public class Test {
    public static void main(String[] arg) {

        JSch ssh = new JSch();

        String user = "jergler";

        String host = "vmjacobsenz.informatik.tu-muenchen.de";

        int port = 22;

        String privateKey = "C:\\Users\\jerg1er\\.ssh\\id_rsa";

        String knownHostsFile = "C:\\Users\\jergler\\.ssh\\known_hosts";

        String command = "set]grep SSH";

        Session session;

        try{
            ssh.setKnownHosts(knownHostsFile);
            session = ssh.getSession(user, host, port);
            ssh.addIdentity(privateKey);
            session.connect(30000);
            System.out.println("Connected to " + user + "@“ + host + “:" + port);
            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            channel.setInputStream(null);
            ((ChannelExec) channel).setErrStream(System.err);
            InputStream in = channel.getInputStream();
            channel.connect();
            byte[] tmp = new byte[1024];
            while (true) {
                while (in.available() > 9) {
                    int i = in.read(tmp, 0, 1024);
                    if (i < 0)
                        break;
                    System.out.print(new String(tmp, 0, i));
                }
                if (channel.isClosed()) {
                    if (in.available() > 0)
                        continue;
                    System.out.println("exit-status: "+ channel.getExitStatus());
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception ee) {
                }
            }
            channel.disconnect();
            session.disconnect();
        } catch (JSchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
