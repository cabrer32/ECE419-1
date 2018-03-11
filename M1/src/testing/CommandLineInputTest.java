package testing;

import app_kvClient.KVClient;
import org.junit.Test;
import junit.framework.TestCase;
public class CommandLineInputTest extends TestCase {

    String InvalidHost = "connect localhost 1111";
    String InvalidPort = "connect localhost 111111111111111";
    String InvalidAction = "make localhost 100";
    String InvalidNumberOfArgs = "connect ";

    @Test // testing invalid hostname
    public void testInvalidHostname() {

        Exception ex = null;

        try {
            KVClient kvClient = new KVClient();
            kvClient.handleCommand(InvalidHost);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }

    @Test //testing invalid port number
    public void testInvalidPort() {

        Exception ex = null;


        try {
            KVClient kvClient = new KVClient();
            kvClient.handleCommand(InvalidPort);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }
    @Test //Testing wrong methods
    public void testWrongMethod() {

        Exception ex = null;


        try {
            KVClient kvClient = new KVClient();
            kvClient.handleCommand(InvalidAction);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }
    @Test //Invalid number of arguments
    public void testInvalidArguments() {

        Exception ex = null;


        try {
            KVClient kvClient = new KVClient();
            kvClient.handleCommand(InvalidNumberOfArgs);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }
}
