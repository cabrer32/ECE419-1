package testing;

import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.*;

public class ECSNodeTest extends TestCase {


    private ECSNode node = null;

    @Before
    public void setUp() {
        node = new ECSNode("server-test", "127.0.0.1", 50000, "a");
        node.setEndingHashValue("z");
    }

    @After
    public void tearDown() {
        node = null;
    }


    @Test
    public void testgetNodeName() {

        String name = node.getNodeName();
        assertTrue(name.equals("server-test"));
    }


    @Test
    public void testgetNodeHost() {

        String host = node.getNodeHost();
        assertTrue(host.equals("127.0.0.1"));
    }

    @Test
    public void testgetNodePort() {

        int port = node.getNodePort();
        assertTrue(port == 50000);
    }

    @Test
    public void testGetHashRange() {

        String range[] = node.getNodeHashRange();

        assertTrue(range[0].equals("a"));
        assertTrue(range[1].equals("z"));
    }

}
