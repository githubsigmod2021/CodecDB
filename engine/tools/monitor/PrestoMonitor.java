import java.util.*;
import javax.management.*;
import javax.management.remote.*;

public class PrestoMonitor {

    public static void main(String... args) throws Exception {

	String url = "service:jmx:rmi:///jndi/rmi://localhost:9080/jmxrmi";
        MBeanServerConnection server = JMXConnectorFactory.connect(new JMXServiceURL(url))
            .getMBeanServerConnection();

	/*
        Set<ObjectInstance> mbeans = server.queryMBeans(null,null);
	for(ObjectInstance mbean: mbeans) {
	       if(mbean.getObjectName().getCanonicalName().startsWith("com.facebook")) {
               System.out.println(mbean.getObjectName().getCanonicalName());
	       System.out.println(mbean.getObjectName().getCanonicalKeyPropertyListString());
	       }
	}
	*/

	ObjectName obj = new ObjectName("com.facebook.presto.memory:name=general,type=MemoryPool");
/*
	MBeanInfo info = server.getMBeanInfo(obj);
    MBeanAttributeInfo[] attrInfo = info.getAttributes();

    for (MBeanAttributeInfo attr : attrInfo)
    {
        System.out.println("  " + attr.getName() + "\n");
    }
*/
	int repeat = Integer.parseInt(args[0]);
	String attr = "ReservedBytes";
	if(args.length > 1) {
		attr = args[1];
	}
	for(int i = 0 ; i < repeat; ++i) {
		Thread.sleep(100);
		System.out.println(server.getAttribute(obj, attr));
	}
    }

}
