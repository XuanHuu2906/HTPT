import org.junit.platform.launcher.core.*;
import org.junit.platform.launcher.listeners.*;
import static org.junit.platform.engine.discovery.DiscoverySelectors.*;
public class RaftD10Runner {
 public static void main(String[] args) {
  var builder = LauncherDiscoveryRequestBuilder.request();
  for (String name : args) builder.selectors(selectClass(name));
  var listener = new SummaryGeneratingListener();
  var launcher = LauncherFactory.create();
  launcher.registerTestExecutionListeners(listener);
  launcher.execute(builder.build());
  listener.getSummary().printTo(new java.io.PrintWriter(System.out));
  listener.getSummary().printFailuresTo(new java.io.PrintWriter(System.out));
  if (listener.getSummary().getTestsFailedCount() > 0) System.exit(1);
 }
}
