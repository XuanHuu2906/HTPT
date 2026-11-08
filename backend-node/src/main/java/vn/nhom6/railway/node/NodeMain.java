package vn.nhom6.railway.node;

/**
 * Entry point delegating to {@link vn.nhom6.railway.networking.NodeMain}.
 *
 * <p>Ensures seamless compatibility with container startup scripts
 * (e.g. {@code docker/entrypoint-backend.sh}) expecting {@code vn.nhom6.railway.node.NodeMain}.
 */
public final class NodeMain {

    private NodeMain() {
        // Prevent instantiation
    }

    public static void main(String[] args) {
        vn.nhom6.railway.networking.NodeMain.main(args);
    }
}
