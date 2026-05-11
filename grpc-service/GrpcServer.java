import io.grpc.Server;
import io.grpc.ServerBuilder;

public class GrpcServer {

    public static void main(String[] args) throws Exception {

        Server server = ServerBuilder
                .forPort(50051)
                .addService(new TemperaturaServiceImpl())
                .build();

        server.start();

        System.out.println("Servidor gRPC iniciado");

        server.awaitTermination();
    }
}
