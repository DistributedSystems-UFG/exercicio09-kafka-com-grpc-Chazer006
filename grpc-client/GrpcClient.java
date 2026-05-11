import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class GrpcClient {

    public static void main(String[] args) {

        ManagedChannel channel =
                ManagedChannelBuilder
                        .forAddress("localhost", 50051)
                        .usePlaintext()
                        .build();

        TemperaturaServiceGrpc.TemperaturaServiceBlockingStub stub =
                TemperaturaServiceGrpc.newBlockingStub(channel);

        SensorRequest request =
                SensorRequest.newBuilder()
                        .setSensorId("sensor-01")
                        .build();

        TemperaturaResponse response =
                stub.getUltimaLeitura(request);

        System.out.println("Média: " + response.getMedia());

        channel.shutdown();
    }
}
