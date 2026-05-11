import io.grpc.stub.StreamObserver;

public class TemperaturaServiceImpl
        extends TemperaturaServiceGrpc.TemperaturaServiceImplBase {

    @Override
    public void getUltimaLeitura(
            SensorRequest request,
            StreamObserver<TemperaturaResponse> responseObserver) {

        TemperaturaResponse response =
                TemperaturaResponse.newBuilder()
                        .setSensorId("sensor-01")
                        .setMedia(30.5)
                        .setMaxima(33.2)
                        .setMinima(28.0)
                        .setTimestamp("2026-05-11")
                        .build();

        responseObserver.onNext(response);

        responseObserver.onCompleted();
    }
}
