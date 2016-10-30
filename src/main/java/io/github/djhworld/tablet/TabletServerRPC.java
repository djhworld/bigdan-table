package io.github.djhworld.tablet;

import com.google.common.collect.Range;
import io.github.djhworld.helloworld.*;
import io.github.djhworld.model.RowMutation;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class TabletServerRPC extends TabletServerGrpc.TabletServerImplBase {
    private TabletServer tabletServer;

    public TabletServerRPC() throws IOException {
        tabletServer = new TabletServer(new TabletMetadataService(), new ScheduledThreadPoolExecutor(1));
        tabletServer.register(Range.open("a","z"), "1");
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            Optional<String> response = tabletServer.get(request.getRow(), request.getColumn());
            responseObserver.onNext(GetResponse.newBuilder().setValue(response.orElse(null)).build());
            responseObserver.onCompleted();
        } catch(Exception e) {
            responseObserver.onError(e);
        } finally {
        }
    }

    @Override
    public void batchAdd(BatchAddItemRequest request, StreamObserver<BatchAddItemResponse> responseObserver) {
        for(AddItemRequest addItemRequest : request.getAddItemRequestsList()) {
            tabletServer.apply(RowMutation.newAddMutation(addItemRequest.getRow(), addItemRequest.getColumn(), addItemRequest.getValue()));
        }

        responseObserver.onNext(BatchAddItemResponse.newBuilder().setOk(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void add(AddItemRequest request, StreamObserver<AddItemResponse> responseObserver) {
        tabletServer.apply(RowMutation.newAddMutation(request.getRow(), request.getColumn(), request.getValue()));
        responseObserver.onNext(AddItemResponse.newBuilder().setOk(true).build());
        responseObserver.onCompleted();
    }
}
