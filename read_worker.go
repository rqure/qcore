package main

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qentity"
	"github.com/rqure/qlib/pkg/qdata/qquery"
	"github.com/rqure/qlib/pkg/qdata/qrequest"
	"github.com/rqure/qlib/pkg/qdata/qstore"
	"github.com/rqure/qlib/pkg/qdata/qstore/qnats"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ReadWorker interface {
	qapp.Worker
	OnStoreConnected(context.Context)
	OnStoreDisconnected()
}

type readWorker struct {
	store            qdata.Store
	natsCore         qnats.Core
	isStoreConnected bool
	modeManager      ModeManager
	handle           qapp.Handle
}

func NewReadWorker(store qdata.Store, natsCore qnats.Core, modeManager ModeManager) ReadWorker {
	return &readWorker{
		store:       store,
		natsCore:    natsCore,
		modeManager: modeManager,
	}
}

func (w *readWorker) Init(ctx context.Context) {
	w.handle = qapp.GetHandle(ctx)
}

func (w *readWorker) Deinit(context.Context) {}
func (w *readWorker) DoWork(context.Context) {}

func (w *readWorker) handleReadRequest(msg *nats.Msg) {
	w.handle.DoInMainThread(func(ctx context.Context) {
		var apiMsg qprotobufs.ApiMessage
		if err := proto.Unmarshal(msg.Data, &apiMsg); err != nil {
			qlog.Error("Could not unmarshal message: %v", err)
			return
		}

		switch {
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiConfigGetEntityRequest{}):
			w.handleGetEntity(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiConfigGetEntityTypesRequest{}):
			w.handleGetEntityTypes(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiConfigGetEntitySchemaRequest{}):
			w.handleGetEntitySchema(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiConfigGetRootRequest{}):
			w.handleGetRoot(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeGetEntitiesRequest{}):
			w.handleGetEntities(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeFieldExistsRequest{}):
			w.handleFieldExists(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeEntityExistsRequest{}):
			w.handleEntityExists(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeGetDatabaseConnectionStatusRequest{}):
			w.handleGetDatabaseConnectionStatus(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeDatabaseRequest{}):
			w.handleDatabaseRequest(ctx, msg, &apiMsg)
		}
	})
}

func (w *readWorker) handleGetEntity(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiConfigGetEntityRequest)
	rsp := new(qprotobufs.ApiConfigGetEntityResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigGetEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isStoreConnected {
		qlog.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigGetEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	ent := w.store.GetEntity(ctx, req.Id)
	if ent == nil {
		qlog.Error("Could not get entity")
		rsp.Status = qprotobufs.ApiConfigGetEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	rsp.Entity = qentity.ToEntityPb(ent)
	rsp.Status = qprotobufs.ApiConfigGetEntityResponse_SUCCESS

	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleGetEntityTypes(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiConfigGetEntityTypesRequest)
	rsp := new(qprotobufs.ApiConfigGetEntityTypesResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	types := w.store.GetEntityTypes(ctx)
	rsp.Types = types

	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleGetEntitySchema(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiConfigGetEntitySchemaRequest)
	rsp := new(qprotobufs.ApiConfigGetEntitySchemaResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	schema := w.store.GetEntitySchema(ctx, req.Type) // Changed from req.EntityType to req.Type
	if schema == nil {
		qlog.Error("Schema not found")
		w.sendResponse(msg, rsp)
		return
	}

	pbSchema := qentity.ToSchemaPb(schema)
	rsp.Schema = pbSchema

	w.sendResponse(msg, rsp)
}

// Add new handler methods
func (w *readWorker) handleGetRoot(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiConfigGetRootRequest)
	rsp := new(qprotobufs.ApiConfigGetRootResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	root := w.store.FindEntities(ctx, "Root")

	for _, id := range root {
		rsp.RootId = id
	}

	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleGetEntities(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeGetEntitiesRequest)
	rsp := new(qprotobufs.ApiRuntimeGetEntitiesResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	entities := qquery.New(w.store).
		Select().
		From(req.EntityType).
		Execute(ctx)

	for _, ent := range entities {
		rsp.Entities = append(rsp.Entities, qentity.ToEntityPb(ent))
	}

	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleFieldExists(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeFieldExistsRequest)
	rsp := new(qprotobufs.ApiRuntimeFieldExistsResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	exists := w.store.FieldExists(ctx, req.FieldName, req.EntityType)
	rsp.Exists = exists

	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleEntityExists(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeEntityExistsRequest)
	rsp := new(qprotobufs.ApiRuntimeEntityExistsResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	exists := w.store.EntityExists(ctx, req.EntityId)
	rsp.Exists = exists

	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleGetDatabaseConnectionStatus(_ context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeGetDatabaseConnectionStatusRequest)
	rsp := new(qprotobufs.ApiRuntimeGetDatabaseConnectionStatusResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	rsp.Connected = w.isStoreConnected

	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleDatabaseRequest(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeDatabaseRequest)
	rsp := new(qprotobufs.ApiRuntimeDatabaseResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	// Only handle READ requests
	if req.RequestType != qprotobufs.ApiRuntimeDatabaseRequest_READ {
		qlog.Error("Only READ requests are supported")
		w.sendResponse(msg, rsp)
		return
	}

	reqs := []qdata.Request{}
	for _, r := range req.Requests {
		reqs = append(reqs, qrequest.FromPb(r))
	}

	qlog.Info("Read request: %v", req.Requests)
	if client := w.store.AuthClient(ctx); client != nil {
		accessorSession := client.AccessTokenToSession(ctx, apiMsg.Header.AccessToken)

		if !accessorSession.IsValid(ctx) {
			qlog.Warn("Invalid session")
			return
		}

		accessorName, err := accessorSession.GetOwnerName(ctx)
		if err != nil {
			qlog.Error("Could not get owner name: %v", err)
			return
		}

		users := qquery.New(w.store).
			Select().
			From("User").
			Where("Name").Equals(accessorName).
			Execute(ctx)

		for _, user := range users {
			authorizer := qstore.NewFieldAuthorizer(user.GetId(), w.store)
			w.store.Read(context.WithValue(ctx, qdata.FieldAuthorizerKey, authorizer), reqs...)

			// Break after first user
			break
		}

		if len(users) == 0 {
			clients := qquery.New(w.store).
				Select().
				From("Client").
				Where("Name").Equals(accessorName).
				Execute(ctx)

			for _, client := range clients {
				authorizer := qstore.NewFieldAuthorizer(client.GetId(), w.store)
				w.store.Read(context.WithValue(ctx, qdata.FieldAuthorizerKey, authorizer), reqs...)

				// Break after first client
				break
			}
		}
	}

	rsp.Response = req.Requests

	w.sendResponse(msg, rsp)
}

// Helper methods for common operations
func (w *readWorker) sendResponse(msg *nats.Msg, response proto.Message) {
	if msg.Reply == "" {
		return
	}

	apiMsg := &qprotobufs.ApiMessage{
		Header: &qprotobufs.ApiHeader{
			Timestamp: timestamppb.Now(),
		},
	}

	if err := apiMsg.Payload.MarshalFrom(response); err != nil {
		qlog.Error("Could not marshal response: %v", err)
		return
	}

	data, err := proto.Marshal(apiMsg)
	if err != nil {
		qlog.Error("Could not marshal message: %v", err)
		return
	}

	if err := msg.Respond(data); err != nil {
		qlog.Error("Could not send response: %v", err)
	}
}

func (w *readWorker) OnStoreConnected(context.Context) {
	w.isStoreConnected = true

	if w.modeManager.HasModes(ModeRead) {
		w.natsCore.QueueSubscribe(w.natsCore.GetKeyGenerator().GetReadSubject(), w.handleReadRequest)
	}
}

func (w *readWorker) OnStoreDisconnected() {
	w.isStoreConnected = false
}
