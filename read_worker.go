package main

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/data/query"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/data/store"
	qnats "github.com/rqure/qlib/pkg/data/store/nats"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ReadWorker interface {
	app.Worker
	OnStoreConnected(context.Context)
	OnStoreDisconnected()
}

type readWorker struct {
	store            data.Store
	natsCore         qnats.Core
	isStoreConnected bool
	modeManager      ModeManager
	handle           app.Handle
}

func NewReadWorker(store data.Store, natsCore qnats.Core, modeManager ModeManager) ReadWorker {
	return &readWorker{
		store:       store,
		natsCore:    natsCore,
		modeManager: modeManager,
	}
}

func (w *readWorker) Init(ctx context.Context, handle app.Handle) {
	w.handle = handle
}

func (w *readWorker) Deinit(context.Context) {}
func (w *readWorker) DoWork(context.Context) {}

func (w *readWorker) handleReadRequest(msg *nats.Msg) {
	w.handle.DoInMainThread(func(ctx context.Context) {
		var apiMsg protobufs.ApiMessage
		if err := proto.Unmarshal(msg.Data, &apiMsg); err != nil {
			log.Error("Could not unmarshal message: %v", err)
			return
		}

		switch {
		case apiMsg.Payload.MessageIs(&protobufs.ApiConfigGetEntityRequest{}):
			w.handleGetEntity(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&protobufs.ApiConfigGetEntityTypesRequest{}):
			w.handleGetEntityTypes(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&protobufs.ApiConfigGetEntitySchemaRequest{}):
			w.handleGetEntitySchema(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&protobufs.ApiConfigGetRootRequest{}):
			w.handleGetRoot(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&protobufs.ApiRuntimeGetEntitiesRequest{}):
			w.handleGetEntities(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&protobufs.ApiRuntimeFieldExistsRequest{}):
			w.handleFieldExists(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&protobufs.ApiRuntimeEntityExistsRequest{}):
			w.handleEntityExists(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&protobufs.ApiRuntimeGetDatabaseConnectionStatusRequest{}):
			w.handleGetDatabaseConnectionStatus(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&protobufs.ApiRuntimeDatabaseRequest{}):
			w.handleDatabaseRequest(ctx, msg, &apiMsg)
		}
	})
}

func (w *readWorker) handleGetEntity(ctx context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiConfigGetEntityRequest)
	rsp := new(protobufs.ApiConfigGetEntityResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		rsp.Status = protobufs.ApiConfigGetEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = protobufs.ApiConfigGetEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	ent := w.store.GetEntity(ctx, req.Id)
	if ent == nil {
		log.Error("Could not get entity")
		rsp.Status = protobufs.ApiConfigGetEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	rsp.Entity = entity.ToEntityPb(ent)
	rsp.Status = protobufs.ApiConfigGetEntityResponse_SUCCESS

	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleGetEntityTypes(ctx context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiConfigGetEntityTypesRequest)
	rsp := new(protobufs.ApiConfigGetEntityTypesResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	types := w.store.GetEntityTypes(ctx)
	rsp.Types = types

	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleGetEntitySchema(ctx context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiConfigGetEntitySchemaRequest)
	rsp := new(protobufs.ApiConfigGetEntitySchemaResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	schema := w.store.GetEntitySchema(ctx, req.Type) // Changed from req.EntityType to req.Type
	if schema == nil {
		log.Error("Schema not found")
		w.sendResponse(msg, rsp)
		return
	}

	pbSchema := entity.ToSchemaPb(schema)
	rsp.Schema = pbSchema

	w.sendResponse(msg, rsp)
}

// Add new handler methods
func (w *readWorker) handleGetRoot(ctx context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiConfigGetRootRequest)
	rsp := new(protobufs.ApiConfigGetRootResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	root := w.store.FindEntities(ctx, "Root")

	for _, id := range root {
		rsp.RootId = id
	}

	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleGetEntities(ctx context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiRuntimeGetEntitiesRequest)
	rsp := new(protobufs.ApiRuntimeGetEntitiesResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	entities := query.New(w.store).
		Select().
		From(req.EntityType).
		Execute(ctx)

	for _, ent := range entities {
		rsp.Entities = append(rsp.Entities, entity.ToEntityPb(ent))
	}

	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleFieldExists(ctx context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiRuntimeFieldExistsRequest)
	rsp := new(protobufs.ApiRuntimeFieldExistsResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	exists := w.store.FieldExists(ctx, req.FieldName, req.EntityType)
	rsp.Exists = exists

	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleEntityExists(ctx context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiRuntimeEntityExistsRequest)
	rsp := new(protobufs.ApiRuntimeEntityExistsResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	exists := w.store.EntityExists(ctx, req.EntityId)
	rsp.Exists = exists

	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleGetDatabaseConnectionStatus(_ context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiRuntimeGetDatabaseConnectionStatusRequest)
	rsp := new(protobufs.ApiRuntimeGetDatabaseConnectionStatusResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	rsp.Connected = w.isStoreConnected

	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleDatabaseRequest(ctx context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiRuntimeDatabaseRequest)
	rsp := new(protobufs.ApiRuntimeDatabaseResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	// Only handle READ requests
	if req.RequestType != protobufs.ApiRuntimeDatabaseRequest_READ {
		log.Error("Only READ requests are supported")
		w.sendResponse(msg, rsp)
		return
	}

	reqs := []data.Request{}
	for _, r := range req.Requests {
		reqs = append(reqs, request.FromPb(r))
	}

	log.Info("Read request: %v", req.Requests)
	if client := w.store.AuthClient(ctx); client != nil {
		accessorSession := client.AccessTokenToSession(ctx, apiMsg.Header.AccessToken)

		if !accessorSession.IsValid(ctx) {
			log.Warn("Invalid session")
			return
		}

		accessorName, err := accessorSession.GetOwnerName(ctx)
		if err != nil {
			log.Error("Could not get owner name: %v", err)
			return
		}

		users := query.New(w.store).
			Select().
			From("User").
			Where("Name").Equals(accessorName).
			Execute(ctx)

		for _, user := range users {
			authorizer := store.NewFieldAuthorizer(user.GetId(), w.store)
			w.store.Read(context.WithValue(ctx, data.FieldAuthorizerKey, authorizer), reqs...)

			// Break after first user
			break
		}

		if len(users) == 0 {
			clients := query.New(w.store).
				Select().
				From("Client").
				Where("Name").Equals(accessorName).
				Execute(ctx)

			for _, client := range clients {
				authorizer := store.NewFieldAuthorizer(client.GetId(), w.store)
				w.store.Read(context.WithValue(ctx, data.FieldAuthorizerKey, authorizer), reqs...)

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

	apiMsg := &protobufs.ApiMessage{
		Header: &protobufs.ApiHeader{
			Timestamp: timestamppb.Now(),
		},
	}

	if err := apiMsg.Payload.MarshalFrom(response); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	data, err := proto.Marshal(apiMsg)
	if err != nil {
		log.Error("Could not marshal message: %v", err)
		return
	}

	if err := msg.Respond(data); err != nil {
		log.Error("Could not send response: %v", err)
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
