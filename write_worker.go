package main

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qauthentication"
	"github.com/rqure/qlib/pkg/qauthorization"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qstore/qnats"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WriteWorker interface {
	qapp.Worker
	OnReady(context.Context)
	OnNotReady(context.Context)
}

type writeWorker struct {
	store       *qdata.Store
	natsCore    qnats.NatsCore
	isReady     bool
	modeManager ModeManager

	handle qcontext.Handle
}

func NewWriteWorker(store *qdata.Store, natsCore qnats.NatsCore, modeManager ModeManager) WriteWorker {
	return &writeWorker{
		store:       store,
		natsCore:    natsCore,
		modeManager: modeManager,
	}
}

func (w *writeWorker) Deinit(context.Context) {}
func (w *writeWorker) DoWork(context.Context) {}
func (w *writeWorker) OnReady(ctx context.Context) {
	w.isReady = true

	if w.modeManager.HasModes(ModeWrite) {
		w.natsCore.QueueSubscribe(
			w.natsCore.GetKeyGenerator().GetWriteSubject(),
			qcontext.GetAppName(ctx),
			w.handleWriteRequest)
	}
}

func (w *writeWorker) OnNotReady(context.Context) {
	w.isReady = false
}

func (w *writeWorker) Init(ctx context.Context) {
	w.handle = qcontext.GetHandle(ctx)
}

func (w *writeWorker) handleWriteRequest(msg *nats.Msg) {
	w.handle.DoInMainThread(func(ctx context.Context) {
		var apiMsg qprotobufs.ApiMessage
		if err := proto.Unmarshal(msg.Data, &apiMsg); err != nil {
			qlog.Warn("Could not unmarshal message: %v", err)
			return
		}

		switch {
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiConfigCreateEntityRequest{}):
			w.handleCreateEntity(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiConfigDeleteEntityRequest{}):
			w.handleDeleteEntity(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiConfigSetEntitySchemaRequest{}):
			w.handleSetEntitySchema(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiConfigRestoreSnapshotRequest{}):
			w.handleRestoreSnapshot(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeDatabaseRequest{}):
			w.handleDatabaseRequest(ctx, msg, &apiMsg)
		}
	})
}

func (w *writeWorker) handleCreateEntity(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiConfigCreateEntityRequest)
	rsp := new(qprotobufs.ApiConfigCreateEntityResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isReady {
		qlog.Warn("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	entity, err := w.store.CreateEntity(
		ctx,
		qdata.EntityType(req.Type),
		qdata.EntityId(req.ParentId),
		req.Name)
	if err != nil {
		qlog.Warn("Could not create entity: %v", err)
		rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	rsp.Id = entity.EntityId.AsString()
	rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *writeWorker) handleDeleteEntity(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiConfigDeleteEntityRequest)
	rsp := new(qprotobufs.ApiConfigDeleteEntityResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isReady {
		qlog.Warn("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if err := w.store.DeleteEntity(ctx, qdata.EntityId(req.Id)); err != nil {
		qlog.Warn("Could not delete entity: %v", err)
		rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *writeWorker) handleSetEntitySchema(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiConfigSetEntitySchemaRequest)
	rsp := new(qprotobufs.ApiConfigSetEntitySchemaResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isReady {
		qlog.Warn("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if err := w.store.SetEntitySchema(ctx, new(qdata.EntitySchema).FromEntitySchemaPb(req.Schema)); err != nil {
		qlog.Warn("Could not set entity schema: %v", err)
		rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *writeWorker) handleRestoreSnapshot(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiConfigRestoreSnapshotRequest)
	rsp := new(qprotobufs.ApiConfigRestoreSnapshotResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isReady {
		qlog.Warn("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if err := w.store.RestoreSnapshot(ctx, new(qdata.Snapshot).FromSnapshotPb(req.Snapshot)); err != nil {
		qlog.Warn("Could not restore snapshot: %v", err)
		rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *writeWorker) handleDatabaseRequest(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeDatabaseRequest)
	rsp := new(qprotobufs.ApiRuntimeDatabaseResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isReady {
		qlog.Warn("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if req.RequestType != qprotobufs.ApiRuntimeDatabaseRequest_WRITE {
		qlog.Warn("Only WRITE requests are supported")
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	reqs := []*qdata.Request{}
	for _, r := range req.Requests {
		reqs = append(reqs, new(qdata.Request).FromRequestPb(r))
	}

	qlog.Info("Write request: %v", req.Requests)
	clientProvider := qcontext.GetClientProvider[qauthentication.Client](ctx)
	client := clientProvider.Client(ctx)
	if client != nil {
		accessorSession := client.AccessTokenToSession(ctx, apiMsg.Header.AccessToken)

		if !accessorSession.IsValid(ctx) {
			qlog.Warn("Invalid session")
			rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
			w.sendResponse(msg, rsp)
			return
		}

		accessorName, err := accessorSession.GetOwnerName(ctx)
		if err != nil {
			qlog.Warn("Could not get owner name: %v", err)
			rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
			w.sendResponse(msg, rsp)
			return
		}

		found := false
		iter, err := w.store.PrepareQuery(`SELECT "$EntityId" FROM User WHERE Name = %q`, accessorName)
		if err != nil {
			qlog.Warn("Could not prepare query: %v", err)
			rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
			w.sendResponse(msg, rsp)
			return
		}
		defer iter.Close()
		iter.ForEach(ctx, func(row qdata.QueryRow) bool {
			user := row.AsEntity()
			w.store.Write(
				context.WithValue(
					ctx,
					qcontext.KeyAuthorizer,
					qauthorization.NewAuthorizer(user.EntityId, w.store)),
				reqs...)

			found = true

			// Break after first user
			return false
		})

		if !found {
			iter, err := w.store.PrepareQuery(`SELECT "$EntityId" FROM Client WHERE Name = %q`, accessorName)
			if err != nil {
				qlog.Warn("Could not prepare query: %v", err)
				rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
				w.sendResponse(msg, rsp)
				return
			}
			defer iter.Close()

			iter.ForEach(ctx, func(row qdata.QueryRow) bool {
				client := row.AsEntity()
				w.store.Write(
					context.WithValue(
						ctx,
						qcontext.KeyAuthorizer,
						qauthorization.NewAuthorizer(client.EntityId, w.store)),
					reqs...)

				// Break after first client
				return false
			})
		}
	}

	rsp.Response = req.Requests
	rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *writeWorker) sendResponse(msg *nats.Msg, response proto.Message) {
	if msg.Reply == "" {
		return
	}

	apiMsg := &qprotobufs.ApiMessage{
		Header: &qprotobufs.ApiHeader{
			Timestamp: timestamppb.Now(),
		},
	}

	var err error
	apiMsg.Payload, err = anypb.New(response)
	if err != nil {
		qlog.Warn("Could not marshal response: %v", err)
		return
	}

	data, err := proto.Marshal(apiMsg)
	if err != nil {
		qlog.Warn("Could not marshal message: %v", err)
		return
	}

	if err := msg.Respond(data); err != nil {
		qlog.Warn("Could not send response: %v", err)
	}
}
