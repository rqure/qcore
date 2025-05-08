package main

import (
	"context"

	"github.com/coder/websocket"
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
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
	OnMessageReceived(args MessageReceivedArgs)
}

type writeWorker struct {
	store   *qdata.Store
	isReady bool

	handle qcontext.Handle
}

func NewWriteWorker(store *qdata.Store) WriteWorker {
	return &writeWorker{
		store: store,
	}
}

func (w *writeWorker) Deinit(context.Context) {}
func (w *writeWorker) DoWork(context.Context) {}
func (w *writeWorker) OnReady(ctx context.Context) {
	w.isReady = true
}

func (w *writeWorker) OnNotReady(context.Context) {
	w.isReady = false
}

func (w *writeWorker) Init(ctx context.Context) {
	w.handle = qcontext.GetHandle(ctx)
}

func (w *writeWorker) OnMessageReceived(args MessageReceivedArgs) {
	switch {
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiConfigCreateEntityRequest{}):
		w.handleCreateEntity(args)
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiConfigDeleteEntityRequest{}):
		w.handleDeleteEntity(args)
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiConfigSetEntitySchemaRequest{}):
		w.handleSetEntitySchema(args)
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiConfigRestoreSnapshotRequest{}):
		w.handleRestoreSnapshot(args)
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeDatabaseRequest{}):
		w.handleDatabaseRequest(args)
	default:
		qlog.Warn("Unknown message type: %v", args.Msg.Payload.TypeUrl)
	}
}

func (w *writeWorker) handleCreateEntity(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiConfigCreateEntityRequest)
	rsp := new(qprotobufs.ApiConfigCreateEntityResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	if !w.isReady {
		qlog.Warn("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := verifyAuthentication(ctx, args.Msg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	entity, err := w.store.CreateEntity(
		ctx,
		qdata.EntityType(req.Type),
		qdata.EntityId(req.ParentId),
		req.Name)
	if err != nil {
		qlog.Warn("Could not create entity: %v", err)
		rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	rsp.Id = entity.EntityId.AsString()
	rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_SUCCESS
	w.sendResponse(args, rsp)
}

func (w *writeWorker) handleDeleteEntity(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiConfigDeleteEntityRequest)
	rsp := new(qprotobufs.ApiConfigDeleteEntityResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	if !w.isReady {
		qlog.Warn("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := verifyAuthentication(ctx, args.Msg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	if err := w.store.DeleteEntity(ctx, qdata.EntityId(req.Id)); err != nil {
		qlog.Warn("Could not delete entity: %v", err)
		rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_SUCCESS
	w.sendResponse(args, rsp)
}

func (w *writeWorker) handleSetEntitySchema(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiConfigSetEntitySchemaRequest)
	rsp := new(qprotobufs.ApiConfigSetEntitySchemaResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	if !w.isReady {
		qlog.Warn("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := verifyAuthentication(ctx, args.Msg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	if err := w.store.SetEntitySchema(ctx, new(qdata.EntitySchema).FromEntitySchemaPb(req.Schema)); err != nil {
		qlog.Warn("Could not set entity schema: %v", err)
		rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_SUCCESS
	w.sendResponse(args, rsp)
}

func (w *writeWorker) handleRestoreSnapshot(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiConfigRestoreSnapshotRequest)
	rsp := new(qprotobufs.ApiConfigRestoreSnapshotResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	if !w.isReady {
		qlog.Warn("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := verifyAuthentication(ctx, args.Msg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	if err := w.store.RestoreSnapshot(ctx, new(qdata.Snapshot).FromSnapshotPb(req.Snapshot)); err != nil {
		qlog.Warn("Could not restore snapshot: %v", err)
		rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_SUCCESS
	w.sendResponse(args, rsp)
}

func (w *writeWorker) handleDatabaseRequest(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeDatabaseRequest)
	rsp := new(qprotobufs.ApiRuntimeDatabaseResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	if !w.isReady {
		qlog.Warn("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	if req.RequestType != qprotobufs.ApiRuntimeDatabaseRequest_WRITE {
		qlog.Warn("Only WRITE requests are supported")
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	rsp.Response = req.Requests

	reqs := []*qdata.Request{}
	for _, r := range req.Requests {
		reqs = append(reqs, new(qdata.Request).FromRequestPb(r))
	}

	qlog.Info("Write request: %v", req.Requests)

	ctx := args.Ctx
	authCtx, ok := verifyAuthentication(ctx, args.Msg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	// Perform write operation with the authorized context
	w.store.Write(ctx, reqs...)

	for i, req := range reqs {
		rsp.Response[i] = req.AsRequestPb()
	}

	rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_SUCCESS
	w.sendResponse(args, rsp)
}

func (w *writeWorker) sendResponse(args MessageReceivedArgs, response proto.Message) {
	args.Msg.Header.Timestamp = timestamppb.Now()

	var err error
	args.Msg.Payload, err = anypb.New(response)
	if err != nil {
		qlog.Warn("Could not marshal response: %v", err)
		return
	}

	data, err := proto.Marshal(args.Msg)
	if err != nil {
		qlog.Warn("Could not marshal message: %v", err)
		return
	}

	if err := args.Conn.Write(args.Ctx, websocket.MessageBinary, data); err != nil {
		qlog.Warn("Could not send response: %v", err)
	}
}
