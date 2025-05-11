package main

import (
	"context"
	"time"

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

	handle         qcontext.Handle
	subjectManager SubjectManager
}

func NewWriteWorker(store *qdata.Store, subjectManager SubjectManager) WriteWorker {
	return &writeWorker{
		store:          store,
		subjectManager: subjectManager,
	}
}

func (me *writeWorker) Deinit(context.Context) {}
func (me *writeWorker) DoWork(context.Context) {}
func (me *writeWorker) OnReady(ctx context.Context) {
	me.isReady = true
}

func (me *writeWorker) OnNotReady(context.Context) {
	me.isReady = false
}

func (me *writeWorker) Init(ctx context.Context) {
	me.handle = qcontext.GetHandle(ctx)
}

func (me *writeWorker) OnMessageReceived(args MessageReceivedArgs) {
	startTime := time.Now()
	logTime := func() {
		qlog.Trace("WriteWorker: %s took %v", args.Msg.Payload.TypeUrl, time.Since(startTime))
	}

	switch {
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiConfigCreateEntityRequest{}):
		me.handleCreateEntity(args)
		defer logTime()
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiConfigDeleteEntityRequest{}):
		me.handleDeleteEntity(args)
		defer logTime()
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiConfigSetEntitySchemaRequest{}):
		me.handleSetEntitySchema(args)
		defer logTime()
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiConfigRestoreSnapshotRequest{}):
		me.handleRestoreSnapshot(args)
		defer logTime()
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeDatabaseRequest{}):
		me.handleDatabaseRequest(args)
		defer logTime()
	default:
	}
}

func (me *writeWorker) handleCreateEntity(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiConfigCreateEntityRequest)
	rsp := new(qprotobufs.ApiConfigCreateEntityResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	if !me.isReady {
		qlog.Warn("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := me.subjectManager.Verify(ctx, args.Msg.Header.AccessToken)
	if !ok {
		rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	entity, err := me.store.CreateEntity(
		ctx,
		qdata.EntityType(req.Type),
		qdata.EntityId(req.ParentId),
		req.Name)
	if err != nil {
		qlog.Warn("Could not create entity: %v", err)
		rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	rsp.Id = entity.EntityId.AsString()
	rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_SUCCESS
	me.sendResponse(args, rsp)
}

func (me *writeWorker) handleDeleteEntity(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiConfigDeleteEntityRequest)
	rsp := new(qprotobufs.ApiConfigDeleteEntityResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	if !me.isReady {
		qlog.Warn("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := me.subjectManager.Verify(ctx, args.Msg.Header.AccessToken)
	if !ok {
		rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	if err := me.store.DeleteEntity(ctx, qdata.EntityId(req.Id)); err != nil {
		qlog.Warn("Could not delete entity: %v", err)
		rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_SUCCESS
	me.sendResponse(args, rsp)
}

func (me *writeWorker) handleSetEntitySchema(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiConfigSetEntitySchemaRequest)
	rsp := new(qprotobufs.ApiConfigSetEntitySchemaResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	if !me.isReady {
		qlog.Warn("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := me.subjectManager.Verify(ctx, args.Msg.Header.AccessToken)
	if !ok {
		rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	if err := me.store.SetEntitySchema(ctx, new(qdata.EntitySchema).FromEntitySchemaPb(req.Schema)); err != nil {
		qlog.Warn("Could not set entity schema: %v", err)
		rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_SUCCESS
	me.sendResponse(args, rsp)
}

func (me *writeWorker) handleRestoreSnapshot(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiConfigRestoreSnapshotRequest)
	rsp := new(qprotobufs.ApiConfigRestoreSnapshotResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	if !me.isReady {
		qlog.Warn("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := me.subjectManager.Verify(ctx, args.Msg.Header.AccessToken)
	if !ok {
		rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	if err := me.store.RestoreSnapshot(ctx, new(qdata.Snapshot).FromSnapshotPb(req.Snapshot)); err != nil {
		qlog.Warn("Could not restore snapshot: %v", err)
		rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_SUCCESS
	me.sendResponse(args, rsp)
}

func (me *writeWorker) handleDatabaseRequest(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeDatabaseRequest)
	rsp := new(qprotobufs.ApiRuntimeDatabaseResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	if !me.isReady {
		qlog.Warn("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	if req.RequestType != qprotobufs.ApiRuntimeDatabaseRequest_WRITE {
		qlog.Warn("Only WRITE requests are supported")
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	rsp.Response = req.Requests

	reqs := []*qdata.Request{}
	for _, r := range req.Requests {
		reqs = append(reqs, new(qdata.Request).FromRequestPb(r))
	}

	qlog.Info("Write request: %v", req.Requests)

	ctx := args.Ctx
	authCtx, ok := me.subjectManager.Verify(ctx, args.Msg.Header.AccessToken)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	// Perform write operation with the authorized context
	me.store.Write(ctx, reqs...)

	for i, req := range reqs {
		rsp.Response[i] = req.AsRequestPb()
	}

	rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_SUCCESS
	me.sendResponse(args, rsp)
}

func (me *writeWorker) sendResponse(args MessageReceivedArgs, response proto.Message) {
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
