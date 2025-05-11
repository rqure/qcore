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

type ReadWorker interface {
	qapp.Worker
	OnReady(context.Context)
	OnNotReady(context.Context)

	OnMessageReceived(args MessageReceivedArgs)
}

type readWorker struct {
	store          *qdata.Store
	isReady        bool
	handle         qcontext.Handle
	subjectManager SubjectManager
}

func NewReadWorker(store *qdata.Store, subjectManager SubjectManager) ReadWorker {
	return &readWorker{
		store:          store,
		subjectManager: subjectManager,
	}
}

func (me *readWorker) Init(ctx context.Context) {
	me.handle = qcontext.GetHandle(ctx)
}

func (me *readWorker) Deinit(context.Context) {}
func (me *readWorker) DoWork(context.Context) {}

func (me *readWorker) OnMessageReceived(args MessageReceivedArgs) {
	startTime := time.Now()
	logTime := func() {
		qlog.Trace("ReadWorker: %s took %v", args.Msg.Payload.TypeUrl, time.Since(startTime))
	}

	switch {
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeGetEntityTypesRequest{}):
		me.handleGetEntityTypes(args)
		defer logTime()
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiConfigGetEntitySchemaRequest{}):
		me.handleGetEntitySchema(args)
		defer logTime()
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiConfigGetRootRequest{}):
		me.handleGetRoot(args)
		defer logTime()
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeFindEntitiesRequest{}):
		me.handleGetEntities(args)
		defer logTime()
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeFieldExistsRequest{}):
		me.handleFieldExists(args)
		defer logTime()
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeEntityExistsRequest{}):
		me.handleEntityExists(args)
		defer logTime()
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeGetDatabaseConnectionStatusRequest{}):
		me.handleGetDatabaseConnectionStatus(args)
		defer logTime()
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeDatabaseRequest{}):
		me.handleDatabaseRequest(args)
		defer logTime()
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeQueryRequest{}):
		me.handleQuery(args)
		defer logTime()
	default:
	}
}

func (me *readWorker) handleGetEntityTypes(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeGetEntityTypesRequest)
	rsp := new(qprotobufs.ApiRuntimeGetEntityTypesResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeGetEntityTypesResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := me.subjectManager.Verify(ctx, args.Msg.Header.AccessToken)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeGetEntityTypesResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	// Apply default page size if not specified
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}

	iter, err := me.store.GetEntityTypes(qdata.POPageSize(pageSize), qdata.POCursorId(req.Cursor))
	if err != nil {
		qlog.Warn("Error fetching entity types: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeGetEntityTypesResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	defer iter.Close()
	pageResult, err := iter.NextPage(ctx)
	if err != nil {
		qlog.Warn("Error fetching entity types: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeGetEntityTypesResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	rsp.EntityTypes = qdata.CastSlice(pageResult.Items, func(t qdata.EntityType) string { return t.AsString() })
	rsp.NextCursor = pageResult.CursorId

	rsp.Status = qprotobufs.ApiRuntimeGetEntityTypesResponse_SUCCESS
	me.sendResponse(args, rsp)
}

func (me *readWorker) handleGetEntitySchema(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiConfigGetEntitySchemaRequest)
	rsp := new(qprotobufs.ApiConfigGetEntitySchemaResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigGetEntitySchemaResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := me.subjectManager.Verify(ctx, args.Msg.Header.AccessToken)
	if !ok {
		rsp.Status = qprotobufs.ApiConfigGetEntitySchemaResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	schema, err := me.store.GetEntitySchema(ctx, qdata.EntityType(req.Type))
	if err != nil {
		qlog.Warn("Could not get entity schema: %v", err)
		rsp.Status = qprotobufs.ApiConfigGetEntitySchemaResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	pbSchema := schema.AsEntitySchemaPb()
	rsp.Schema = pbSchema

	rsp.Status = qprotobufs.ApiConfigGetEntitySchemaResponse_SUCCESS
	me.sendResponse(args, rsp)
}

func (me *readWorker) handleGetRoot(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiConfigGetRootRequest)
	rsp := new(qprotobufs.ApiConfigGetRootResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		me.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := me.subjectManager.Verify(ctx, args.Msg.Header.AccessToken)
	if !ok {
		me.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	iter, err := me.store.FindEntities("Root")
	if err != nil {
		qlog.Warn("Could not find root entity: %v", err)
		me.sendResponse(args, rsp)
		return
	}
	defer iter.Close()

	iter.ForEach(ctx, func(entityId qdata.EntityId) bool {
		rsp.RootId = entityId.AsString()
		return false // Break after first root
	})

	me.sendResponse(args, rsp)
}

func (me *readWorker) handleGetEntities(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeFindEntitiesRequest)
	rsp := new(qprotobufs.ApiRuntimeFindEntitiesResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeFindEntitiesResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := me.subjectManager.Verify(ctx, args.Msg.Header.AccessToken)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeFindEntitiesResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	// Apply default page size if not specified
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100 // default page size
	}

	iter, err := me.store.FindEntities(qdata.EntityType(req.EntityType),
		qdata.POPageSize(pageSize),
		qdata.POCursorId(req.Cursor))
	if err != nil {
		qlog.Warn("Error fetching entities: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeFindEntitiesResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}
	defer iter.Close()

	pageResult, err := iter.NextPage(ctx)
	if err != nil {
		qlog.Warn("Error fetching entities: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeFindEntitiesResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	// Convert EntityId array to string array
	entityIds := make([]string, 0, len(pageResult.Items))
	for _, entityId := range pageResult.Items {
		entityIds = append(entityIds, string(entityId))
	}

	rsp.NextCursor = pageResult.CursorId
	rsp.Entities = entityIds

	rsp.Status = qprotobufs.ApiRuntimeFindEntitiesResponse_SUCCESS
	me.sendResponse(args, rsp)
}

func (me *readWorker) handleFieldExists(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeFieldExistsRequest)
	rsp := new(qprotobufs.ApiRuntimeFieldExistsResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeFieldExistsResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := me.subjectManager.Verify(ctx, args.Msg.Header.AccessToken)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeFieldExistsResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	exists, err := me.store.FieldExists(
		ctx,
		qdata.EntityType(req.EntityType),
		qdata.FieldType(req.FieldName))
	if err != nil {
		qlog.Warn("Could not check field existence: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeFieldExistsResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	rsp.Exists = exists

	rsp.Status = qprotobufs.ApiRuntimeFieldExistsResponse_SUCCESS
	me.sendResponse(args, rsp)
}

func (me *readWorker) handleEntityExists(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeEntityExistsRequest)
	rsp := new(qprotobufs.ApiRuntimeEntityExistsResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeEntityExistsResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := me.subjectManager.Verify(ctx, args.Msg.Header.AccessToken)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeEntityExistsResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	exists, err := me.store.EntityExists(ctx, qdata.EntityId(req.EntityId))
	if err != nil {
		qlog.Warn("Could not check entity existence: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeEntityExistsResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	rsp.Exists = exists

	rsp.Status = qprotobufs.ApiRuntimeEntityExistsResponse_SUCCESS
	me.sendResponse(args, rsp)
}

func (me *readWorker) handleGetDatabaseConnectionStatus(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeGetDatabaseConnectionStatusRequest)
	rsp := new(qprotobufs.ApiRuntimeGetDatabaseConnectionStatusResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeGetDatabaseConnectionStatusResponse_DISCONNECTED
		me.sendResponse(args, rsp)
		return
	}

	// Connection status can be checked without authentication
	if me.isReady {
		rsp.Status = qprotobufs.ApiRuntimeGetDatabaseConnectionStatusResponse_CONNECTED
	} else {
		rsp.Status = qprotobufs.ApiRuntimeGetDatabaseConnectionStatusResponse_DISCONNECTED
	}
	me.sendResponse(args, rsp)
}

func (me *readWorker) handleDatabaseRequest(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeDatabaseRequest)
	rsp := new(qprotobufs.ApiRuntimeDatabaseResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	// Only handle READ requests
	if req.RequestType != qprotobufs.ApiRuntimeDatabaseRequest_READ {
		qlog.Warn("Only READ requests are supported")
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	rsp.Response = req.Requests

	reqs := []*qdata.Request{}
	for _, r := range req.Requests {
		reqs = append(reqs, new(qdata.Request).FromRequestPb(r))
	}

	qlog.Info("Read request: %v", req.Requests)

	ctx := args.Ctx
	authCtx, ok := me.subjectManager.Verify(ctx, args.Msg.Header.AccessToken)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	me.store.Read(ctx, reqs...)

	for i, req := range reqs {
		rsp.Response[i] = req.AsRequestPb()
	}

	rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_SUCCESS
	me.sendResponse(args, rsp)
}

func (me *readWorker) handleQuery(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeQueryRequest)
	rsp := new(qprotobufs.ApiRuntimeQueryResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeQueryResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := me.subjectManager.Verify(ctx, args.Msg.Header.AccessToken)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeQueryResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	if !me.isReady {
		qlog.Warn("Could not handle query request. Database is not connected.")
		rsp.Status = qprotobufs.ApiRuntimeQueryResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	// Apply default page size if not specified
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100 // default page size
	}

	// Convert type hints to qdata.TypeHintOpts
	opts := make([]interface{}, 0)
	for _, hint := range req.TypeHints {
		opts = append(opts,
			qdata.TypeHint(
				hint.FieldType,
				qdata.ValueType(hint.ValueType),
			),
		)
	}

	if req.Engine == "" {
		req.Query = string(qdata.QESqlite)
	}

	opts = append(opts, qdata.POPageSize(pageSize))
	opts = append(opts, qdata.POCursorId(req.Cursor))
	opts = append(opts, qdata.QueryEngineType(req.Engine))

	// Prepare and execute the query with pagination
	iter, err := me.store.PrepareQuery(
		req.Query,
		opts...,
	)
	if err != nil {
		qlog.Warn("Error preparing query: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeQueryResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}
	defer iter.Close()

	pageResult, err := iter.NextPage(ctx)
	if err != nil {
		qlog.Warn("Error executing query: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeQueryResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	// Convert the results to protobuf format
	rsp.Rows = make([]*qprotobufs.QueryRow, 0, len(pageResult.Items))
	for _, row := range pageResult.Items {
		rsp.Rows = append(rsp.Rows, row.AsQueryRowPb())
	}

	rsp.NextCursor = pageResult.CursorId

	rsp.Status = qprotobufs.ApiRuntimeQueryResponse_SUCCESS
	me.sendResponse(args, rsp)
}

func (me *readWorker) sendResponse(args MessageReceivedArgs, response proto.Message) {
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

func (me *readWorker) OnReady(ctx context.Context) {
	me.isReady = true
}

func (me *readWorker) OnNotReady(context.Context) {
	me.isReady = false
}
