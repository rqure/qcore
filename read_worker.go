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

type ReadWorker interface {
	qapp.Worker
	OnReady(context.Context)
	OnNotReady(context.Context)

	OnMessageReceived(args MessageReceivedArgs)
}

type readWorker struct {
	store   *qdata.Store
	isReady bool
	handle  qcontext.Handle
}

func NewReadWorker(store *qdata.Store) ReadWorker {
	return &readWorker{
		store: store,
	}
}

func (w *readWorker) Init(ctx context.Context) {
	w.handle = qcontext.GetHandle(ctx)
}

func (w *readWorker) Deinit(context.Context) {}
func (w *readWorker) DoWork(context.Context) {}

func (w *readWorker) OnMessageReceived(args MessageReceivedArgs) {
	switch {
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeGetEntityTypesRequest{}):
		w.handleGetEntityTypes(args)
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiConfigGetEntitySchemaRequest{}):
		w.handleGetEntitySchema(args)
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiConfigGetRootRequest{}):
		w.handleGetRoot(args)
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeFindEntitiesRequest{}):
		w.handleGetEntities(args)
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeFieldExistsRequest{}):
		w.handleFieldExists(args)
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeEntityExistsRequest{}):
		w.handleEntityExists(args)
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeGetDatabaseConnectionStatusRequest{}):
		w.handleGetDatabaseConnectionStatus(args)
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeDatabaseRequest{}):
		w.handleDatabaseRequest(args)
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeQueryRequest{}):
		w.handleQuery(args)
	default:
		qlog.Warn("Unknown message type: %v", args.Msg.Payload.TypeUrl)
	}
}

func (w *readWorker) handleGetEntityTypes(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeGetEntityTypesRequest)
	rsp := new(qprotobufs.ApiRuntimeGetEntityTypesResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeGetEntityTypesResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := verifyAuthentication(ctx, args.Msg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeGetEntityTypesResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	// Apply default page size if not specified
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}

	iter, err := w.store.GetEntityTypes(qdata.POPageSize(pageSize), qdata.POCursorId(req.Cursor))
	if err != nil {
		qlog.Warn("Error fetching entity types: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeGetEntityTypesResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	defer iter.Close()
	pageResult, err := iter.NextPage(ctx)
	if err != nil {
		qlog.Warn("Error fetching entity types: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeGetEntityTypesResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	rsp.EntityTypes = qdata.CastSlice(pageResult.Items, func(t qdata.EntityType) string { return t.AsString() })
	rsp.NextCursor = pageResult.CursorId

	rsp.Status = qprotobufs.ApiRuntimeGetEntityTypesResponse_SUCCESS
	w.sendResponse(args, rsp)
}

func (w *readWorker) handleGetEntitySchema(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiConfigGetEntitySchemaRequest)
	rsp := new(qprotobufs.ApiConfigGetEntitySchemaResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigGetEntitySchemaResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := verifyAuthentication(ctx, args.Msg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiConfigGetEntitySchemaResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	schema, err := w.store.GetEntitySchema(ctx, qdata.EntityType(req.Type))
	if err != nil {
		qlog.Warn("Could not get entity schema: %v", err)
		rsp.Status = qprotobufs.ApiConfigGetEntitySchemaResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	pbSchema := schema.AsEntitySchemaPb()
	rsp.Schema = pbSchema

	rsp.Status = qprotobufs.ApiConfigGetEntitySchemaResponse_SUCCESS
	w.sendResponse(args, rsp)
}

func (w *readWorker) handleGetRoot(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiConfigGetRootRequest)
	rsp := new(qprotobufs.ApiConfigGetRootResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		w.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := verifyAuthentication(ctx, args.Msg.Header.AccessToken, w.store)
	if !ok {
		w.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	iter, err := w.store.FindEntities("Root")
	if err != nil {
		qlog.Warn("Could not find root entity: %v", err)
		w.sendResponse(args, rsp)
		return
	}
	defer iter.Close()

	iter.ForEach(ctx, func(entityId qdata.EntityId) bool {
		rsp.RootId = entityId.AsString()
		return false // Break after first root
	})

	w.sendResponse(args, rsp)
}

func (w *readWorker) handleGetEntities(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeFindEntitiesRequest)
	rsp := new(qprotobufs.ApiRuntimeFindEntitiesResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeFindEntitiesResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := verifyAuthentication(ctx, args.Msg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeFindEntitiesResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	// Apply default page size if not specified
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100 // default page size
	}

	iter, err := w.store.FindEntities(qdata.EntityType(req.EntityType),
		qdata.POPageSize(pageSize),
		qdata.POCursorId(req.Cursor))
	if err != nil {
		qlog.Warn("Error fetching entities: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeFindEntitiesResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}
	defer iter.Close()

	pageResult, err := iter.NextPage(ctx)
	if err != nil {
		qlog.Warn("Error fetching entities: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeFindEntitiesResponse_FAILURE
		w.sendResponse(args, rsp)
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
	w.sendResponse(args, rsp)
}

func (w *readWorker) handleFieldExists(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeFieldExistsRequest)
	rsp := new(qprotobufs.ApiRuntimeFieldExistsResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeFieldExistsResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := verifyAuthentication(ctx, args.Msg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeFieldExistsResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	exists, err := w.store.FieldExists(
		ctx,
		qdata.EntityType(req.EntityType),
		qdata.FieldType(req.FieldName))
	if err != nil {
		qlog.Warn("Could not check field existence: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeFieldExistsResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	rsp.Exists = exists

	rsp.Status = qprotobufs.ApiRuntimeFieldExistsResponse_SUCCESS
	w.sendResponse(args, rsp)
}

func (w *readWorker) handleEntityExists(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeEntityExistsRequest)
	rsp := new(qprotobufs.ApiRuntimeEntityExistsResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeEntityExistsResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := verifyAuthentication(ctx, args.Msg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeEntityExistsResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	exists, err := w.store.EntityExists(ctx, qdata.EntityId(req.EntityId))
	if err != nil {
		qlog.Warn("Could not check entity existence: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeEntityExistsResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	rsp.Exists = exists

	rsp.Status = qprotobufs.ApiRuntimeEntityExistsResponse_SUCCESS
	w.sendResponse(args, rsp)
}

func (w *readWorker) handleGetDatabaseConnectionStatus(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeGetDatabaseConnectionStatusRequest)
	rsp := new(qprotobufs.ApiRuntimeGetDatabaseConnectionStatusResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeGetDatabaseConnectionStatusResponse_DISCONNECTED
		w.sendResponse(args, rsp)
		return
	}

	// Connection status can be checked without authentication
	if w.isReady {
		rsp.Status = qprotobufs.ApiRuntimeGetDatabaseConnectionStatusResponse_CONNECTED
	} else {
		rsp.Status = qprotobufs.ApiRuntimeGetDatabaseConnectionStatusResponse_DISCONNECTED
	}
	w.sendResponse(args, rsp)
}

func (w *readWorker) handleDatabaseRequest(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeDatabaseRequest)
	rsp := new(qprotobufs.ApiRuntimeDatabaseResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	// Only handle READ requests
	if req.RequestType != qprotobufs.ApiRuntimeDatabaseRequest_READ {
		qlog.Warn("Only READ requests are supported")
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	rsp.Response = req.Requests

	reqs := []*qdata.Request{}
	for _, r := range req.Requests {
		reqs = append(reqs, new(qdata.Request).FromRequestPb(r))
	}

	qlog.Info("Read request: %v", req.Requests)

	ctx := args.Ctx
	authCtx, ok := verifyAuthentication(ctx, args.Msg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	w.store.Read(ctx, reqs...)

	for i, req := range reqs {
		rsp.Response[i] = req.AsRequestPb()
	}

	rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_SUCCESS
	w.sendResponse(args, rsp)
}

func (w *readWorker) handleQuery(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeQueryRequest)
	rsp := new(qprotobufs.ApiRuntimeQueryResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeQueryResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	ctx := args.Ctx
	authCtx, ok := verifyAuthentication(ctx, args.Msg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeQueryResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}
	ctx = authCtx

	if !w.isReady {
		qlog.Warn("Could not handle query request. Database is not connected.")
		rsp.Status = qprotobufs.ApiRuntimeQueryResponse_FAILURE
		w.sendResponse(args, rsp)
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
		req.Query = string(qdata.QEExprLang)
	}

	opts = append(opts, qdata.POPageSize(pageSize))
	opts = append(opts, qdata.POCursorId(req.Cursor))
	opts = append(opts, qdata.QueryEngineType(req.Engine))

	// Prepare and execute the query with pagination
	iter, err := w.store.PrepareQuery(
		req.Query,
		opts...,
	)
	if err != nil {
		qlog.Warn("Error preparing query: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeQueryResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}
	defer iter.Close()

	pageResult, err := iter.NextPage(ctx)
	if err != nil {
		qlog.Warn("Error executing query: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeQueryResponse_FAILURE
		w.sendResponse(args, rsp)
		return
	}

	// Convert the results to protobuf format
	rsp.Rows = make([]*qprotobufs.QueryRow, 0, len(pageResult.Items))
	for _, row := range pageResult.Items {
		rsp.Rows = append(rsp.Rows, row.AsQueryRowPb())
	}

	rsp.NextCursor = pageResult.CursorId

	rsp.Status = qprotobufs.ApiRuntimeQueryResponse_SUCCESS
	w.sendResponse(args, rsp)
}

func (w *readWorker) sendResponse(args MessageReceivedArgs, response proto.Message) {
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

func (w *readWorker) OnReady(ctx context.Context) {
	w.isReady = true
}

func (w *readWorker) OnNotReady(context.Context) {
	w.isReady = false
}
