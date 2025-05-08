package main

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
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

func (w *readWorker) handleReadRequest(msg *nats.Msg) {
	startTime := time.Now()
	defer func() {
		qlog.Trace("Took %s to process", time.Since(startTime))
	}()

	var apiMsg qprotobufs.ApiMessage
	if err := proto.Unmarshal(msg.Data, &apiMsg); err != nil {
		qlog.Warn("Could not unmarshal message: %v", err)
		return
	}

	switch {
	case apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeGetEntityTypesRequest{}):
		w.handleGetEntityTypes(ctx, msg, &apiMsg)
	case apiMsg.Payload.MessageIs(&qprotobufs.ApiConfigGetEntitySchemaRequest{}):
		w.handleGetEntitySchema(ctx, msg, &apiMsg)
	case apiMsg.Payload.MessageIs(&qprotobufs.ApiConfigGetRootRequest{}):
		w.handleGetRoot(ctx, msg, &apiMsg)
	case apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeFindEntitiesRequest{}):
		w.handleGetEntities(ctx, msg, &apiMsg)
	case apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeFieldExistsRequest{}):
		w.handleFieldExists(ctx, msg, &apiMsg)
	case apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeEntityExistsRequest{}):
		w.handleEntityExists(ctx, msg, &apiMsg)
	case apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeGetDatabaseConnectionStatusRequest{}):
		w.handleGetDatabaseConnectionStatus(ctx, msg, &apiMsg)
	case apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeDatabaseRequest{}):
		w.handleDatabaseRequest(ctx, msg, &apiMsg)
	case apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeQueryRequest{}):
		w.handleQuery(ctx, msg, &apiMsg)
	default:
		qlog.Warn("Unknown message type: %v", apiMsg.Payload.TypeUrl)
	}

	w.sendResponse(msg, response)
}

func (w *readWorker) handleGetEntityTypes(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeGetEntityTypesRequest)
	rsp := new(qprotobufs.ApiRuntimeGetEntityTypesResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeGetEntityTypesResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	authCtx, ok := verifyAuthentication(ctx, apiMsg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeGetEntityTypesResponse_FAILURE
		w.sendResponse(msg, rsp)
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
		w.sendResponse(msg, rsp)
		return
	}

	defer iter.Close()
	pageResult, err := iter.NextPage(ctx)
	if err != nil {
		qlog.Warn("Error fetching entity types: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeGetEntityTypesResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	rsp.EntityTypes = qdata.CastSlice(pageResult.Items, func(t qdata.EntityType) string { return t.AsString() })
	rsp.NextCursor = pageResult.CursorId

	rsp.Status = qprotobufs.ApiRuntimeGetEntityTypesResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleGetEntitySchema(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiConfigGetEntitySchemaRequest)
	rsp := new(qprotobufs.ApiConfigGetEntitySchemaResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigGetEntitySchemaResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	authCtx, ok := verifyAuthentication(ctx, apiMsg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiConfigGetEntitySchemaResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}
	ctx = authCtx

	schema, err := w.store.GetEntitySchema(ctx, qdata.EntityType(req.Type))
	if err != nil {
		qlog.Warn("Could not get entity schema: %v", err)
		rsp.Status = qprotobufs.ApiConfigGetEntitySchemaResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	pbSchema := schema.AsEntitySchemaPb()
	rsp.Schema = pbSchema

	rsp.Status = qprotobufs.ApiConfigGetEntitySchemaResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleGetRoot(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiConfigGetRootRequest)
	rsp := new(qprotobufs.ApiConfigGetRootResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	authCtx, ok := verifyAuthentication(ctx, apiMsg.Header.AccessToken, w.store)
	if !ok {
		w.sendResponse(msg, rsp)
		return
	}
	ctx = authCtx

	iter, err := w.store.FindEntities("Root")
	if err != nil {
		qlog.Warn("Could not find root entity: %v", err)
		w.sendResponse(msg, rsp)
		return
	}
	defer iter.Close()

	iter.ForEach(ctx, func(entityId qdata.EntityId) bool {
		rsp.RootId = entityId.AsString()
		return false // Break after first root
	})

	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleGetEntities(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeFindEntitiesRequest)
	rsp := new(qprotobufs.ApiRuntimeFindEntitiesResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeFindEntitiesResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	authCtx, ok := verifyAuthentication(ctx, apiMsg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeFindEntitiesResponse_FAILURE
		w.sendResponse(msg, rsp)
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
		w.sendResponse(msg, rsp)
		return
	}
	defer iter.Close()

	pageResult, err := iter.NextPage(ctx)
	if err != nil {
		qlog.Warn("Error fetching entities: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeFindEntitiesResponse_FAILURE
		w.sendResponse(msg, rsp)
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
	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleFieldExists(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeFieldExistsRequest)
	rsp := new(qprotobufs.ApiRuntimeFieldExistsResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeFieldExistsResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	authCtx, ok := verifyAuthentication(ctx, apiMsg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeFieldExistsResponse_FAILURE
		w.sendResponse(msg, rsp)
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
		w.sendResponse(msg, rsp)
		return
	}

	rsp.Exists = exists

	rsp.Status = qprotobufs.ApiRuntimeFieldExistsResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleEntityExists(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeEntityExistsRequest)
	rsp := new(qprotobufs.ApiRuntimeEntityExistsResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeEntityExistsResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	authCtx, ok := verifyAuthentication(ctx, apiMsg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeEntityExistsResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}
	ctx = authCtx

	exists, err := w.store.EntityExists(ctx, qdata.EntityId(req.EntityId))
	if err != nil {
		qlog.Warn("Could not check entity existence: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeEntityExistsResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	rsp.Exists = exists

	rsp.Status = qprotobufs.ApiRuntimeEntityExistsResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleGetDatabaseConnectionStatus(_ context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeGetDatabaseConnectionStatusRequest)
	rsp := new(qprotobufs.ApiRuntimeGetDatabaseConnectionStatusResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeGetDatabaseConnectionStatusResponse_DISCONNECTED
		w.sendResponse(msg, rsp)
		return
	}

	// Connection status can be checked without authentication
	if w.isReady {
		rsp.Status = qprotobufs.ApiRuntimeGetDatabaseConnectionStatusResponse_CONNECTED
	} else {
		rsp.Status = qprotobufs.ApiRuntimeGetDatabaseConnectionStatusResponse_DISCONNECTED
	}
	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleDatabaseRequest(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeDatabaseRequest)
	rsp := new(qprotobufs.ApiRuntimeDatabaseResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	// Only handle READ requests
	if req.RequestType != qprotobufs.ApiRuntimeDatabaseRequest_READ {
		qlog.Warn("Only READ requests are supported")
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	rsp.Response = req.Requests

	reqs := []*qdata.Request{}
	for _, r := range req.Requests {
		reqs = append(reqs, new(qdata.Request).FromRequestPb(r))
	}

	qlog.Info("Read request: %v", req.Requests)
	authCtx, ok := verifyAuthentication(ctx, apiMsg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}
	ctx = authCtx

	w.store.Read(ctx, reqs...)

	for i, req := range reqs {
		rsp.Response[i] = req.AsRequestPb()
	}

	rsp.Status = qprotobufs.ApiRuntimeDatabaseResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *readWorker) handleQuery(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeQueryRequest)
	rsp := new(qprotobufs.ApiRuntimeQueryResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeQueryResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	authCtx, ok := verifyAuthentication(ctx, apiMsg.Header.AccessToken, w.store)
	if !ok {
		rsp.Status = qprotobufs.ApiRuntimeQueryResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}
	ctx = authCtx

	if !w.isReady {
		qlog.Warn("Could not handle query request. Database is not connected.")
		rsp.Status = qprotobufs.ApiRuntimeQueryResponse_FAILURE
		w.sendResponse(msg, rsp)
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
		w.sendResponse(msg, rsp)
		return
	}
	defer iter.Close()

	pageResult, err := iter.NextPage(ctx)
	if err != nil {
		qlog.Warn("Error executing query: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeQueryResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	// Convert the results to protobuf format
	rsp.Rows = make([]*qprotobufs.QueryRow, 0, len(pageResult.Items))
	for _, row := range pageResult.Items {
		rsp.Rows = append(rsp.Rows, row.AsQueryRowPb())
	}

	rsp.NextCursor = pageResult.CursorId

	rsp.Status = qprotobufs.ApiRuntimeQueryResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *readWorker) sendResponse(msg *nats.Msg, response proto.Message) {
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

func (w *readWorker) OnReady(ctx context.Context) {
	w.isReady = true
}

func (w *readWorker) OnNotReady(context.Context) {
	w.isReady = false
}
