package main

import (
	"context"
	"unicode"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/data/query"
	"github.com/rqure/qlib/pkg/data/snapshot"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	web "github.com/rqure/qlib/pkg/web/go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ConfigWorker struct {
	store            data.Store
	modeManager      ModeManager
	isStoreConnected bool
}

func NewConfigWorker(store data.Store, modeManager ModeManager) *ConfigWorker {
	return &ConfigWorker{
		store:            store,
		isStoreConnected: false,
		modeManager:      modeManager,
	}
}

func (w *ConfigWorker) Init(context.Context, app.Handle) {

}

func (w *ConfigWorker) Deinit(context.Context) {

}

func (w *ConfigWorker) DoWork(context.Context) {

}

func (w *ConfigWorker) TriggerSchemaUpdate(ctx context.Context) {
	roots := query.New(w.store).
		Select().
		From("Root").
		Execute(ctx)

	for _, root := range roots {
		root.GetField("SchemaUpdateTrigger").WriteInt(ctx)
	}
}

func (w *ConfigWorker) OnNewClientMessage(ctx context.Context, args ...interface{}) {
	client := args[0].(web.Client)
	msg := args[1].(web.Message)

	if msg.Payload.MessageIs(&protobufs.ApiConfigCreateEntityRequest{}) {
		if w.modeManager.HasModes(ModeWrite) {
			w.onConfigCreateEntityRequest(ctx, client, msg)
		}
	} else if msg.Payload.MessageIs(&protobufs.ApiConfigDeleteEntityRequest{}) {
		if w.modeManager.HasModes(ModeWrite) {
			w.onConfigDeleteEntityRequest(ctx, client, msg)
		}
	} else if msg.Payload.MessageIs(&protobufs.ApiConfigGetEntityRequest{}) {
		if w.modeManager.HasModes(ModeRead) {
			w.onConfigGetEntityRequest(ctx, client, msg)
		}
	} else if msg.Payload.MessageIs(&protobufs.ApiConfigGetEntityTypesRequest{}) {
		if w.modeManager.HasModes(ModeRead) {
			w.onConfigGetEntityTypesRequest(ctx, client, msg)
		}
	} else if msg.Payload.MessageIs(&protobufs.ApiConfigGetEntitySchemaRequest{}) {
		if w.modeManager.HasModes(ModeRead) {
			w.onConfigGetEntitySchemaRequest(ctx, client, msg)
		}
	} else if msg.Payload.MessageIs(&protobufs.ApiConfigSetEntitySchemaRequest{}) {
		if w.modeManager.HasModes(ModeWrite) {
			w.onConfigSetEntitySchemaRequest(ctx, client, msg)
		}
	} else if msg.Payload.MessageIs(&protobufs.ApiConfigCreateSnapshotRequest{}) {
		if w.modeManager.HasModes(ModeRead) {
			w.onConfigCreateSnapshotRequest(ctx, client, msg)
		}
	} else if msg.Payload.MessageIs(&protobufs.ApiConfigRestoreSnapshotRequest{}) {
		if w.modeManager.HasModes(ModeWrite) {
			w.onConfigRestoreSnapshotRequest(ctx, client, msg)
		}
	} else if msg.Payload.MessageIs(&protobufs.ApiConfigGetRootRequest{}) {
		if w.modeManager.HasModes(ModeRead) {
			w.onConfigGetRootRequest(ctx, client, msg)
		}
	}
}

func (w *ConfigWorker) onConfigCreateEntityRequest(ctx context.Context, client web.Client, msg web.Message) {
	req := new(protobufs.ApiConfigCreateEntityRequest)
	rsp := new(protobufs.ApiConfigCreateEntityResponse)

	if err := msg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = protobufs.ApiConfigCreateEntityResponse_FAILURE
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	log.Info("Created entity: %v", req)
	w.store.CreateEntity(ctx, req.Type, req.ParentId, req.Name)

	rsp.Status = protobufs.ApiConfigCreateEntityResponse_SUCCESS
	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(rsp); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
	w.TriggerSchemaUpdate(ctx)
}

func (w *ConfigWorker) onConfigDeleteEntityRequest(ctx context.Context, client web.Client, msg web.Message) {
	req := new(protobufs.ApiConfigDeleteEntityRequest)
	rsp := new(protobufs.ApiConfigDeleteEntityResponse)

	if err := msg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = protobufs.ApiConfigDeleteEntityResponse_FAILURE
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	log.Info("Deleted entity: %v", req)
	w.store.DeleteEntity(ctx, req.Id)

	rsp.Status = protobufs.ApiConfigDeleteEntityResponse_SUCCESS
	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(rsp); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
	w.TriggerSchemaUpdate(ctx)
}

func (w *ConfigWorker) onConfigGetEntityRequest(ctx context.Context, client web.Client, msg web.Message) {
	req := new(protobufs.ApiConfigGetEntityRequest)
	rsp := new(protobufs.ApiConfigGetEntityResponse)

	if err := msg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = protobufs.ApiConfigGetEntityResponse_FAILURE
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	ent := w.store.GetEntity(ctx, req.Id)
	if ent == nil {
		log.Error("Could not get entity")
		rsp.Status = protobufs.ApiConfigGetEntityResponse_FAILURE
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	rsp.Entity = entity.ToEntityPb(ent)
	rsp.Status = protobufs.ApiConfigGetEntityResponse_SUCCESS
	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(rsp); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
}

func (w *ConfigWorker) onConfigGetEntityTypesRequest(ctx context.Context, client web.Client, msg web.Message) {
	request := new(protobufs.ApiConfigGetEntityTypesRequest)
	response := new(protobufs.ApiConfigGetEntityTypesResponse)

	if err := msg.Payload.UnmarshalTo(request); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	types := w.store.GetEntityTypes(ctx)

	response.Types = types
	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(response); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
}

func (w *ConfigWorker) onConfigGetEntitySchemaRequest(ctx context.Context, client web.Client, msg web.Message) {
	req := new(protobufs.ApiConfigGetEntitySchemaRequest)
	rsp := new(protobufs.ApiConfigGetEntitySchemaResponse)

	if err := msg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = protobufs.ApiConfigGetEntitySchemaResponse_FAILURE
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	sch := w.store.GetEntitySchema(ctx, req.Type)
	if sch == nil {
		log.Error("Could not get entity schema")
		rsp.Status = protobufs.ApiConfigGetEntitySchemaResponse_FAILURE
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	rsp.Schema = entity.ToSchemaPb(sch)
	rsp.Status = protobufs.ApiConfigGetEntitySchemaResponse_SUCCESS
	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(rsp); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
}

func (w *ConfigWorker) onConfigSetEntitySchemaRequest(ctx context.Context, client web.Client, msg web.Message) {
	req := new(protobufs.ApiConfigSetEntitySchemaRequest)
	rsp := new(protobufs.ApiConfigSetEntitySchemaResponse)

	if err := msg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = protobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	// Check if entityType is alphanumeric
	isAlphanumeric := func(entityType string) bool {
		for _, char := range entityType {
			if !unicode.IsLetter(char) && !unicode.IsNumber(char) {
				return false
			}
		}
		return true
	}

	if !isAlphanumeric(req.Schema.Name) {
		log.Error("Could not handle request %v. Entity type is not alphanumeric.", req)
		rsp.Status = protobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	log.Info("Set entity schema: %v", req)
	sch := entity.FromSchemaPb(req.Schema)
	w.store.SetEntitySchema(ctx, sch)

	rsp.Status = protobufs.ApiConfigSetEntitySchemaResponse_SUCCESS
	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(rsp); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}
	client.Write(msg)
	w.TriggerSchemaUpdate(ctx)
}

func (w *ConfigWorker) onConfigCreateSnapshotRequest(ctx context.Context, client web.Client, msg web.Message) {
	req := new(protobufs.ApiConfigCreateSnapshotRequest)
	rsp := new(protobufs.ApiConfigCreateSnapshotResponse)

	if err := msg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = protobufs.ApiConfigCreateSnapshotResponse_FAILURE
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	log.Info("Created snapshot: %v", req)
	ss := w.store.CreateSnapshot(ctx)

	rsp.Snapshot = snapshot.ToPb(ss)
	rsp.Status = protobufs.ApiConfigCreateSnapshotResponse_SUCCESS
	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(rsp); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
}

func (w *ConfigWorker) onConfigRestoreSnapshotRequest(ctx context.Context, client web.Client, msg web.Message) {
	req := new(protobufs.ApiConfigRestoreSnapshotRequest)
	rsp := new(protobufs.ApiConfigRestoreSnapshotResponse)

	if err := msg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = protobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	log.Info("Restored snapshot: %v", req)
	w.store.RestoreSnapshot(ctx, snapshot.FromPb(req.Snapshot))

	rsp.Status = protobufs.ApiConfigRestoreSnapshotResponse_SUCCESS
	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(rsp); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
	w.TriggerSchemaUpdate(ctx)
}

func (w *ConfigWorker) onConfigGetRootRequest(ctx context.Context, client web.Client, msg web.Message) {
	request := new(protobufs.ApiConfigGetRootRequest)
	response := new(protobufs.ApiConfigGetRootResponse)

	if err := msg.Payload.UnmarshalTo(request); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", request)
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(response); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	root := w.store.FindEntities(ctx, "Root")

	for _, id := range root {
		response.RootId = id
	}
	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(response); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
}

func (w *ConfigWorker) OnStoreConnected(context.Context) {
	w.isStoreConnected = true
}

func (w *ConfigWorker) OnStoreDisconnected() {
	w.isStoreConnected = true
}
