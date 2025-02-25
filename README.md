# qstore

## Database Backups and Restores

Example of taking a database backup:

```
curl localhost:20000/api -d '{"header":{"id":"ea9e4bfb-a208-419e-88cc-8dc694b287ef","timestamp":"2024-07-04T22:37:18.544393318Z"},"payload":{"@type":"type.googleapis.com/qdb.WebConfigCreateSnapshotRequest"}}' | jq '.payload |= (del(.status) | .["@type"] = "type.googleapis.com/qdb.WebConfigRestoreSnapshotRequest")' > snapshot.json
```

Example of restoring a database backup:

```
curl localhost:20000/api -d @tmp/snapshot.json
```

## API

### Create Entity

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigCreateEntityRequest",
    "type": "entityType",
    "name": "entityName",
    "parentId": "parentEntityId"
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigCreateEntityResponse",
    "status": "SUCCESS",
    "id": "newEntityId"
  }
}
```

### Delete Entity

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigDeleteEntityRequest",
    "id": "entityId"
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigDeleteEntityResponse",
    "status": "SUCCESS"
  }
}
```

### Get Entity Types

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigGetEntityTypesRequest"
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigGetEntityTypesResponse",
    "types": ["type1", "type2", "type3"]
  }
}
```

### Get Entity

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigGetEntityRequest",
    "id": "entityId"
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigGetEntityResponse",
    "status": "SUCCESS",
    "entity": {
      "id": "entityId",
      "type": "entityType",
      "name": "entityName",
      "parent": {"raw": "parentEntityId"},
      "children": [{"raw": "childEntityId1"}, {"raw": "childEntityId2"}]
    }
  }
}
```

### Get Field Schema

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigGetFieldSchemaRequest",
    "field": "fieldName"
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigGetFieldSchemaResponse",
    "status": "SUCCESS",
    "schema": {
      "name": "fieldName",
      "type": "fieldType"
    }
  }
}
```

### Set Field Schema

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigSetFieldSchemaRequest",
    "field": "fieldName",
    "schema": {
      "name": "fieldName",
      "type": "fieldType"
    }
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigSetFieldSchemaResponse",
    "status": "SUCCESS"
  }
}
```

### Get Entity Schema

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigGetEntitySchemaRequest",
    "type": "entityType"
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigGetEntitySchemaResponse",
    "status": "SUCCESS",
    "schema": {
      "name": "entityType",
      "fields": ["field1", "field2", "field3"]
    }
  }
}
```

### Set Entity Schema

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigSetEntitySchemaRequest",
    "name": "entityType",
    "fields": ["field1", "field2", "field3"]
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigSetEntitySchemaResponse",
    "status": "SUCCESS"
  }
}
```

### Create Snapshot

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigCreateSnapshotRequest"
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigCreateSnapshotResponse",
    "status": "SUCCESS",
    "snapshot": {
      "entities": [
        {
          "id": "entityId",
          "type": "entityType",
          "name": "entityName",
          "parent": {"raw": "parentEntityId"},
          "children": [{"raw": "childEntityId1"}, {"raw": "childEntityId2"}]
        }
      ],
      "fields": [
        {
          "id": "fieldId",
          "name": "fieldName",
          "value": {},
          "writeTime": "timestamp",
          "writerId": "writerId"
        }
      ],
      "entitySchemas": [
        {
          "name": "entityType",
          "fields": ["field1", "field2", "field3"]
        }
      ],
      "fieldSchemas": [
        {
          "name": "fieldName",
          "type": "fieldType"
        }
      ]
    }
  }
}
```

### Restore Snapshot

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigRestoreSnapshotRequest",
    "snapshot": {
      "entities": [
        {
          "id": "entityId",
          "type": "entityType",
          "name": "entityName",
          "parent": {"raw": "parentEntityId"},
          "children": [{"raw": "childEntityId1"}, {"raw": "childEntityId2"}]
        }
      ],
      "fields": [
        {
          "id": "fieldId",
          "name": "fieldName",
          "value": {},
          "writeTime": "timestamp",
          "writerId": "writerId"
        }
      ],
      "entitySchemas": [
        {
          "name": "entityType",
          "fields": ["field1", "field2", "field3"]
        }
      ],
      "fieldSchemas": [
        {
          "name": "fieldName",
          "type": "fieldType"
        }
      ]
    }
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigRestoreSnapshotResponse",
    "status": "SUCCESS"
  }
}
```

### Get Root Entity

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigGetRootRequest"
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigGetRootResponse",
    "rootId": "rootEntityId"
  }
}
```

### Get All Fields

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigGetAllFieldsRequest"
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebConfigGetAllFieldsResponse",
    "fields": ["field1", "field2", "field3"]
  }
}
```

### Runtime Database Request

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebRuntimeDatabaseRequest",
    "requestType": "READ",
    "requests": [
      {
        "id": "requestId",
        "field": "fieldName",
        "value": {},
        "writeTime": "timestamp",
        "writerId": "writerId",
        "success": true
      }
    ]
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebRuntimeDatabaseResponse",
    "response": [
      {
        "id": "requestId",
        "field": "fieldName",
        "value": {},
        "writeTime": "timestamp",
        "writerId": "writerId",
        "success": true
      }
    ]
  }
}
```

### Register Notification

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebRuntimeRegisterNotificationRequest",
    "requests": [
      {
        "id": "notificationId",
        "type": "notificationType",
        "field": "fieldName",
        "contextFields": ["contextField1", "contextField2"],
        "notifyOnChange": true
      }
    ]
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebRuntimeRegisterNotificationResponse",
    "tokens": ["notificationToken1", "notificationToken2"]
  }
}
```

### Get Notifications

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebRuntimeGetNotificationsRequest"
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebRuntimeGetNotificationsResponse",
    "notifications": [
      {
        "token": "notificationToken",
        "current": {
          "id": "fieldId",
          "name": "fieldName",
          "value": {},
          "writeTime": "timestamp",
          "writerId": "writerId"
        },
        "previous": {
          "id": "fieldId",
          "name": "fieldName",
          "value": {},
          "writeTime": "timestamp",
          "writerId": "writerId"
        },
        "context": [
          {
            "id": "fieldId",
            "name": "fieldName",
            "value": {},
            "writeTime": "timestamp",
            "writerId": "writerId"
          }
        ]
      }
    ]
  }
}
```

### Unregister Notification

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebRuntimeUnregisterNotificationRequest",
    "tokens": ["notificationToken1", "notificationToken2"]
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebRuntimeUnregisterNotificationResponse",
    "status": "SUCCESS"
  }
}
```

### Get Database Connection Status

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebRuntimeGetDatabaseConnectionStatusRequest"
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebRuntimeGetDatabaseConnectionStatusResponse",
    "status": "CONNECTED"
  }
}
```

### Get Entities

Method: POST

Request:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebRuntimeGetEntitiesRequest",
    "entityType": "entityType"
  }
}
```

Response:

```json
{
  "header": {
    "id": "unique-id",
    "timestamp": "timestamp"
  },
  "payload": {
    "@type": "type.googleapis.com/qdb.WebRuntimeGetEntitiesResponse",
    "entities": [
      {
        "id": "entityId",
        "type": "entityType",
        "name": "entityName",
        "parent": {"raw": "parentEntityId"},
        "children": [{"raw": "childEntityId1"}, {"raw": "childEntityId2"}]
      }
    ]
  }
}
```
