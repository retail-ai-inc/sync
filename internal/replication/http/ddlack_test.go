package replicationhttp

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

const mysqlTaskJSON = `{"type":"mysql","taskName":"trial",
	"sourceConn":{"host":"h","port":3306,"user":"u","password":"p","database":"shop"},
	"targetConn":{"host":"h","port":3306,"user":"u","password":"p","database":"shop_bk"}}`

func acknowledge(t *testing.T, id, body string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/sync/"+id+"/ddl-acknowledgements",
		strings.NewReader(body))
	handler := SyncDDLAcknowledgeHandler(func(*http.Request) string { return "jack" })
	serveWithURLParams(rec, req, handler, map[string]string{"id": id})
	return rec
}

func TestAcknowledgingAStatementAndReadingItBack(t *testing.T) {
	db := useTempTaskDB(t)
	insertSyncTask(t, db, 1, mysqlTaskJSON)

	rec := acknowledge(t, "1", `{"statement":"ALTER TABLE orders DROP COLUMN email"}`)
	if resp := decodeEnvelope(t, rec); resp["success"] != true {
		t.Fatalf("acknowledging failed: %v", resp)
	}

	list := httptest.NewRecorder()
	serveWithURLParams(list, httptest.NewRequest(http.MethodGet, "/api/sync/1/ddl-acknowledgements", nil),
		SyncDDLAcknowledgementsHandler, map[string]string{"id": "1"})

	resp := decodeEnvelope(t, list)
	items, ok := resp["data"].([]interface{})
	if !ok || len(items) != 1 {
		t.Fatalf("data = %v, want one acknowledgement", resp["data"])
	}
	item := items[0].(map[string]interface{})
	if item["statement"] != "ALTER TABLE orders DROP COLUMN email" {
		t.Errorf("statement = %v", item["statement"])
	}
	if item["used"] != false {
		t.Errorf("used = %v, want it outstanding", item["used"])
	}
	if item["createdBy"] != "jack" {
		t.Errorf("createdBy = %v, want the caller", item["createdBy"])
	}
}

func TestAcknowledgingWithoutAStatementIsRefused(t *testing.T) {
	db := useTempTaskDB(t)
	insertSyncTask(t, db, 1, mysqlTaskJSON)

	if resp := decodeEnvelope(t, acknowledge(t, "1", `{}`)); resp["success"] == true {
		t.Fatalf("an acknowledgement naming no statement was accepted: %v", resp)
	}
}

func TestAcknowledgingUnreadableJSONIsRefused(t *testing.T) {
	db := useTempTaskDB(t)
	insertSyncTask(t, db, 1, mysqlTaskJSON)

	if resp := decodeEnvelope(t, acknowledge(t, "1", `not json`)); resp["success"] == true {
		t.Fatal("a body that is not JSON was accepted")
	}
}

func TestAcknowledgingAnUnknownTaskIsRefused(t *testing.T) {
	useTempTaskDB(t)

	resp := decodeEnvelope(t, acknowledge(t, "404", `{"statement":"DROP TABLE orders"}`))
	if resp["success"] == true {
		t.Fatalf("an acknowledgement was recorded for a task that does not exist: %v", resp)
	}
}

func TestWithdrawingAnAcknowledgementOverHTTP(t *testing.T) {
	db := useTempTaskDB(t)
	insertSyncTask(t, db, 1, mysqlTaskJSON)

	created := decodeEnvelope(t, acknowledge(t, "1", `{"statement":"DROP TABLE orders"}`))
	id := int64(created["data"].(map[string]interface{})["id"].(float64))

	rec := httptest.NewRecorder()
	serveWithURLParams(rec,
		httptest.NewRequest(http.MethodDelete, "/api/sync/1/ddl-acknowledgements/1", nil),
		SyncDDLAcknowledgementDeleteHandler,
		map[string]string{"id": "1", "ack": strconv.FormatInt(id, 10)})

	if resp := decodeEnvelope(t, rec); resp["success"] != true {
		t.Fatalf("withdrawing failed: %v", resp)
	}
}

func TestWithdrawingSomethingThatIsNotAnIdIsRefused(t *testing.T) {
	db := useTempTaskDB(t)
	insertSyncTask(t, db, 1, mysqlTaskJSON)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec,
		httptest.NewRequest(http.MethodDelete, "/api/sync/1/ddl-acknowledgements/latest", nil),
		SyncDDLAcknowledgementDeleteHandler,
		map[string]string{"id": "1", "ack": "latest"})

	if resp := decodeEnvelope(t, rec); resp["success"] == true {
		t.Fatal("a non-numeric acknowledgement id was accepted")
	}
}
