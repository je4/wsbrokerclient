package event_test

import (
	"encoding/json/v2"
	"testing"

	"github.com/je4/wsbrokerclient/pkg/event"
)

type customPayload struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
}

func TestEventCreationAndSerialization(t *testing.T) {
	// 1. NewMessageEvent with struct
	payload := customPayload{ID: 42, Title: "Test"}
	evt, err := event.NewMessageEvent("client-1", "channel-alerts", payload)
	if err != nil {
		t.Fatalf("failed to create message event: %v", err)
	}

	if evt.GetType() != event.TypeMessage {
		t.Errorf("expected type %s, got %s", event.TypeMessage, evt.GetType())
	}
	if evt.GetSource() != "client-1" {
		t.Errorf("expected source client-1, got %s", evt.GetSource())
	}
	if evt.GetTarget() != "channel-alerts" {
		t.Errorf("expected target channel-alerts, got %s", evt.GetTarget())
	}

	var decoded customPayload
	if err := evt.UnmarshalData(&decoded); err != nil {
		t.Fatalf("failed to unmarshal struct data: %v", err)
	}
	if decoded.ID != 42 || decoded.Title != "Test" {
		t.Errorf("unexpected unmarshaled struct: %+v", decoded)
	}

	// 2. NewMessageEvent with string
	strEvt, err := event.NewMessageEvent("client-1", "channel-news", "breaking news")
	if err != nil {
		t.Fatalf("failed to create string message event: %v", err)
	}
	strData, err := strEvt.GetStringData()
	if err != nil {
		t.Fatalf("failed to get string data: %v", err)
	}
	if strData != "breaking news" {
		t.Errorf("expected 'breaking news', got '%s'", strData)
	}

	// 3. NewAttachEvent
	attachEvt, err := event.NewAttachEvent("client-1", "metrics")
	if err != nil {
		t.Fatalf("failed to create attach event: %v", err)
	}
	if attachEvt.GetType() != event.TypeAttach {
		t.Errorf("expected type %s, got %s", event.TypeAttach, attachEvt.GetType())
	}
	if attachEvt.GetTarget() != "metrics" {
		t.Errorf("expected target metrics, got %s", attachEvt.GetTarget())
	}

	// 4. NewDetachEvent
	detachEvt, err := event.NewDetachEvent("client-1", "metrics")
	if err != nil {
		t.Fatalf("failed to create detach event: %v", err)
	}
	if detachEvt.GetType() != event.TypeDetach {
		t.Errorf("expected type %s, got %s", event.TypeDetach, detachEvt.GetType())
	}

	// 5. NewPingEvent & NewPongEvent
	pingEvt, err := event.NewPingEvent("client-1")
	if err != nil {
		t.Fatalf("failed to create ping event: %v", err)
	}
	if pingEvt.GetType() != event.TypePing {
		t.Errorf("expected type %s, got %s", event.TypePing, pingEvt.GetType())
	}

	pongEvt, err := event.NewPongEvent("server", "client-1")
	if err != nil {
		t.Fatalf("failed to create pong event: %v", err)
	}
	if pongEvt.GetType() != event.TypePong {
		t.Errorf("expected type %s, got %s", event.TypePong, pongEvt.GetType())
	}

	// 6. GenericStringMessage
	gsm := event.NewGenericStringMessage(event.TypeMessage, "hello generic")
	evtFromData, err := event.NewEvent(gsm, "alerts", "token123")
	if err != nil {
		t.Fatalf("failed to create event from DataInterface: %v", err)
	}
	if evtFromData.GetToken() != "token123" {
		t.Errorf("expected token token123, got %s", evtFromData.GetToken())
	}

	// 7. JSON Marshal & Unmarshal of Event Envelope
	b, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}
	var roundtrip event.Event
	if err := json.Unmarshal(b, &roundtrip); err != nil {
		t.Fatalf("failed to unmarshal event: %v", err)
	}
	if roundtrip.Type != evt.Type || roundtrip.Source != evt.Source || roundtrip.Target != evt.Target {
		t.Errorf("roundtrip mismatch: %+v vs %+v", roundtrip, evt)
	}

	// 8. NTP Query, Response & Error Events
	rawNTP := make([]byte, 48)
	rawNTP[0] = 0x1b // NTP request header
	ntpQuery, err := event.NewNTPQueryEvent("client-1", rawNTP)
	if err != nil {
		t.Fatalf("failed to create NTP query event: %v", err)
	}
	if ntpQuery.GetType() != event.TypeNTPQuery {
		t.Errorf("expected type %s, got %s", event.TypeNTPQuery, ntpQuery.GetType())
	}
	gotNTPData, err := ntpQuery.GetData()
	if err != nil {
		t.Fatalf("failed to get NTP query data: %v", err)
	}
	rawBytes, ok := gotNTPData.([]byte)
	if !ok || len(rawBytes) != 48 || rawBytes[0] != 0x1b {
		t.Errorf("expected 48-byte NTP slice with first byte 0x1b, got %v (%T)", gotNTPData, gotNTPData)
	}

	ntpResp, err := event.NewNTPResponseEvent("wsbroker", "client-1", rawNTP)
	if err != nil {
		t.Fatalf("failed to create NTP response event: %v", err)
	}
	if ntpResp.GetType() != event.TypeNTPResponse {
		t.Errorf("expected type %s, got %s", event.TypeNTPResponse, ntpResp.GetType())
	}
	gotRespData, err := ntpResp.GetData()
	if err != nil {
		t.Fatalf("failed to get NTP response data: %v", err)
	}
	respBytes, ok := gotRespData.([]byte)
	if !ok || len(respBytes) != 48 {
		t.Errorf("expected 48 bytes NTP response, got %v", gotRespData)
	}

	ntpErr, err := event.NewNTPErrorEvent("wsbroker", "client-1", "ntp server timeout")
	if err != nil {
		t.Fatalf("failed to create NTP error event: %v", err)
	}
	if ntpErr.GetType() != event.TypeNTPError {
		t.Errorf("expected type %s, got %s", event.TypeNTPError, ntpErr.GetType())
	}
	gotErrData, err := ntpErr.GetData()
	if err != nil {
		t.Fatalf("failed to get NTP error data: %v", err)
	}
	if errStr, ok := gotErrData.(string); !ok || errStr != "ntp server timeout" {
		t.Errorf("expected error string 'ntp server timeout', got %v", gotErrData)
	}

	// 9. File Message & File Event
	filePayload := []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A} // PNG header
	fileMsg := event.NewFileMessage("sample.png", "image/png", filePayload)
	if fileMsg.Type() != event.TypeFile {
		t.Errorf("expected fileMsg type %s, got %s", event.TypeFile, fileMsg.Type())
	}
	if fileMsg.Size != int64(len(filePayload)) {
		t.Errorf("expected size %d, got %d", len(filePayload), fileMsg.Size)
	}
	if fileMsg.String() != "File: sample.png (image/png, 8 bytes)" {
		t.Errorf("unexpected String representation: %s", fileMsg.String())
	}

	fileEvt, err := event.NewFileEvent("client-1", "channel-files", fileMsg, "auth-token-1")
	if err != nil {
		t.Fatalf("failed to create file event: %v", err)
	}
	if fileEvt.GetType() != event.TypeFile {
		t.Errorf("expected event type %s, got %s", event.TypeFile, fileEvt.GetType())
	}
	if fileEvt.GetToken() != "auth-token-1" {
		t.Errorf("expected token auth-token-1, got %s", fileEvt.GetToken())
	}

	// Test GetData with TypeFile
	gotFileData, err := fileEvt.GetData()
	if err != nil {
		t.Fatalf("failed to get file event data: %v", err)
	}
	unmarshaledFileMsg, ok := gotFileData.(*event.FileMessage)
	if !ok {
		t.Fatalf("expected *event.FileMessage, got %T", gotFileData)
	}
	if unmarshaledFileMsg.Filename != "sample.png" || unmarshaledFileMsg.MimeType != "image/png" || unmarshaledFileMsg.Size != 8 {
		t.Errorf("unexpected file metadata: %+v", unmarshaledFileMsg)
	}
	if string(unmarshaledFileMsg.Data) != string(filePayload) {
		t.Errorf("payload mismatch: %v vs %v", unmarshaledFileMsg.Data, filePayload)
	}

	// Test UnmarshalData
	var directFileMsg event.FileMessage
	if err := fileEvt.UnmarshalData(&directFileMsg); err != nil {
		t.Fatalf("failed to unmarshal file data directly: %v", err)
	}
	if directFileMsg.Filename != "sample.png" || directFileMsg.MimeType != "image/png" {
		t.Errorf("unexpected direct unmarshal result: %+v", directFileMsg)
	}

	// Test NewEvent with DataInterface
	evtFromInterface, err := event.NewEvent(fileMsg, "target-ch", "tok")
	if err != nil {
		t.Fatalf("failed to create event from DataInterface for FileMessage: %v", err)
	}
	if evtFromInterface.GetType() != event.TypeFile {
		t.Errorf("expected type %s, got %s", event.TypeFile, evtFromInterface.GetType())
	}

	// 10. Channel Info Query, Response & Error Events
	ciQuery, err := event.NewChannelInfoQueryEvent("client-1", "channel-alerts")
	if err != nil {
		t.Fatalf("failed to create channel info query event: %v", err)
	}
	if ciQuery.GetType() != event.TypeChannelInfoQuery {
		t.Errorf("expected type %s, got %s", event.TypeChannelInfoQuery, ciQuery.GetType())
	}
	if ciQuery.GetTarget() != "channel-alerts" {
		t.Errorf("expected target 'channel-alerts', got %s", ciQuery.GetTarget())
	}
	gotQueryData, err := ciQuery.GetData()
	if err != nil {
		t.Fatalf("failed to get channel info query data: %v", err)
	}
	if qStr, ok := gotQueryData.(string); !ok || qStr != "channel-alerts" {
		t.Errorf("expected query data 'channel-alerts', got %v", gotQueryData)
	}

	info := event.ChannelInfo{
		Name:        "channel-alerts",
		Subscribers: 2,
		Clients:     []string{"client-1", "client-2"},
	}
	if info.Type() != event.TypeChannelInfoResponse {
		t.Errorf("expected info type %s, got %s", event.TypeChannelInfoResponse, info.Type())
	}
	if info.String() != "Channel: channel-alerts (2 subscribers)" {
		t.Errorf("unexpected ChannelInfo String: %s", info.String())
	}

	ciResp, err := event.NewChannelInfoResponseEvent("client-1", info)
	if err != nil {
		t.Fatalf("failed to create channel info response event: %v", err)
	}
	if ciResp.GetType() != event.TypeChannelInfoResponse {
		t.Errorf("expected type %s, got %s", event.TypeChannelInfoResponse, ciResp.GetType())
	}
	if ciResp.GetTarget() != "client-1" {
		t.Errorf("expected target 'client-1', got %s", ciResp.GetTarget())
	}
	if ciResp.GetSource() != "wsbroker" {
		t.Errorf("expected source 'wsbroker', got %s", ciResp.GetSource())
	}

	gotRespCiData, err := ciResp.GetData()
	if err != nil {
		t.Fatalf("failed to get channel info response data: %v", err)
	}
	unmarshaledCI, ok := gotRespCiData.(*event.ChannelInfo)
	if !ok {
		t.Fatalf("expected *event.ChannelInfo, got %T", gotRespCiData)
	}
	if unmarshaledCI.Name != "channel-alerts" || unmarshaledCI.Subscribers != 2 || len(unmarshaledCI.Clients) != 2 {
		t.Errorf("unexpected unmarshaled ChannelInfo: %+v", unmarshaledCI)
	}

	var directCI event.ChannelInfo
	if err := ciResp.UnmarshalData(&directCI); err != nil {
		t.Fatalf("failed to unmarshal channel info directly: %v", err)
	}
	if directCI.Name != "channel-alerts" || directCI.Subscribers != 2 {
		t.Errorf("unexpected direct unmarshal: %+v", directCI)
	}

	ciErr, err := event.NewChannelInfoErrorEvent("client-1", "access denied to channel")
	if err != nil {
		t.Fatalf("failed to create channel info error event: %v", err)
	}
	if ciErr.GetType() != event.TypeChannelInfoError {
		t.Errorf("expected type %s, got %s", event.TypeChannelInfoError, ciErr.GetType())
	}
	gotCiErrData, err := ciErr.GetData()
	if err != nil {
		t.Fatalf("failed to get channel info error data: %v", err)
	}
	if errStr, ok := gotCiErrData.(string); !ok || errStr != "access denied to channel" {
		t.Errorf("expected error string 'access denied to channel', got %v", gotCiErrData)
	}

	// 11. WSMessage & NewWSMessage
	wsMsg := event.NewWSMessage("in", evt)
	if wsMsg.Direction != "in" {
		t.Errorf("expected direction 'in', got %s", wsMsg.Direction)
	}
	if wsMsg.Type != string(event.TypeMessage) {
		t.Errorf("expected type '%s', got %s", event.TypeMessage, wsMsg.Type)
	}
	if wsMsg.Source != "client-1" {
		t.Errorf("expected source 'client-1', got %s", wsMsg.Source)
	}
	if wsMsg.Target != "channel-alerts" {
		t.Errorf("expected target 'channel-alerts', got %s", wsMsg.Target)
	}

	nilWSMsg := event.NewWSMessage("out", nil)
	if nilWSMsg.Direction != "out" {
		t.Errorf("expected direction 'out', got %s", nilWSMsg.Direction)
	}
}
