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
	var roundtrip event.RawEvent
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
	evt.ID = "evt-12345"
	evt.RefID = "ref-0000"
	evt.Channel = "channel-alerts"
	wsMsg := event.NewWSMessage("in", evt)
	if wsMsg.Direction != "in" {
		t.Errorf("expected direction 'in', got %s", wsMsg.Direction)
	}
	if wsMsg.ID != "evt-12345" {
		t.Errorf("expected id 'evt-12345', got %s", wsMsg.ID)
	}
	if wsMsg.RefID != "ref-0000" {
		t.Errorf("expected refID 'ref-0000', got %s", wsMsg.RefID)
	}
	if wsMsg.Channel != "channel-alerts" {
		t.Errorf("expected channel 'channel-alerts', got %s", wsMsg.Channel)
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

func TestEventIDAndChannelHelpers(t *testing.T) {
	// Test GenerateEventID uniqueness and length
	id1 := event.GenerateEventID()
	id2 := event.GenerateEventID()
	if id1 == "" || id2 == "" {
		t.Fatalf("expected non-empty event IDs, got %q and %q", id1, id2)
	}
	if id1 == id2 {
		t.Fatalf("expected unique event IDs, got duplicate %q", id1)
	}
	if len(id1) < 16 {
		t.Errorf("expected sufficiently long event ID, got length %d", len(id1))
	}

	// Test Getters and Setters on nil Event
	var nilEvt *event.RawEvent
	if nilEvt.GetID() != "" {
		t.Errorf("expected empty ID for nil event, got %q", nilEvt.GetID())
	}
	if nilEvt.GetChannel() != "" {
		t.Errorf("expected empty Channel for nil event, got %q", nilEvt.GetChannel())
	}
	nilEvt.SetID("test-id")
	nilEvt.SetChannel("test-ch")

	// Test Getters and Setters on active Event
	evt := &event.RawEvent{}
	evt.SetID("evt-custom-99")
	evt.SetChannel("room-dev")
	if evt.GetID() != "evt-custom-99" {
		t.Errorf("expected ID 'evt-custom-99', got %q", evt.GetID())
	}
	if evt.GetChannel() != "room-dev" {
		t.Errorf("expected Channel 'room-dev', got %q", evt.GetChannel())
	}

	// Test String() format with Channel and ID
	strWithCh := evt.String()
	if strWithCh == "" || !containsSubstring(strWithCh, "room-dev") || !containsSubstring(strWithCh, "evt-custom-99") {
		t.Errorf("unexpected event String() representation: %s", strWithCh)
	}

	evtOnlyCh := &event.RawEvent{BaseEvent: event.BaseEvent{Type: event.TypeMessage, Source: "s", Target: "t", Channel: "ch1"}}
	if evtOnlyCh.String() != "message [s -> t (channel: ch1)]" {
		t.Errorf("expected 'message [s -> t (channel: ch1)]', got %q", evtOnlyCh.String())
	}

	evtOnlyID := &event.RawEvent{BaseEvent: event.BaseEvent{Type: event.TypeMessage, Source: "s", Target: "t", ID: "id1"}}
	if evtOnlyID.String() != "message [s -> t] (id1)" {
		t.Errorf("expected 'message [s -> t] (id1)', got %q", evtOnlyID.String())
	}

	evtBoth := &event.RawEvent{BaseEvent: event.BaseEvent{Type: event.TypeMessage, Source: "s", Target: "t", Channel: "ch1", ID: "id1"}}
	if evtBoth.String() != "message [s -> t (channel: ch1)] (id1)" {
		t.Errorf("expected 'message [s -> t (channel: ch1)] (id1)', got %q", evtBoth.String())
	}

	// Test JSON serialization preserves ID and Channel
	b, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}
	var unmarshaled event.RawEvent
	if err := json.Unmarshal(b, &unmarshaled); err != nil {
		t.Fatalf("failed to unmarshal event: %v", err)
	}
	if unmarshaled.ID != "evt-custom-99" {
		t.Errorf("expected unmarshaled ID 'evt-custom-99', got %q", unmarshaled.ID)
	}
	if unmarshaled.Channel != "room-dev" {
		t.Errorf("expected unmarshaled Channel 'room-dev', got %q", unmarshaled.Channel)
	}
}

func containsSubstring(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || stringContains(s, sub))
}

func stringContains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestEventRefIDAndReplyHelpers(t *testing.T) {
	// 1. Getters and Setters for RefID
	var nilEvt *event.RawEvent
	if nilEvt.GetRefID() != "" {
		t.Errorf("expected empty RefID for nil event, got %q", nilEvt.GetRefID())
	}
	nilEvt.SetRefID("ref-123")

	orig := &event.RawEvent{
		BaseEvent: event.BaseEvent{
			ID:      "evt-orig-123",
			Type:    event.TypeMessage,
			Source:  "client-alice",
			Target:  "client-bob",
			Channel: "chat-room",
			Token:   "tok-secret",
		},
	}
	orig.SetRefID("ref-parent-0")
	if orig.GetRefID() != "ref-parent-0" {
		t.Errorf("expected RefID 'ref-parent-0', got %q", orig.GetRefID())
	}

	// 2. String() with RefID formatting
	evtOnlyRef := &event.RawEvent{BaseEvent: event.BaseEvent{Type: event.TypeMessage, Source: "s", Target: "t", RefID: "ref1"}}
	if evtOnlyRef.String() != "message [s -> t] (ref: ref1)" {
		t.Errorf("expected 'message [s -> t] (ref: ref1)', got %q", evtOnlyRef.String())
	}

	evtBothIDAndRef := &event.RawEvent{BaseEvent: event.BaseEvent{Type: event.TypeMessage, Source: "s", Target: "t", ID: "id1", RefID: "ref1"}}
	if evtBothIDAndRef.String() != "message [s -> t] (id: id1, ref: ref1)" {
		t.Errorf("expected 'message [s -> t] (id: id1, ref: ref1)', got %q", evtBothIDAndRef.String())
	}

	evtBothWithChan := &event.RawEvent{BaseEvent: event.BaseEvent{Type: event.TypeMessage, Source: "s", Target: "t", Channel: "ch1", ID: "id1", RefID: "ref1"}}
	if evtBothWithChan.String() != "message [s -> t (channel: ch1)] (id: id1, ref: ref1)" {
		t.Errorf("expected 'message [s -> t (channel: ch1)] (id: id1, ref: ref1)', got %q", evtBothWithChan.String())
	}

	// 3. WSMessage with RefID
	wsMsg := event.NewWSMessage("in", evtBothIDAndRef)
	if wsMsg.RefID != "ref1" {
		t.Errorf("expected WSMessage RefID 'ref1', got %q", wsMsg.RefID)
	}

	// 4. CreateReply & NewReplyEvent
	reply, err := orig.CreateReply(event.TypeMessage, "hello alice, this is bob")
	if err != nil {
		t.Fatalf("CreateReply failed: %v", err)
	}
	if reply.ID == "" || reply.ID == orig.ID {
		t.Errorf("expected unique new ID for reply, got %q", reply.ID)
	}
	if reply.RefID != orig.ID {
		t.Errorf("expected reply RefID %q, got %q", orig.ID, reply.RefID)
	}
	if reply.Target != orig.Source {
		t.Errorf("expected reply target %q, got %q", orig.Source, reply.Target)
	}
	if reply.Channel != orig.Channel {
		t.Errorf("expected reply channel %q, got %q", orig.Channel, reply.Channel)
	}
	if reply.Token != orig.Token {
		t.Errorf("expected reply token %q, got %q", orig.Token, reply.Token)
	}
	replyData, err := reply.GetStringData()
	if err != nil || replyData != "hello alice, this is bob" {
		t.Errorf("unexpected reply data: %q (err: %v)", replyData, err)
	}

	replyViaFunc, err := event.NewReplyEvent(orig, event.TypePong, "pong-data")
	if err != nil {
		t.Fatalf("NewReplyEvent failed: %v", err)
	}
	if replyViaFunc.RefID != orig.ID || replyViaFunc.Target != orig.Source || replyViaFunc.Type != event.TypePong {
		t.Errorf("unexpected replyViaFunc: %+v", replyViaFunc)
	}

	// 5. Response constructors with RefID
	pong, err := event.NewPongEvent("server", "client-1", "pong-payload", "ref-ping-99")
	if err != nil {
		t.Fatalf("NewPongEvent failed: %v", err)
	}
	if pong.RefID != "ref-ping-99" {
		t.Errorf("expected pong RefID 'ref-ping-99', got %q", pong.RefID)
	}

	ntpResp, err := event.NewNTPResponseEvent("server", "client-1", []byte{1, 2, 3}, "ref-ntp-1")
	if err != nil {
		t.Fatalf("NewNTPResponseEvent failed: %v", err)
	}
	if ntpResp.RefID != "ref-ntp-1" {
		t.Errorf("expected ntpResp RefID 'ref-ntp-1', got %q", ntpResp.RefID)
	}

	ntpErr, err := event.NewNTPErrorEvent("server", "client-1", "timeout", "ref-ntp-2")
	if err != nil {
		t.Fatalf("NewNTPErrorEvent failed: %v", err)
	}
	if ntpErr.RefID != "ref-ntp-2" {
		t.Errorf("expected ntpErr RefID 'ref-ntp-2', got %q", ntpErr.RefID)
	}

	ciResp, err := event.NewChannelInfoResponseEvent("client-1", event.ChannelInfo{Name: "alerts"}, "broker", "ref-ci-1")
	if err != nil {
		t.Fatalf("NewChannelInfoResponseEvent failed: %v", err)
	}
	if ciResp.RefID != "ref-ci-1" {
		t.Errorf("expected ciResp RefID 'ref-ci-1', got %q", ciResp.RefID)
	}

	ciErr, err := event.NewChannelInfoErrorEvent("client-1", "err", "broker", "ref-ci-2")
	if err != nil {
		t.Fatalf("NewChannelInfoErrorEvent failed: %v", err)
	}
	if ciErr.RefID != "ref-ci-2" {
		t.Errorf("expected ciErr RefID 'ref-ci-2', got %q", ciErr.RefID)
	}
}

func TestGenericEventAndConversions(t *testing.T) {
	// 1. NewEventTyped
	payload := customPayload{ID: 100, Title: "Generic Test"}
	typedEvt, err := event.NewEventTyped(event.TypeMessage, "client-a", "channel-b", payload)
	if err != nil {
		t.Fatalf("NewEventTyped failed: %v", err)
	}
	if typedEvt.Data.ID != 100 || typedEvt.Data.Title != "Generic Test" {
		t.Errorf("unexpected typedEvt Data: %+v", typedEvt.Data)
	}
	if typedEvt.GetSource() != "client-a" || typedEvt.GetTarget() != "channel-b" {
		t.Errorf("unexpected metadata on typedEvt: source=%s, target=%s", typedEvt.GetSource(), typedEvt.GetTarget())
	}

	// 2. ToRaw from typed Event
	rawEvt, err := event.ToRaw(typedEvt)
	if err != nil {
		t.Fatalf("ToRaw failed: %v", err)
	}
	if rawEvt.Type != event.TypeMessage || rawEvt.Source != "client-a" || rawEvt.Target != "channel-b" {
		t.Errorf("metadata mismatch on rawEvt: %+v", rawEvt)
	}

	// 3. ToTyped from RawEvent
	backToTyped, err := event.ToTyped[customPayload](rawEvt)
	if err != nil {
		t.Fatalf("ToTyped failed: %v", err)
	}
	if backToTyped.Data.ID != 100 || backToTyped.Data.Title != "Generic Test" {
		t.Errorf("unexpected backToTyped Data: %+v", backToTyped.Data)
	}

	// 4. Wire format parity between Event[T] and RawEvent
	typedJSON, err := json.Marshal(typedEvt)
	if err != nil {
		t.Fatalf("Marshal typedEvt failed: %v", err)
	}
	rawJSON, err := json.Marshal(rawEvt)
	if err != nil {
		t.Fatalf("Marshal rawEvt failed: %v", err)
	}
	if string(typedJSON) != string(rawJSON) {
		t.Errorf("JSON serialization mismatch:\ntyped: %s\nraw:   %s", string(typedJSON), string(rawJSON))
	}

	// 5. Primitive type payloads in Event[T]
	strTyped, err := event.NewEventTyped(event.TypeMessage, "s", "t", "simple string")
	if err != nil {
		t.Fatalf("NewEventTyped with string failed: %v", err)
	}
	strRaw, err := event.ToRaw(strTyped)
	if err != nil {
		t.Fatalf("ToRaw with string failed: %v", err)
	}
	backStr, err := event.ToTyped[string](strRaw)
	if err != nil {
		t.Fatalf("ToTyped with string failed: %v", err)
	}
	if backStr.Data != "simple string" {
		t.Errorf("expected 'simple string', got %q", backStr.Data)
	}

	// 6. BaseEvent timestamp
	now := typedEvt.Timestamp
	typedEvt.SetTimestamp(now)
	if typedEvt.GetTimestamp() != now {
		t.Errorf("expected timestamp %v, got %v", now, typedEvt.GetTimestamp())
	}
}
