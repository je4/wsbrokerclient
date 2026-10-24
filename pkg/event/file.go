package event

import (
	"fmt"
)

// FileMessage represents a binary file payload with metadata.
type FileMessage struct {
	Filename string `json:"filename"`
	MimeType string `json:"mime_type"`
	Size     int64  `json:"size"`
	Data     []byte `json:"data"`
}

// NewFileMessage creates a new FileMessage with the provided metadata and binary content.
// If size <= 0, it is automatically derived from the length of the data slice.
func NewFileMessage(filename, mimeType string, data []byte, size ...int64) *FileMessage {
	dataSize := int64(len(data))
	if len(size) > 0 && size[0] > 0 {
		dataSize = size[0]
	}
	return &FileMessage{
		Filename: filename,
		MimeType: mimeType,
		Size:     dataSize,
		Data:     data,
	}
}

// String returns a human-readable representation of the file metadata.
func (f *FileMessage) String() string {
	if f == nil {
		return "<nil file>"
	}
	return fmt.Sprintf("File: %s (%s, %d bytes)", f.Filename, f.MimeType, f.Size)
}

// Type returns the EventType for file messages.
func (f *FileMessage) Type() EventType {
	return TypeFile
}

var _ DataInterface = (*FileMessage)(nil)
