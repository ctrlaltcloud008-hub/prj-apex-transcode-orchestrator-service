package models

type RenditionSpec struct {
	Name              string `json:"name"`
	Width             int    `json:"width"`
	Height            int    `json:"height"`
	TargetBitrateKbps int64  `json:"target_bitrate_kbps"`
	Codec             string `json:"codec"`
	IsHDR             bool   `json:"is_hdr"`
}

type TranscodeJobRequestedPayload struct {
	VideoID         string `json:"video_id"`
	RenditionName   string `json:"rendition_name"`
	Attempt         int64  `json:"attempt"`
	SourceGCSURI    string `json:"source_gcs_uri"`
	OutputGCSPrefix string `json:"output_gcs_prefix"`
	RenditionSpec
}

type TranscodeJobCompletedPayload struct {
	VideoID         string `json:"video_id"`
	RenditionName   string `json:"rendition_name"`
	Attempt         int64  `json:"attempt"`
	Status          string `json:"status"`
	WorkerID        string `json:"worker_id"`
	OutputGCSPrefix string `json:"output_gcs_prefix"`
	ErrorMessage    string `json:"error_message,omitempty"`
}

type VideoTranscodedPayload struct {
	VideoID string `json:"video_id"`
}
