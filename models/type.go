package models

import "time"

type Payload struct{
	Key       string    `json:"key"`
	Bucket    string    `json:"bucket"`
	Timestamp time.Time `json:"timestamp"`
}

type ConfirmRequest struct {
	Key string `json:"key"`
}