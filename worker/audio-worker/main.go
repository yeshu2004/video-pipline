package main

import (
	"context"
	"log"

	"github.com/yeshu2004/video-pipline/nats"
)

func main() {
	ctx := context.Background()

	nat, err := nats.NewNATS()
	if err != nil {
		log.Fatalf("%s", err.Error())
	}

	if err := nat.CreateVideoStream(ctx); err != nil {
		log.Fatalf("create video stream failed: %v", err)
	}

	if err := nat.CreateAudioTranscription(ctx); err != nil {
		log.Fatalf("create audio transcription consumer failed: %v", err)
	}

	if err := nat.ConsumeAudioTranscriptionEvent(ctx); err != nil {
		log.Fatalf("consume audio transcription falied: %v", err);
	}

}
