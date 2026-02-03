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

	if err := nat.CreateVideoDownloadConsumer(ctx); err != nil {
		log.Fatalf("create video download consumer failed: %v", err)
	}

	if err := nat.ConsumeVideoDownlodEvent(ctx); err != nil {
		log.Fatalf("consume video download falied: %v", err)
	}

}
