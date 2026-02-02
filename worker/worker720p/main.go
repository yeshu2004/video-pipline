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

	if err := nat.CreateFFmpeg720Consumer(ctx); err!= nil{
		log.Fatalf("create ffmpeg 720 consumer failed: %v", err)
	}

	if err := nat.ConsumeFFmpeg720Event(ctx); err != nil {
		log.Fatalf("consume ffmpeg 720 consumer failed: %v", err)
	}
}
