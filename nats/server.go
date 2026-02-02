package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"

	// "path/filepath"

	// "os/exec"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/yeshu2004/video-pipline/aws"
	"github.com/yeshu2004/video-pipline/models"
)

type Nats struct {
	js jetstream.JetStream
}

func NewNATS() (*Nats, error) {
	conn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		return nil, fmt.Errorf("nats connection error: %w", err)
	}

	// jetstream instance
	js, err := jetstream.New(conn)
	if err != nil {
		return nil, fmt.Errorf("jetstream error: %w", err)
	}

	return &Nats{js: js}, nil
}

func (nats *Nats) CreateVideoStream(ctx context.Context) error {
	_, err := nats.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:        "VIDEO",
		Description: "Video processing events",
		Subjects:    []string{"VIDEO.*"},
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
		Discard:     jetstream.DiscardOld,
		MaxMsgs:     -1,
		MaxBytes:    -1,
	})
	return err
}

func (nats *Nats) PublishVideoUplodedEvent(ctx context.Context, key string, payload []byte) error {
	_, err := nats.js.Publish(ctx, "VIDEO.uploaded", payload, jetstream.WithMsgID(fmt.Sprintf("video-%s", key)))
	return err
}

func (nats *Nats) CreateFFmpeg480Consumer(ctx context.Context) error {
	_, err := nats.js.CreateConsumer(ctx, "VIDEO", jetstream.ConsumerConfig{
		Name:          "ffmpeg-480-worker",
		Durable:       "ffmpeg-480-worker",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
		MaxDeliver:    5,
		FilterSubject: "VIDEO.uploaded",
	})
	return err
}

func (nats *Nats) CreateFFmpeg720Consumer(ctx context.Context) error {
	_, err := nats.js.CreateConsumer(ctx, "VIDEO", jetstream.ConsumerConfig{
		Name:          "ffmpeg-720-worker",
		Durable:       "ffmpeg-720-worker",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
		MaxDeliver:    5,
		FilterSubject: "VIDEO.uploaded",
	})
	return err
}

func (nats *Nats) ConsumeFFmpeg480Event(ctx context.Context) error {
	c, err := nats.js.Consumer(ctx, "VIDEO", "ffmpeg-480-worker")
	if err != nil {
		return err
	}

	godotenv.Load();
	cfg := aws.LoadAwsConifg();
	service := aws.NewS3Service(cfg);

	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down booking event consumer...")
			return nil
		default:
			msgs, err := c.Fetch(1, jetstream.FetchMaxWait(30*time.Second))
			if err != nil {
				if errors.Is(err, jetstream.ErrNotJSMessage) {
					continue
				}
				log.Println("fetch error:", err)
				continue
			}

			for msg := range msgs.Messages() {
				if err := processVideoForFFmpeg(ctx, service, "480", msg.Data()); err != nil {
					log.Println("480 processing failed:", err)
					msg.Nak()
					continue
				}

				msg.Ack()
			}
		}
	}
}

func processVideoForFFmpeg(ctx context.Context, awsService *aws.S3Service, videoQuality string, eventData []byte) error {
	quality, err := strconv.Atoi(videoQuality)
	if err != nil {
		return err
	}

	var payload *models.Payload
	if err := json.Unmarshal(eventData, &payload); err != nil {
		return fmt.Errorf("json unmarshal error: %v", err)
	}

	log.Printf("processing video with key %s at %dp\n", payload.Key, quality)

	// 0. fetch the presigned url from s3 service
	url, err := awsService.FetchGetPresignedURL(ctx, payload.Bucket, payload.Key);
	if err != nil{
		return  fmt.Errorf("error fetching sigend url of given file (%s) over bucket (%s): error: %v", payload.Key, payload.Bucket, err);
	}

	// 1. download the video using http.get
	resp, err := http.Get(url);
	if err != nil{
		return err;
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download video: status %d", resp.StatusCode)
	}
	log.Printf("downloaded video with key %s from s3 bucket successfully\n", payload.Key);	
	defer resp.Body.Close()

	//2. create local directory and file
	localPath := payload.Key;
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return err
	}
	log.Printf("created local directory for key %s successfully\n", payload.Key);

	out, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer out.Close()
	log.Printf("created local file for key %s successfully\n", payload.Key);

	//3. copy the downloaded content into local file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	log.Printf("saved video to local file for key %s successfully\n", payload.Key);

	// //3. run the ffmpeg command
	outputFolder:= "processed/480p"
	if err := os.MkdirAll(outputFolder, 0755);err != nil{
		return err;
	}
	log.Printf("created output directory %s successfully\n", outputFolder);

	outputFilePath := filepath.Join(outputFolder, filepath.Base(payload.Key));

	scale := fmt.Sprintf("scale=-2:%d", quality)
	cmd := exec.CommandContext(
		ctx,
		"ffmpeg",
		"-i", localPath,
		"-vf", scale,
		"-c:v", "libx264",
		"-crf", "23",
		"-c:a", "copy",
		outputFilePath,
	)

	b, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ffmpeg error: %v, output: %s", err, string(b))
	}
	// //4. store in processed folder
	// //5. upload back to cloud


	log.Printf("processing video done with key %s at %dp\n", payload.Key, quality)
	return nil
}
