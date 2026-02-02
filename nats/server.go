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

func (nats *Nats) CreateFFmpeg240Consumer(ctx context.Context) error {
	_, err := nats.js.CreateConsumer(ctx, "VIDEO", jetstream.ConsumerConfig{
		Name:          "ffmpeg-240-worker",
		Durable:       "ffmpeg-240-worker",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
		MaxDeliver:    5,
		FilterSubject: "VIDEO.uploaded",
	})
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

func (nats *Nats) ConsumeFFmpeg720Event(ctx context.Context) error {
	c, err := nats.js.Consumer(ctx, "VIDEO", "ffmpeg-720-worker")
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
				if err := processVideoForFFmpeg(ctx, service, "720", msg.Data()); err != nil {
					log.Println("720 processing failed:", err)
					msg.Nak()
					continue
				}

				msg.Ack()
			}
		}
	}
}

func (nats *Nats) ConsumeFFmpeg240Event(ctx context.Context) error {
	c, err := nats.js.Consumer(ctx, "VIDEO", "ffmpeg-240-worker")
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
				if err := processVideoForFFmpeg(ctx, service, "240", msg.Data()); err != nil {
					log.Println("240 processing failed:", err)
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

	log.Printf("(%s) processing video with key %s at %dp\n", videoQuality, payload.Key, quality)

	// 0. fetch the presigned url from s3 service
	url, err := awsService.FetchGetPresignedURL(ctx, payload.Bucket, payload.Key);
	if err != nil{
		return  fmt.Errorf("(%s) error fetching presigned url of given file (%s) over bucket (%s): error: %v", videoQuality, payload.Key, payload.Bucket, err);
	}

	// 1. download the video using http.get
	resp, err := http.Get(url);
	if err != nil{
		return err;
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("(%s) failed to download video: status %d", videoQuality, resp.StatusCode)
	}
	log.Printf("(%s) downloaded video with key %s from s3 bucket successfully\n", videoQuality, payload.Key);	
	defer resp.Body.Close()

	//2. create local directory and file
	filename := filepath.Base(payload.Key)
	rawPath := filepath.Join("raw", filename)

	if err := os.MkdirAll("raw", 0755); err != nil {
		return err
	}

	out, err := os.Create(rawPath)
	if err != nil {
		return err
	}

	defer out.Close()
	log.Printf("(%s) created local file for key %s successfully\n", videoQuality, payload.Key);

	// 3. copy the downloaded content into local file
	log.Printf("(%s) copying the file (%s) locally..\n.", videoQuality, payload.Key);
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	log.Printf("(%s) saved video to local file for key %s successfully\n", videoQuality, payload.Key);

	// 4. run the ffmpeg command & store in processed folder
	outputFolder:= fmt.Sprintf("processed/%sp", videoQuality);
	if err := os.MkdirAll(outputFolder, 0755);err != nil{
		return err;
	}
	log.Printf("(%s) created output directory %s successfully\n", videoQuality, outputFolder);

	outputFilePath := filepath.Join(outputFolder, filepath.Base(payload.Key));

	scale := fmt.Sprintf("scale=-2:%d", quality)
	cmd := exec.CommandContext(
		ctx,
		"ffmpeg",
		"-i", rawPath,
		"-vf", scale,
		"-c:v", "libx264",
		"-crf", "23",
		"-c:a", "copy",
		outputFilePath,
	)

	b, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("(%s) ffmpeg error: %v, output: %s", videoQuality, err, string(b))
	}
	// //5. upload back to cloud


	log.Printf("(%s) processing video done with key %s at %dp\n", videoQuality, payload.Key, quality)
	return nil
}
