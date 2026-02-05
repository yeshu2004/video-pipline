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
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/yeshu2004/video-pipline/ai"
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

// PUBLISH EVENT
func (nats *Nats) PublishVideoUploadedEvent(ctx context.Context, key string, payload []byte) error {
	_, err := nats.js.Publish(ctx, "VIDEO.uploaded", payload, jetstream.WithMsgID(fmt.Sprintf("video-uploaded-%s", key)))
	return err
}

// CREATE CONSUMER

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

func (nats *Nats) CreateAudioTranscription(ctx context.Context) error {
	_, err := nats.js.CreateConsumer(ctx, "VIDEO", jetstream.ConsumerConfig{
		Name:          "audio-transcription-woker",
		Durable:       "audio-transcription-woker",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
		MaxDeliver:    5,
		FilterSubject: "VIDEO.uploaded",
	})
	return err
}

// CONSUME EVENT
func (nats *Nats) ConsumeAudioTranscriptionEvent(ctx context.Context) error {
	c, err := nats.js.Consumer(ctx, "VIDEO", "audio-transcription-woker")
	if err != nil {
		return err
	}

	godotenv.Load()
	cfg := aws.LoadAwsConifg()
	service := aws.NewS3Service(cfg)

	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down audio transcription event consumer...")
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
				if err := nats.processAudioTranscriptionEvent(ctx, service, msg.Data()); err != nil {
					log.Println("processing failed:", err)
					msg.Nak()
					continue
				}

				msg.Ack()
			}
		}
	}
}

func (nats *Nats) ConsumeFFmpeg480Event(ctx context.Context) error {
	c, err := nats.js.Consumer(ctx, "VIDEO", "ffmpeg-480-worker")
	if err != nil {
		return err
	}

	godotenv.Load()
	cfg := aws.LoadAwsConifg()
	service := aws.NewS3Service(cfg)

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
				if err := nats.processVideoForFFmpeg(ctx, service, "480", msg.Data()); err != nil {
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

	godotenv.Load()
	cfg := aws.LoadAwsConifg()
	service := aws.NewS3Service(cfg)

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
				if err := nats.processVideoForFFmpeg(ctx, service, "720", msg.Data()); err != nil {
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

	godotenv.Load()
	cfg := aws.LoadAwsConifg()
	service := aws.NewS3Service(cfg)

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
				if err := nats.processVideoForFFmpeg(ctx, service, "240", msg.Data()); err != nil {
					log.Println("240 processing failed:", err)
					msg.Nak()
					continue
				}

				msg.Ack()
			}
		}
	}
}

// HELPER FUNCTION
func (nats *Nats) downloadRawVideo(ctx context.Context, awsService *aws.S3Service, eventData []byte, videoQuality string) error {
	log.Println("downloading raw video from s3 bucket...")
	var payload models.Payload
	if err := json.Unmarshal(eventData, &payload); err != nil {
		return fmt.Errorf("json unmarshal error: %v", err)
	}

	url, err := awsService.FetchGetPresignedURL(ctx, payload.Bucket, payload.Key)
	if err != nil {
		return fmt.Errorf("(%s) error fetching presigned url of given file over bucket (%s): error: %v", payload.Key, payload.Bucket, err)
	}

	// 1. download the video using http.get
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("(%s) failed to download video: status %d", payload.Key, resp.StatusCode)
	}
	log.Printf("(%s) downloaded video from s3 bucket successfully\n", payload.Key)
	defer resp.Body.Close()

	//2. create local directory and file
	filename := filepath.Base(payload.Key)
	var rawPath string;
	if(videoQuality != "audio"){
		rawPath = filepath.Join(fmt.Sprintf("raw/%sp",videoQuality ), filename);
	} else {
		rawPath = filepath.Join(fmt.Sprintf("raw/%s",videoQuality ), filename);
	}

	if err := os.MkdirAll(filepath.Dir(rawPath), 0755); err != nil {
		return err
	}

	out, err := os.Create(rawPath)
	if err != nil {
		return err
	}

	defer out.Close()
	log.Printf("(%s) created local file successfully\n", payload.Key)

	// 3. copy the downloaded content into local file
	log.Printf("(%s) copying the file locally...\n", payload.Key)
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	log.Printf("(%s) key video saved locally\n", payload.Key)
	return nil;
}

func (nats *Nats) processVideoForFFmpeg(ctx context.Context, awsService *aws.S3Service, videoQuality string, eventData []byte) error {
	log.Printf("processing ffmpeg %sp event...\n", videoQuality)
	quality, err := strconv.Atoi(videoQuality)
	if err != nil {
		return err
	}

	// 0. download the video from s3 using presigned url
	if err := nats.downloadRawVideo(ctx, awsService, eventData, videoQuality); err != nil {
		return err
	}

	var payload models.Payload
	if err := json.Unmarshal(eventData, &payload); err != nil {
		return fmt.Errorf("json unmarshal error: %v", err)
	}

	log.Printf("(%s) processing video with key %s at %dp\n", videoQuality, payload.Key, quality)

	// 1. locate the raw video path
	filename := filepath.Base(payload.Key)
	rawPath := filepath.Join(fmt.Sprintf("raw/%sp",videoQuality ), filename);

	// 2. local HLS output folder
	hlsDir := filepath.Join("processed/tmp/hls", videoQuality+"p")

	if err := os.MkdirAll(hlsDir, 0755); err != nil {
		return err
	}

	indexPath := filepath.Join(hlsDir, "index.m3u8")
	segmentPattern := filepath.Join(hlsDir, "seg_%03d.ts")

	scale := fmt.Sprintf("scale=-2:%d", quality)

	// 3. run ffmpeg command to process the video
	cmd := exec.CommandContext(
		ctx,
		"ffmpeg",
		"-i", rawPath,
		"-vf", scale,
		"-c:v", "libx264",
		"-preset", "medium",
		"-crf", "22",
		"-g", "48",
		"-keyint_min", "48",
		"-sc_threshold", "0",
		"-c:a", "aac",
		"-b:a", "128k",
		"-f", "hls",
		"-hls_time", "6",
		"-hls_playlist_type", "vod",
		"-hls_segment_filename", segmentPattern,
		indexPath,
	)

	b, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("(%s) ffmpeg error: %v, output: %s", videoQuality, err, string(b))
	}
	log.Printf("(%s) HLS generation complete", videoQuality)

	// if err := awsService.UploadDirectory(ctx, payload.Bucket, hlsDir, "processed/"+videoQuality+"p/"); err != nil {
	// 	return err
	// }

	// log.Printf("(%s) uploaded HLS to S3", videoQuality)

	// // publish done event (JetStream)
	// if err := publishDoneEvent(payload.VideoID, videoQuality+"p"); err != nil {
	// 	return err
	// }

	log.Printf("(%s) DONE event published", videoQuality);
	return nil
}



func (nats *Nats)processAudioTranscriptionEvent(ctx context.Context, awsService *aws.S3Service, eventData []byte) error {
	// 0. unmarshal the eventData
	var payload models.Payload
	if err := json.Unmarshal(eventData, &payload); err != nil {
		return err
	}

	if err := nats.downloadRawVideo(ctx, awsService, eventData, "audio"); err != nil{
		return err;
	}

	// 1. create a folder for the output audio mp3
	rawPath := filepath.Join("raw", "audio", filepath.Base(payload.Key));
	outputFolder := "processed/audio"
	base := strings.TrimSuffix(filepath.Base(payload.Key), filepath.Ext(payload.Key))
	outputFilePath := filepath.Join(outputFolder, base+".mp3")
	if err := os.MkdirAll(outputFolder, 0755); err != nil {
		return err
	}
	log.Printf("(%s) created output directory %s successfully\n", payload.Key, outputFolder)
	// 2. extract the audio i.e mp3 format
	cmd := exec.CommandContext(
		ctx,
		"ffmpeg",
		"-i", rawPath,
		"-vn",
		"-map", "a?",
		"-acodec", "libmp3lame",
		"-q:a", "4",
		outputFilePath,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ffmpeg failed: %v\noutput: %s", err, string(out))
	}
	log.Printf("(%s) audio extracted from the video", payload.Key)

	// 3. genrate the audio transcript i.e. text format
	info, err := os.Stat(outputFilePath)
	if err != nil || info.Size() == 0 {
		log.Printf("(%s) no audio stream found, skipping transcription", payload.Key)
		return nil
	}
	audioBytes, err := os.ReadFile(outputFilePath)
	if err != nil {
		return err
	}
	transcript, err := ai.GenrateTextFromAudio(ctx, audioBytes)
	if err != nil {
		return err
	}
	log.Printf("(%s) audio transcript generated: %s", payload.Key, transcript)

	// 4. save the transcript to a txt file
	txtPath := filepath.Join(outputFolder, base+".txt")
	if err := os.WriteFile(txtPath, []byte(transcript), 0644); err != nil {
		return err
	}

	// 4. TODO: will figure out
	return nil
}