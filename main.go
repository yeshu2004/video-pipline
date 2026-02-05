package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/yeshu2004/video-pipline/aws"
	"github.com/yeshu2004/video-pipline/models"
	"github.com/yeshu2004/video-pipline/nats"
)

var (
	uploadBucket string = "video-pipline"
)

type Handler struct {
	awsService *aws.S3Service
	nats       *nats.Nats
}

func NewHandler(awsService *aws.S3Service, nats *nats.Nats) *Handler {
	return &Handler{
		awsService: awsService,
		nats:       nats,
	}
}

func main() {
	godotenv.Load()

	cfg := aws.LoadAwsConifg()
	awsService := aws.NewS3Service(cfg)

	nats, err := nats.NewNATS()
	if err != nil {
		log.Fatalf("%s", err.Error())
	}
	h := NewHandler(awsService, nats)

	gin.SetMode(gin.DebugMode)
	router := gin.Default()

	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://localhost:5173"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))
	router.Use(gin.Logger())

	// 1. client will try to upload the video, so he req signed url to upload
	router.POST("/upload/signed/url", h.signedURLHandler)

	// 2. after the video is upload(if sucessfull i.e client side logic)
	// to aws from client side a cofirmation event comes with keyname
	// to backend for start doing the pipline actions
	// like creating a event into NATS stream
	router.POST("/upload/confirmation", h.videoUplodedHandler)

	router.Run(":8080")
}

func (h *Handler) signedURLHandler(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	key := fmt.Sprintf("raw/%s.mp4", uuid.New().String())
	url, err := h.awsService.FetchUploadPresignedURL(ctx, uploadBucket, key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to generate presigned url:%s", err.Error()),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "retrived signed url sucessfully",
		"key":     key,
		"url":     url,
	})
}

func (h *Handler) videoUplodedHandler(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	var req models.ConfirmRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	payload := &models.Payload{
		Key:       req.Key,
		Bucket:    uploadBucket,
		Timestamp: time.Now(),
	}

	payloadByte, err := json.Marshal(payload)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	h.nats.PublishVideoUploadedEvent(ctx, payload.Key, payloadByte)

	c.JSON(http.StatusOK, gin.H{
		"message": "upload confirmed",
		"payload": payload,
	})
}
