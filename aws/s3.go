package aws

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func LoadAwsConifg() aws.Config {
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("ap-south-1"))
	if err != nil {
		log.Fatalf("AWS config error: %v", err)
	}
	return cfg
}

type S3Service struct {
	client *s3.Client
}

func NewS3Service(cfg aws.Config) *S3Service {
	return &S3Service{
		client: s3.NewFromConfig(cfg),
	}
}

// UploadFile uploads a file to the specified S3 bucket with the given key
// without presigned URL
func (s *S3Service) UploadFile(ctx context.Context, bucketName, key string, body io.Reader) (error) {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body: body,
	})
	return err
}

func (s *S3Service) GetFile(ctx context.Context, bucketName, key string) (io.ReadCloser, error) {
	output, err := s.client.GetObject(ctx,&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
        return nil, err
    }

	return output.Body, nil
}

func (s *S3Service) FetchGetPresignedURL(ctx context.Context, bucketName, key string) (string, error) {
	preSignedClieint := s3.NewPresignClient(s.client)

	req, err := preSignedClieint.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(10*time.Minute))
	if err != nil {
		return "", err
	}

	return req.URL, nil
}

func (s *S3Service) FetchUploadPresignedURL(ctx context.Context, bucketName, key string) (string, error) {
	preSignedClient := s3.NewPresignClient(s.client)

	req, err := preSignedClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(10*time.Minute))
	if err != nil {
		return "", err
	}

	return req.URL, nil
}
