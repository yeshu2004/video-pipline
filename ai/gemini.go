package ai

import (
	"context"

	"github.com/joho/godotenv"
	"google.golang.org/genai"
)

func GenrateTextFromAudio(ctx context.Context, audioBytes []byte) ([]byte, error){
	client, err := geminiCleint(ctx)
	if err != nil{
		return nil, err
	}
	res, err := client.Models.GenerateContent(ctx, "gemini-3-flash-preview", []*genai.Content{
		{
			Parts: []*genai.Part{
				genai.NewPartFromBytes(audioBytes, "audio/mpeg"),
				genai.NewPartFromText("Transcribe this audio accurately."),
			},
		},
	}, nil);
	if err != nil{
		return nil, err;
	}
	return []byte(res.Text()), nil
}

func geminiCleint(ctx context.Context) (*genai.Client, error) {
	if err := godotenv.Load(); err != nil {
		return nil, err
	}

	client, err := genai.NewClient(ctx, nil)
	return client, err
}
