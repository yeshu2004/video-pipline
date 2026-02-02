<img width="1159" height="524" alt="Screenshot 2026-02-02 at 11 56 17 PM" src="https://github.com/user-attachments/assets/b52c4797-745a-4dff-8376-f5d00dfdf607" />

# Plan: Adaptive Bitrate Streaming
1. upload to blob storage using singned url
2. after the video is upload then create event.
3. publish event into pubsub/fanout queue
4. create multiple workers like:
    - transcode_240
   - transcode_480
   - transcode_720
   - transcript
5. airflow/step functions or other tools for :
   - Waits for all tasks
   - Generates master playlist
   - Marks video READY
6. warm CDN cache
7. deliver to user based on bandwidth

