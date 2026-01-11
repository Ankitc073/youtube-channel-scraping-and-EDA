from youtube_transcript_api import YouTubeTranscriptApi
import pandas as pd
import time
import random
import sys
sys.stdout.reconfigure(encoding="utf-8")

ytt_api = YouTubeTranscriptApi()                                                # Initialize the API client once
transcript_list = []

df=pd.read_csv("youtube scraping\channel_videos.csv")
all_video_ids=df['video_id'].tolist()

for v_id in all_video_ids:
    try:
        
        fetched_transcript = ytt_api.fetch(v_id, languages=['en', 'hi'])        # Priority: English first ('en'), then Hindi ('hi') if 'en' is not present    

        full_text = " ".join([snippet.text for snippet in fetched_transcript])

        transcript_list.append({
            'video_id': v_id,
            'transcript': full_text,
            'language': fetched_transcript.language_code                        # metadata
        })

        print(f"Extracted: {v_id}")

        time.sleep(random.uniform(5, 15))

    except Exception as e:
        print(f"Skipping {v_id}: Transcript not available.")
        print(e)
        

df_transcripts = pd.DataFrame(transcript_list)
df_transcripts.to_csv("youtube_video_transcript.csv", index=False)
