
from googleapiclient.discovery import build
import pandas as pd
from googleapiclient.errors import HttpError
import time

API_KEY = "AIzaSyDW53AmI3tXHFolHfUVKfILZHIPjgKIF-M"
CHANNEL_ID = "UCOtQWL2z-tFbI-mgy_Rpdgg"

youtube = build("youtube", "v3", developerKey=API_KEY)

########## Extracting Channel Info ##########
channel_req = youtube.channels().list(
    part="snippet,statistics,contentDetails",
    id=CHANNEL_ID
)
channel_data = channel_req.execute()["items"][0]
channel_details=[]
data={"Name": channel_data["snippet"]["title"],
"Description": channel_data["snippet"]["description"],
"Subscribers": channel_data["statistics"]["subscriberCount"],
"Channel Published": channel_data["snippet"]["publishedAt"],
"Total Views": channel_data["statistics"]["viewCount"],
"Total Videos": channel_data["statistics"]["videoCount"]}
channel_details.append(data)
channel_details_df=pd.DataFrame(channel_details)
print(channel_details)
channel_details_df.to_csv("channel_details.csv", index=False)

# playlist of all uploads
upload_playlist = channel_data["contentDetails"]["relatedPlaylists"]["uploads"]

########## Extracting all videos ID's ##########

all_video_ids = []
next_page = None

while True:
    try:
        playlist_req = youtube.playlistItems().list(
            part="contentDetails",                                              #  Only request what you need to save bandwidth
            playlistId=upload_playlist,
            maxResults=50,
            pageToken=next_page
        )
        playlist_res = playlist_req.execute()

        # Optimization: Use .get() to avoid KeyError if 'items' is missing
        items = playlist_res.get("items", [])

        for item in items:
            # video_id = item["snippet"]["resourceId"]["videoId"],              # some time, videosId is not present in snippet. so use contentDetails
            # contentDetails is the most reliable place for the ID
            video_id = item.get("contentDetails", {}).get("videoId")
            if video_id:
                all_video_ids.append(video_id)

        print(f"Fetched {len(all_video_ids)} video IDs so far...")

        next_page = playlist_res.get("nextPageToken")
        if not next_page:
            break

    except HttpError as e:
        if e.resp.status in [500, 502, 503, 504]:
            print("Server error, retrying in 5 seconds...")
            time.sleep(5)
            continue
        elif e.resp.status == 403:
            print("Quota exceeded! Stopping extraction.")
            break
        else:
            print(f"An API error occurred: {e}")
            break

print(f"\nTotal videos found: {len(all_video_ids)}")

##########  Extracting the stats of each Videos ##########

videos_detail=[]
for i in range(0, len(all_video_ids), 50):
    batch_ids = ",".join(all_video_ids[i : i + 50])
    try:
        # API Call
        stats_req = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=batch_ids)
        stats_res = stats_req.execute()

        # Extract data from the batch response
        for item in stats_res.get("items", []):
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            content = item.get("contentDetails", {})

            videos_detail.append({
                "video_id": item["id"],
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "published": snippet.get("publishedAt"),
                "views": stats.get("viewCount"),
                "likes": stats.get("likeCount"),
                "comments": stats.get("commentCount"),
                "Category": snippet.get("categoryId"),
                "duration": content.get("duration"),
                "Definition": content.get("definition")
            })

    except HttpError as e:
        if e.resp.status == 403:
            print("\n QUOTA EXCEEDED: You have hit the 10,000 unit limit.")
        else:
            print(f"\n An API error occurred: {e}")
        break # Stop the loop but proceed to save what we have

videos_detail_df = pd.DataFrame(videos_detail)
videos_detail_df.to_csv("channel_videos.csv", index=False)
print("\n Data saved to: channel_videos.csv")

########## Extracting Comments and also using pagination for extracting comments from multiple pages ##########
print("\n DOWNLOADING ALL COMMENTS...")
all_comments = []

for v in all_video_ids:
    next_page = None
    count=0
    while count<2:
        try:
            count+=1
            comment_request = youtube.commentThreads().list(
                part="snippet",
                videoId=v,
                maxResults=100,
                pageToken=next_page,
                textFormat="plainText"                                          # Smaller payload than HTML
            )

            
            response = None
            for attempt in range(5):
                try:
                    response = comment_request.execute()
                    break
                except HttpError as e:
                    if e.resp.status in [500, 502, 503, 504]:
                        wait_time = 2 ** attempt
                        print(f"Server error. Retrying in {wait_time}s...")
                        time.sleep(wait_time)

                    elif e.resp.status == 403:
                        print("Quota exceeded. Saving collected data and stopping.")
                        next_page = None                                        # Force break from inner loop
                        
                    elif e.resp.status == 404:
                        print(f"Video {v} not found or comments disabled.")
                        
                    else:
                        print(f"Error: {e}")
                                                                  

            if not response: break

            for item in response.get("items", []):
                comment = item["snippet"]["topLevelComment"]["snippet"]
                all_comments.append({
                    'video_id': v,
                    'author': comment["authorDisplayName"],
                    'comment': comment["textDisplay"],
                    'likes': comment["likeCount"],
                    'published': comment["publishedAt"]
                })

            next_page = response.get("nextPageToken")                           # using pagination
            if not next_page:
                break

        except HttpError as e:
            print(f"Error: {e}")
            break

comments_df = pd.DataFrame(all_comments)
comments_df.to_csv("youtube_video_comments.csv", index=False)
print("\n Data saved to: youtube_video_comments.csv")

