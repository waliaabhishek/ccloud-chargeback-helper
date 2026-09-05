#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
    echo "usage: encode.sh SPEC_PATH MEDIA_ROOT" >&2
    exit 2
fi

spec_path=$1
media_root=$2
work_dir=$media_root/work
assets_dir=$media_root/assets
raw_video=$work_dir/chitragupta-demo-walkthrough.webm
captions=$work_dir/captions.srt
dashboard=$assets_dir/chitragupta-demo-dashboard.png
video=$assets_dir/chitragupta-demo-walkthrough.mp4
poster=$assets_dir/chitragupta-demo-dashboard-poster.webp
encoder_result=$work_dir/encoder-result.json

[ -f "$spec_path" ] || {
    echo "capture specification is missing: $spec_path" >&2
    exit 1
}
[ -f "$raw_video" ] || {
    echo "raw browser recording is missing: $raw_video" >&2
    exit 1
}
[ -f "$captions" ] || {
    echo "caption file is missing: $captions" >&2
    exit 1
}
[ -f "$dashboard" ] || {
    echo "dashboard screenshot is missing: $dashboard" >&2
    exit 1
}

mkdir -p "$work_dir" "$assets_dir"

ffmpeg -hide_banner -loglevel error -y \
    -i "$raw_video" \
    -vf "subtitles=$captions:force_style='FontName=DejaVu Sans,FontSize=24,Outline=2,Shadow=1,MarginV=40'" \
    -r 30 -c:v libx264 -profile:v high -pix_fmt yuv420p -movflags +faststart -an "$video"

ffmpeg -hide_banner -loglevel error -y \
    -i "$dashboard" -vf "scale=960:540:flags=lanczos" \
    -frames:v 1 -c:v libwebp -lossless 0 -q:v 80 -an "$poster"

duration=$(ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "$video")
codec=$(ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of default=nw=1:nk=1 "$video")
width=$(ffprobe -v error -select_streams v:0 -show_entries stream=width -of default=nw=1:nk=1 "$video")
height=$(ffprobe -v error -select_streams v:0 -show_entries stream=height -of default=nw=1:nk=1 "$video")
frame_rate=$(ffprobe -v error -select_streams v:0 -show_entries stream=avg_frame_rate -of default=nw=1:nk=1 "$video")
audio_stream_count=$(ffprobe -v error -select_streams a -show_entries stream=index -of csv=p=0 "$video" | wc -l | tr -d ' ')

case "$frame_rate" in
    30/1|30000/1001) frame_rate=30 ;;
    *) echo "encoded video frame rate is not 30: $frame_rate" >&2; exit 1 ;;
esac

[ "$codec" = h264 ] || { echo "encoded video codec is not h264: $codec" >&2; exit 1; }
[ "$width" = 1600 ] && [ "$height" = 900 ] || {
    echo "encoded video dimensions are not 1600x900: ${width}x${height}" >&2
    exit 1
}
[ "$audio_stream_count" -eq 0 ] || {
    echo "encoded video unexpectedly contains audio" >&2
    exit 1
}

awk -v duration="$duration" 'BEGIN { exit !(duration >= 60 && duration <= 90) }' || {
    echo "encoded video duration is outside 60-90 seconds: $duration" >&2
    exit 1
}

rm -f "$raw_video"
printf '%s\n' \
    "{\"duration_seconds\":$duration,\"video_codec\":\"$codec\",\"width\":$width,\"height\":$height,\"frame_rate\":$frame_rate,\"audio_stream_count\":$audio_stream_count,\"captions_burned_in\":true,\"caption_filter\":\"subtitles\",\"caption_source\":\"captions.srt\",\"webm_removed\":true}" \
    > "$encoder_result"
