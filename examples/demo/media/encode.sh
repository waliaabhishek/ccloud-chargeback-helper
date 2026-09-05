#!/bin/sh
set -eu

if [ "$#" -ne 12 ]; then
    echo "usage: encode.sh MEDIA_ROOT RAW_VIDEO CAPTIONS DASHBOARD VIDEO POSTER VIDEO_WIDTH VIDEO_HEIGHT POSTER_WIDTH POSTER_HEIGHT MIN_SECONDS MAX_SECONDS" >&2
    exit 2
fi

media_root=$1
raw_video_relative=$2
captions_relative=$3
dashboard_relative=$4
video_relative=$5
poster_relative=$6
video_width=$7
video_height=$8
poster_width=$9
poster_height=${10}
minimum_seconds=${11}
maximum_seconds=${12}

resolve_media_path() {
    relative_path=$1
    case "$relative_path" in
        ""|/*|.|..|./*|*/./*|*/.|../*|*/../*|*/..)
            echo "media path must be relative and stay beneath MEDIA_ROOT: $relative_path" >&2
            exit 2
            ;;
    esac
    printf '%s/%s' "$media_root" "$relative_path"
}

raw_video=$(resolve_media_path "$raw_video_relative")
captions=$(resolve_media_path "$captions_relative")
dashboard=$(resolve_media_path "$dashboard_relative")
video=$(resolve_media_path "$video_relative")
poster=$(resolve_media_path "$poster_relative")
work_dir=$media_root/work
assets_dir=$media_root/assets
encoder_result=$work_dir/encoder-result.json

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
    -i "$dashboard" -vf "scale=${poster_width}:${poster_height}:flags=lanczos" \
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
[ "$width" = "$video_width" ] && [ "$height" = "$video_height" ] || {
    echo "encoded video dimensions are not ${video_width}x${video_height}: ${width}x${height}" >&2
    exit 1
}
[ "$audio_stream_count" -eq 0 ] || {
    echo "encoded video unexpectedly contains audio" >&2
    exit 1
}

awk -v duration="$duration" -v minimum="$minimum_seconds" -v maximum="$maximum_seconds" \
    'BEGIN { exit !(duration >= minimum && duration <= maximum) }' || {
    echo "encoded video duration is outside ${minimum_seconds}-${maximum_seconds} seconds: $duration" >&2
    exit 1
}

rm -f "$raw_video"
printf '%s\n' \
    "{\"duration_seconds\":$duration,\"video_codec\":\"$codec\",\"width\":$width,\"height\":$height,\"frame_rate\":$frame_rate,\"audio_stream_count\":$audio_stream_count,\"captions_burned_in\":true,\"caption_filter\":\"subtitles\",\"caption_source\":\"captions.srt\",\"webm_removed\":true}" \
    > "$encoder_result"
