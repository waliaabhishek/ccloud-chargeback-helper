#!/bin/sh
set -eu

usage() {
    echo "usage: encode.sh MEDIA_ROOT RAW_VIDEO CAPTIONS DASHBOARD VIDEO POSTER MARKER_COLOR MARKER_AVERAGE_TOLERANCE MARKER_WITHIN_PLANE_SPREAD SPEED VIDEO_WIDTH CONTENT_HEIGHT CAPTION_BAND_HEIGHT POSTER_WIDTH POSTER_HEIGHT CAPTION_FONT_SIZE MIN_SECONDS MAX_SECONDS" >&2
}

fail() {
    echo "$1" >&2
    exit 1
}

if [ "$#" -ne 18 ]; then
    usage
    exit 2
fi

media_root=$1
shift
raw_video_relative=$1
shift
captions_relative=$1
shift
dashboard_relative=$1
shift
video_relative=$1
shift
poster_relative=$1
shift
marker_color=$1
shift
marker_average_tolerance=$1
shift
marker_within_plane_spread=$1
shift
speed=$1
shift
video_width=$1
shift
content_height=$1
shift
caption_band_height=$1
shift
poster_width=$1
shift
poster_height=$1
shift
caption_font_size=$1
shift
minimum_seconds=$1
shift
maximum_seconds=$1

[ -n "$media_root" ] || fail "media root is empty"

case "$marker_color" in
    '#00ff00') ;;
    *) fail "encoder marker color is not the approved #00ff00" ;;
esac
case "$marker_average_tolerance" in
    8) ;;
    *) fail "encoder marker average tolerance is not the approved 8" ;;
esac
case "$marker_within_plane_spread" in
    12) ;;
    *) fail "encoder marker within-plane spread is not the approved 12" ;;
esac
case "$video_width:$content_height:$caption_band_height:$poster_width:$poster_height:$caption_font_size" in
    1600:800:100:960:540:32) ;;
    *) fail "encoder video geometry is not the approved 1600x800 content, 100-pixel band, 960x540 poster, and 32-pixel captions" ;;
esac

case "$minimum_seconds:$maximum_seconds:$video_relative" in
    0:90:assets/chitragupta-demo-walkthrough.mp4)
        mode=full
        ;;
    15:20:review/chitragupta-demo-investigation-draft.mp4)
        mode=draft
        ;;
    *)
        fail "encoder mode, duration bounds, or output path is not approved"
        ;;
esac

case "$mode:$speed" in
    full:1.4375|draft:1.15) ;;
    full:*|draft:*) fail "encoder speed is not the approved 1.4375 for full or 1.15 for draft" ;;
    *) fail "encoder mode is not approved" ;;
esac

resolve_media_path() {
    relative_path=$1
    case "$relative_path" in
        ""|/*|.|..|./*|*/./*|*/.|../*|*/../*|*/..)
            fail "media path must be relative and stay beneath MEDIA_ROOT: $relative_path"
            ;;
    esac
    printf '%s/%s' "$media_root" "$relative_path"
}

raw_video=$(resolve_media_path "$raw_video_relative")
captions=$(resolve_media_path "$captions_relative")
dashboard=$(resolve_media_path "$dashboard_relative")
video=$(resolve_media_path "$video_relative")
poster=$(resolve_media_path "$poster_relative")
timeline=$media_root/work/edit-timeline.json
work_dir=$media_root/work

[ "$raw_video_relative" = "work/chitragupta-demo-walkthrough.webm" ] || fail "raw video path is not canonical"
[ "$captions_relative" = "work/captions.srt" ] || fail "caption path is not canonical"
[ "$dashboard_relative" = "assets/chitragupta-demo-dashboard.png" ] || fail "dashboard path is not canonical"
[ "$poster_relative" = "assets/chitragupta-demo-dashboard-poster.webp" ] || fail "poster path is not canonical"

[ -f "$raw_video" ] || fail "raw browser recording is missing: $raw_video"
[ -f "$captions" ] || fail "caption file is missing: $captions"
[ -f "$dashboard" ] || fail "dashboard screenshot is missing: $dashboard"
[ -f "$timeline" ] || fail "edit timeline is missing: $timeline"

command -v ffmpeg >/dev/null 2>&1 || fail "ffmpeg is unavailable in the media encoder"
command -v ffprobe >/dev/null 2>&1 || fail "ffprobe is unavailable in the media encoder"
command -v sha256sum >/dev/null 2>&1 || fail "sha256sum is unavailable in the media encoder"

mkdir -p "$work_dir" "$(dirname "$video")" "$(dirname "$poster")"

temporary_dir=$(mktemp -d "$work_dir/.encode.XXXXXX") || fail "unable to create encoder temporary directory"
cleanup() {
    rm -rf "$temporary_dir"
}
trap cleanup EXIT HUP INT TERM

raw_width=$(ffprobe -v error -select_streams v:0 -show_entries stream=width -of default=nw=1:nk=1 "$raw_video") ||
    fail "unable to probe raw browser recording width"
raw_height=$(ffprobe -v error -select_streams v:0 -show_entries stream=height -of default=nw=1:nk=1 "$raw_video") ||
    fail "unable to probe raw browser recording height"
raw_frame_rate_value=$(ffprobe -v error -select_streams v:0 -show_entries stream=avg_frame_rate -of default=nw=1:nk=1 "$raw_video") ||
    fail "unable to probe raw browser recording frame rate"
raw_dimensions=$raw_width"x"$raw_height
expected_raw_dimensions=$video_width"x"$content_height
[ "$raw_dimensions" = "$expected_raw_dimensions" ] || fail "raw video dimensions are not $expected_raw_dimensions: $raw_dimensions"

raw_frame_rate=$(printf '%s\n' "$raw_frame_rate_value" | awk -F/ '
    NF == 2 && $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ && ($1 + 0) > 0 && ($2 + 0) > 0 {
        rate = ($1 + 0) / ($2 + 0)
        if (rate > 0) {
            printf "%.6f\n", rate
        }
    }
' | awk 'NR == 1 { print; found = 1 } END { exit found ? 0 : 1 }') ||
    fail "raw video frame rate is invalid: $raw_frame_rate_value"

marker_minimum_frames=$(awk -v rate="$raw_frame_rate" 'BEGIN {
    frames = int(0.3 * rate + 0.999999)
    if (frames < 6) frames = 6
    print frames
}')
marker_maximum_frames=$(awk -v rate="$raw_frame_rate" 'BEGIN {
    print int(0.5 * rate + 0.000001)
}')
[ "$marker_maximum_frames" -ge "$marker_minimum_frames" ] || fail "raw video frame rate is too low for marker bounds: $raw_frame_rate_value"

reference_metadata=$temporary_dir/reference-metadata.txt
ffmpeg -hide_banner -loglevel error -f lavfi \
    -i "color=c=$marker_color:s=$video_width""x""$content_height:r=$raw_frame_rate:d=1" \
    -vf 'format=yuv420p,signalstats,metadata=mode=print:file=-' \
    -frames:v 1 -f null - >"$reference_metadata" \
    || fail "unable to generate the approved marker reference"

reference_values=$(awk -F= '
    /lavfi\.signalstats\.YAVG=/ { y = $2 }
    /lavfi\.signalstats\.UAVG=/ { u = $2 }
    /lavfi\.signalstats\.VAVG=/ { v = $2 }
    END {
        if (y == "" || u == "" || v == "") {
            exit 1
        }
        print y, u, v
    }
' "$reference_metadata") || fail "marker reference statistics are incomplete"
set -- $reference_values
[ "$#" -eq 3 ] || fail "marker reference statistics are malformed"
reference_y=$1
reference_u=$2
reference_v=$3

frame_metadata=$temporary_dir/raw-metadata.txt
ffmpeg -hide_banner -loglevel error -i "$raw_video" -map 0:v:0 \
    -vf 'format=yuv420p,signalstats,metadata=mode=print:file=-' \
    -an -f null - >"$frame_metadata" \
    || fail "unable to inspect raw browser recording marker frames"

marker_runs=$temporary_dir/marker-runs.txt
awk -F= \
    -v reference_y="$reference_y" \
    -v reference_u="$reference_u" \
    -v reference_v="$reference_v" \
    -v average_tolerance="$marker_average_tolerance" \
    -v plane_spread="$marker_within_plane_spread" '
    function absolute(value) {
        return value < 0 ? -value : value
    }
    function emit_run() {
        printf "%d %d %d\n", run_start, run_end, run_length
    }
    function inspect_frame(    is_marker) {
        if (!frame_complete) {
            return
        }
        is_marker = absolute(y_average - reference_y) <= average_tolerance &&
            absolute(u_average - reference_u) <= average_tolerance &&
            absolute(v_average - reference_v) <= average_tolerance &&
            y_maximum - y_minimum <= plane_spread &&
            u_maximum - u_minimum <= plane_spread &&
            v_maximum - v_minimum <= plane_spread
        if (is_marker) {
            if (run_active && frame_number == previous_frame + 1) {
                run_end = frame_number
                run_length++
            } else {
                if (run_active) {
                    emit_run()
                }
                run_start = frame_number
                run_end = frame_number
                run_length = 1
                run_active = 1
            }
        } else if (run_active) {
            emit_run()
            run_active = 0
        }
        previous_frame = frame_number
    }
    /^frame:/ {
        if (frame_seen) {
            inspect_frame()
        }
        frame_number = $1
        sub(/^frame:/, "", frame_number)
        frame_number += 0
        frame_seen = 1
        frame_complete = 0
        y_average = u_average = v_average = ""
        y_minimum = y_maximum = u_minimum = u_maximum = v_minimum = v_maximum = ""
        next
    }
    /lavfi\.signalstats\.YAVG=/ { y_average = $2; next }
    /lavfi\.signalstats\.UAVG=/ { u_average = $2; next }
    /lavfi\.signalstats\.VAVG=/ { v_average = $2; next }
    /lavfi\.signalstats\.YMIN=/ { y_minimum = $2; next }
    /lavfi\.signalstats\.YMAX=/ { y_maximum = $2; next }
    /lavfi\.signalstats\.UMIN=/ { u_minimum = $2; next }
    /lavfi\.signalstats\.UMAX=/ { u_maximum = $2; next }
    /lavfi\.signalstats\.VMIN=/ { v_minimum = $2; next }
    /lavfi\.signalstats\.VMAX=/ {
        v_maximum = $2
        frame_complete = 1
        next
    }
    END {
        if (frame_seen) {
            inspect_frame()
        }
        if (run_active) {
            emit_run()
        }
    }
' "$frame_metadata" >"$marker_runs"

run_count=$(awk 'END { print NR + 0 }' "$marker_runs")
[ "$run_count" = 2 ] || fail "expected exactly two approved marker runs, found $run_count"
if awk -v minimum="$marker_minimum_frames" -v maximum="$marker_maximum_frames" '$3 < minimum || $3 > maximum { found = 1 } END { exit found ? 0 : 1 }' "$marker_runs"; then
    fail "marker run is outside the approved 0.3-0.5 second range"
fi

first_marker_end=$(awk 'NR == 1 { print $2 }' "$marker_runs")
second_marker_start=$(awk 'NR == 2 { print $1 }' "$marker_runs")
first_marker_frames=$(awk 'NR == 1 { print $3 }' "$marker_runs")
second_marker_frames=$(awk 'NR == 2 { print $3 }' "$marker_runs")
story_first_frame=$((first_marker_end + 1))
story_last_frame=$((second_marker_start - 1))
[ "$story_first_frame" -le "$story_last_frame" ] || fail "marker runs leave no story frames to encode"

temporary_video=$temporary_dir/encoded.mp4
video_filter="select='between(n,$story_first_frame,$story_last_frame)',setpts=(PTS-STARTPTS)/$speed,pad=$video_width:$content_height+$caption_band_height:0:0:black,subtitles=$captions:force_style='PlayResX=1600,PlayResY=900,FontName=DejaVu Sans,FontSize=$caption_font_size,Outline=2,Shadow=1,MarginV=32,Alignment=2',format=yuv420p"
ffmpeg -hide_banner -loglevel error -y -i "$raw_video" -map 0:v:0 \
    -vf "$video_filter" -r 30 -c:v libx264 -profile:v high -pix_fmt yuv420p \
    -movflags +faststart -an "$temporary_video" \
    || fail "unable to encode the marker-trimmed walkthrough"

temporary_poster=$temporary_dir/poster.webp
ffmpeg -hide_banner -loglevel error -y -i "$dashboard" \
    -vf "scale=$poster_width:$poster_height:flags=lanczos" -frames:v 1 \
    -c:v libwebp -lossless 0 -q:v 80 -an "$temporary_poster" \
    || fail "unable to create the dashboard poster"

duration=$(ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "$temporary_video") ||
    fail "unable to probe encoded video duration"
codec=$(ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of default=nw=1:nk=1 "$temporary_video") ||
    fail "unable to probe encoded video codec"
encoded_dimensions=$(ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0:s=x "$temporary_video") ||
    fail "unable to probe encoded video dimensions"
encoded_frame_rate=$(ffprobe -v error -select_streams v:0 -show_entries stream=avg_frame_rate -of default=nw=1:nk=1 "$temporary_video") ||
    fail "unable to probe encoded video frame rate"
audio_streams=$(ffprobe -v error -select_streams a -show_entries stream=index -of csv=p=0 "$temporary_video") ||
    fail "unable to probe encoded video audio streams"
audio_stream_count=$(printf '%s\n' "$audio_streams" | awk 'NF { count++ } END { print count + 0 }')
poster_dimensions=$(ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0:s=x "$temporary_poster") ||
    fail "unable to probe dashboard poster dimensions"

[ "$codec" = h264 ] || fail "encoded video codec is not h264: $codec"
expected_output_dimensions=$video_width"x"$((content_height + caption_band_height))
[ "$encoded_dimensions" = "$expected_output_dimensions" ] || fail "encoded video dimensions are not $expected_output_dimensions: $encoded_dimensions"
case "$encoded_frame_rate" in
    30/1|30000/1001) ;;
    *) fail "encoded video frame rate is not 30: $encoded_frame_rate" ;;
esac
[ "$audio_stream_count" -eq 0 ] || fail "encoded video unexpectedly contains audio"
expected_poster_dimensions=$poster_width"x"$poster_height
[ "$poster_dimensions" = "$expected_poster_dimensions" ] || fail "dashboard poster dimensions are not $expected_poster_dimensions: $poster_dimensions"

awk -v duration="$duration" -v minimum="$minimum_seconds" -v maximum="$maximum_seconds" \
    'BEGIN {
        if ((duration + 0) < minimum || (duration + 0) > maximum) {
            exit 1
        }
    }' || fail "encoded video duration is outside $minimum_seconds-$maximum_seconds seconds: $duration"

# The browser recorder's raw frame rate is runtime-dependent (Playwright is
# 25 fps in the production image). Replace the capture's planned marker
# counts with the frame-derived evidence before hashing the timeline. Keep the
# same JSON shape and replace only the two marker runs, atomically.
updated_timeline=$temporary_dir/edit-timeline.json
awk -v first_frames="$first_marker_frames" \
    -v second_frames="$second_marker_frames" \
    -v rate="$raw_frame_rate" '
    function marker_duration(frames) {
        return sprintf("%.6f", frames / rate)
    }
    /"runs"[[:space:]]*:[[:space:]]*\[/ {
        in_marker_runs = 1
        run_number = 0
    }
    in_marker_runs && /"duration_seconds"[[:space:]]*:/ {
        run_number++
        frames = run_number == 1 ? first_frames : second_frames
        sub(/"duration_seconds"[[:space:]]*:[[:space:]]*[0-9]+(\.[0-9]+)?/, "\"duration_seconds\": " marker_duration(frames))
        duration_replacements++
    }
    in_marker_runs && /"frames"[[:space:]]*:/ {
        frames = run_number == 1 ? first_frames : second_frames
        sub(/"frames"[[:space:]]*:[[:space:]]*[0-9]+/, "\"frames\": " frames)
        frame_replacements++
        if (run_number == 2) in_marker_runs = 0
    }
    { print }
    END {
        if (duration_replacements != 2 || frame_replacements != 2) exit 1
    }
' "$timeline" >"$updated_timeline" || fail "unable to reconcile marker evidence in edit timeline"
mv "$updated_timeline" "$timeline" || fail "unable to atomically update edit timeline marker evidence"

timeline_sha256=$(sha256sum "$timeline") || fail "unable to hash edit timeline"
timeline_sha256=$(printf '%s\n' "$timeline_sha256" | awk '{ print $1 }')
if ! printf '%s\n' "$timeline_sha256" | awk 'length($0) == 64 && $0 !~ /[^0-9a-f]/ { found = 1 } END { exit found ? 0 : 1 }'; then
    fail "edit timeline hash is malformed"
fi

temporary_result=$temporary_dir/encoder-result.json
printf '%s\n' \
    '{"duration_seconds":'"$duration"',"video_codec":"'"$codec"'","width":'"$video_width"',"height":'"$((content_height + caption_band_height))"',"frame_rate":30,"audio_stream_count":'"$audio_stream_count"',"captions_burned_in":true,"caption_filter":"subtitles","caption_source":"captions.srt","webm_removed":true,"story_first_frame":'"$story_first_frame"',"story_last_frame":'"$story_last_frame"',"raw_frame_rate":'"$raw_frame_rate"',"mode":"'"$mode"'","speed":'"$speed"',"edited_duration_seconds":'"$duration"',"timeline_sha256":"'"$timeline_sha256"'"}' \
    >"$temporary_result"

rm -f "$raw_video"
mv "$temporary_video" "$video"
mv "$temporary_poster" "$poster"
mv "$temporary_result" "$work_dir/encoder-result.json"
