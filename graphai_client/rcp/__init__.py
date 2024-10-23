from graphai_client.utils import  execute_query, status_msg
from graphai_client.client import download_url


def get_video_token_and_codec_types(
        platform, video_id, piper_connection, login_info, force=False, force_download=False, debug=False, sections=()
):
    video_info = execute_query(
        piper_connection,
        f'SELECT videoUrl FROM gen_video.Videos WHERE platform="{platform}" AND videoId="{video_id}";'
    )
    if len(video_info) != 1 or len(video_info[0]) != 1:
        status_msg(
            f'The video {video_id} on {platform} could not be found in gen_video.Videos',
            color='red', sections=list(sections) + ['ERROR']
        )
        return None, None
    video_url = video_info[0][0]
    video_token, video_size, streams = download_url(
        video_url, login_info, force=force, force_download=force_download, debug=debug
    )
    if video_token is None:
        status_msg(
            f'Download of the video {video_id} on {platform}  at {video_url} failed.',
            color='red', sections=list(sections) + ['ERROR']
        )
        return None, None
    codec_types = [s['codec_type'] for s in streams]
    return video_token, codec_types
