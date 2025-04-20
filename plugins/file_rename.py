import os
import re
import time
import shutil
import asyncio
import logging
from datetime import datetime, timedelta
from PIL import Image
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import InputMediaDocument, Message
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser
from pyrogram.enums import ParseMode
from plugins.antinsfw import check_anti_nsfw
from helper.utils import progress_for_pyrogram, humanbytes
from helper import convert
from helper.database import DARKXSIDE78
from config import Config
import random
import string
import aiohttp
import pytz
from asyncio import Semaphore
import subprocess
import json

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

queue_tasks: dict[int, asyncio.Task] = {}

renaming_operations = {}
active_sequences = {}
message_ids = {}
flood_control = {}
file_queues = {}
USER_SEMAPHORES = {}
USER_LIMITS = {}
global PREMIUM_MODE, PREMIUM_MODE_EXPIRY
PREMIUM_MODE = Config.GLOBAL_TOKEN_MODE
PREMIUM_MODE_EXPIRY = Config.GLOBAL_TOKEN_MODE

def detect_quality(file_name):
    quality_order = {
        "144p": 1,
        "240p": 2,
        "360p": 3,
        "480p": 4,
        "720p": 5, 
        "1080p": 6,
        "1440p": 7,
        "2160p": 8
        }
    match = re.search(r"(144p|240p|360p|480p|720p|1080p|1440p|2160p)", file_name)
    return quality_order.get(match.group(1), 8) if match else 9

@Client.on_message(filters.command("ssequence") & filters.private)
async def start_sequence(client, message: Message):
    user_id = message.from_user.id
    if user_id in active_sequences:
        await message.reply_text("**A sᴇǫᴜᴇɴᴄᴇ ɪs ᴀʟʀᴇᴀᴅʏ ᴀᴄᴛɪᴠᴇ! Usᴇ /esequence ᴛᴏ ᴇɴᴅ ɪᴛ.**")
    else:
        active_sequences[user_id] = []
        message_ids[user_id] = []
        msg = await message.reply_text("**Sᴇǫᴜᴇɴᴄᴇ ʜᴀs ʙᴇᴇɴ sᴛᴀʀᴛᴇᴅ! Sᴇɴᴅ ʏᴏᴜʀ ғɪʟᴇs...**")
        message_ids[user_id].append(msg.id)

@Client.on_message(filters.command("esequence") & filters.private)
async def end_sequence(client, message: Message):
    user_id = message.from_user.id
    if user_id not in active_sequences:
        await message.reply_text("**Nᴏ ᴀᴄᴛɪᴠᴇ sᴇǫᴜᴇɴᴄᴇ ғᴏᴜɴᴅ!**\n**Aᴄᴛɪᴠᴀᴛᴇ sᴇǫᴜᴇɴᴄᴇ ʙʏ ᴜsɪɴɢ /ssequence**")
        return

    file_list = active_sequences.pop(user_id, [])
    delete_messages = message_ids.pop(user_id, [])

    if not file_list:
        await message.reply_text("**Nᴏ ғɪʟᴇs ᴡᴇʀᴇ sᴇɴᴛ ɪɴ ᴛʜɪs sᴇǫᴜᴇɴᴄᴇ!**")
        return

    def sorting_key(f):
        file_name = f.get("file_name", "")
        season, episode = extract_season_episode(file_name)
        quality = extract_quality(file_name)
        
        quality = quality.lower().replace(" ", "")
        if quality.isdigit():
            quality += "p"
        
        quality_order = {
            "144p": 1,
            "240p": 2,
            "360p": 3,
            "480p": 4,
            "720p": 5, 
            "1080p": 6,
            "1440p": 7,
            "2160p": 8
        }
        quality_priority = quality_order.get(quality, 9)
        
        try:
            season_str = str(season).upper().replace('S', '').strip()
            season_num = int(season_str) if season_str.isdigit() else 0

            episode_str = str(episode).strip()
            episode_num = int(episode_str) if episode_str.isdigit() else 0
            padded_episode = f"{episode_num:02d}"
        except Exception:
            season_num = 0
            padded_episode = "00"
        
        return (season_num, quality_priority, padded_episode, file_name)

    sorted_files = sorted(file_list, key=sorting_key)

    await message.reply_text(f"**Sᴇǫᴜᴇɴᴄᴇ ᴇɴᴅᴇᴅ! Sᴇɴᴅɪɴɢ {len(sorted_files)} ғɪʟᴇs ʙᴀᴄᴋ...**")


    for index, file in enumerate(sorted_files):
        try:
            await client.send_document(
                message.chat.id,
                file["file_id"],
                caption=f"**{file.get('file_name', '')}**"
            )
            
            if index < len(sorted_files) - 1:
                await asyncio.sleep(0.5)
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            logger.error(f"Error sending file: {e}")

    try:
        await client.delete_messages(chat_id=message.chat.id, message_ids=delete_messages)
    except Exception as e:
        logger.error(f"Error deleting messages: {e}")

@Client.on_message(filters.command("premium") & filters.private)
async def global_premium_control(client, message: Message):
    global PREMIUM_MODE, PREMIUM_MODE_EXPIRY

    user_id = message.from_user.id
    if user_id not in Config.ADMIN:
        return await message.reply_text("**Tʜɪs ᴄᴏᴍᴍᴀɴᴅ ɪs ʀᴇsᴛʀɪᴄᴛᴇᴅ ᴛᴏ ᴀᴅᴍɪɴs ᴏɴʟʏ!!!**")

    args = message.command[1:]
    if not args:
        status = "ON" if PREMIUM_MODE else "OFF"
        expiry = f" (expires {PREMIUM_MODE_EXPIRY:%Y-%m-%d %H:%M})" if PREMIUM_MODE_EXPIRY else ""
        return await message.reply_text(
            f"**➠ Cᴜʀʀᴇɴᴛ Pʀᴇᴍɪᴜᴍ Mᴏᴅᴇ: {status}{expiry}**\n\n"
            "**Usᴀɢᴇ:\n**"
            "**/premium on [days]  — ᴅɪsᴀʙʟᴇ ᴛᴏᴋᴇɴ ᴜsᴀɢᴇ\n**"
            "*/premium off [days] — ʀᴇ-ᴇɴᴀʙʟᴇ ᴛᴏᴋᴇɴ ᴜsᴀɢᴇ**"
        )

    action = args[0].lower()
    if action not in ("on", "off"):
        return await message.reply_text("**Iɴᴠᴀʟɪᴅ ᴀᴄᴛɪᴏɴ! Usᴇ `on` ᴏʀ `off`**")

    days = int(args[1]) if len(args) > 1 and args[1].isdigit() else None
    if action == "on":
        PREMIUM_MODE = False
        PREMIUM_MODE_EXPIRY = datetime.now() + timedelta(days=days) if days else None
        msg = f"**Tᴏᴋᴇɴ ᴜsᴀɢᴇ ʜᴀs ʙᴇᴇɴ Dɪsᴀʙʟᴇᴅ{f' ғᴏʀ {days} ᴅᴀʏs' if days else ''}**"
    else:
        PREMIUM_MODE = True
        PREMIUM_MODE_EXPIRY = datetime.now() + timedelta(days=days) if days else None
        msg = f"**Tᴏᴋᴇɴ ᴜsᴀɢᴇ ʜᴀs ʙᴇᴇɴ Eɴᴀʙʟᴇᴅ{f' ғᴏʀ {days} ᴅᴀʏs' if days else ''}**"

    # persist
    await DARKXSIDE78.global_settings.update_one(
        {"_id": "premium_mode"},
        {"$set": {"status": PREMIUM_MODE, "expiry": PREMIUM_MODE_EXPIRY}},
        upsert=True
    )
    await message.reply_text(msg)

async def check_premium_mode():
    global PREMIUM_MODE, PREMIUM_MODE_EXPIRY

    settings = await DARKXSIDE78.global_settings.find_one({"_id": "premium_mode"})
    if not settings:
        return

    PREMIUM_MODE        = settings.get("status", True)
    PREMIUM_MODE_EXPIRY = settings.get("expiry", None)

    if PREMIUM_MODE_EXPIRY and datetime.now() > PREMIUM_MODE_EXPIRY:
        PREMIUM_MODE = True
        await DARKXSIDE78.global_settings.update_one(
            {"_id": "premium_mode"},
            {"$set": {"status": PREMIUM_MODE}}
        )

SEASON_EPISODE_PATTERNS = [
    (re.compile(r'S(\d+)\s+(\d{3,4}p?)\b'), ('season', None)), 
    (re.compile(r'S(\d+)(?:E|EP)(\d+)'), ('season', 'episode')),
    (re.compile(r'S(\d+)[\s-]*(?:E|EP)(\d+)'), ('season', 'episode')),
    (re.compile(r'Season\s*(\d+)\s*Episode\s*(\d+)', re.IGNORECASE), ('season', 'episode')),
    (re.compile(r'\[S(\d+)\]\[E(\d+)\]'), ('season', 'episode')),
    (re.compile(r'S(\d+)[^\d]*(\d+)'), ('season', 'episode')),
    (re.compile(r'(?:E|EP|Episode)\s*(\d+)', re.IGNORECASE), (None, 'episode')),
    (re.compile(r'\b(\d+)\b'), (None, 'episode'))
]

QUALITY_PATTERNS = [
    (re.compile(r'\b(S\d+\s*)?(\d{3,4})p?\b'), lambda m: f"{m.group(2)}p"),
    (re.compile(r'\b(4k|2160p)\b', re.IGNORECASE), lambda m: "2160p"),
    (re.compile(r'\b(2k|1440p)\b', re.IGNORECASE), lambda m: "1440p"),
    (re.compile(r'\b(\d{3,4}[pi])\b', re.IGNORECASE), lambda m: m.group(1)),
    (re.compile(r'\b(4k|2160p)\b', re.IGNORECASE), lambda m: "4k"),
    (re.compile(r'\b(2k|1440p)\b', re.IGNORECASE), lambda m: "2k"),
    (re.compile(r'\b(HDRip|HDTV)\b', re.IGNORECASE), lambda m: m.group(1)),
    (re.compile(r'\b(4kX264|4kx265)\b', re.IGNORECASE), lambda m: m.group(1)),
    (re.compile(r'\[(\d{3,4}[pi])\]', re.IGNORECASE), lambda m: m.group(1))
]

def extract_season_episode(filename):
    for pattern, (season_group, episode_group) in SEASON_EPISODE_PATTERNS:
        match = pattern.search(filename)
        if match:
            if season_group is not None:
                season = match.group(1)
                episode = match.group(2)
            else:
                season = "1"
                episode = match.group(1)
            logger.info(f"Extracted season: {season}, episode: {episode} from {filename}")
            return season, episode
    logger.warning(f"No season/episode pattern matched for {filename}")
    return "1", None

def extract_quality(filename):
    for pattern, extractor in QUALITY_PATTERNS:
        match = pattern.search(filename)
        if match:
            quality = extractor(match)
            logger.info(f"Extracted quality: {quality} from {filename}")
            return quality
    logger.warning(f"No quality pattern matched for {filename}")
    return "Unknown"

async def detect_audio_info(file_path):
    ffprobe = shutil.which('ffprobe')
    if not ffprobe:
        raise RuntimeError("ffprobe not found in PATH")

    cmd = [
        ffprobe,
        '-v', 'quiet',
        '-print_format', 'json',
        '-show_streams',
        file_path
    ]

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()

    try:
        info = json.loads(stdout)
        streams = info.get('streams', [])
        
        audio_streams = [s for s in streams if s.get('codec_type') == 'audio']
        sub_streams = [s for s in streams if s.get('codec_type') == 'subtitle']

        japanese_audio = 0
        english_audio = 0
        for audio in audio_streams:
            lang = audio.get('tags', {}).get('language', '').lower()
            if lang in {'ja', 'jpn', 'japanese'}:
                japanese_audio += 1
            elif lang in {'en', 'eng', 'english'}:
                english_audio += 1

        english_subs = len([
            s for s in sub_streams 
            if s.get('tags', {}).get('language', '').lower() in {'en', 'eng', 'english'}
        ])

        return len(audio_streams), len(sub_streams), japanese_audio, english_audio, english_subs
    except Exception as e:
        logger.error(f"Audio detection error: {e}")
        return 0, 0, 0, 0, 0

def get_audio_label(audio_info):
    audio_count, sub_count, jp_audio, en_audio, en_subs = audio_info
    
    if audio_count == 1:
        if jp_audio >= 1 and en_subs >= 1:
            return "Sub" + ("s" if sub_count > 1 else "")
        if en_audio >= 1:
            return "Dub"
    
    if audio_count == 2:
        return "Dual"
    elif audio_count == 3:
        return "Tri"
    elif audio_count >= 4:
        return "Multi"
    
    return "Unknown"

async def cleanup_files(*paths):
    for path in paths:
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except Exception as e:
            logger.error(f"Error removing {path}: {e}")

async def process_thumbnail(thumb_path):
    if not thumb_path or not os.path.exists(thumb_path):
        return None
    try:
        with Image.open(thumb_path) as img:
            img = img.convert("RGB").resize((320, 320))
            img.save(thumb_path, "JPEG")
        return thumb_path
    except Exception as e:
        logger.error(f"Thumbnail processing failed: {e}")
        await cleanup_files(thumb_path)
        return None

async def add_metadata(input_path, output_path, user_id):
    ffmpeg = shutil.which('ffmpeg')
    if not ffmpeg:
        raise RuntimeError("FFmpeg not found in PATH")

    metadata = {
        'title': await DARKXSIDE78.get_title(user_id),
        'artist': await DARKXSIDE78.get_artist(user_id),
        'author': await DARKXSIDE78.get_author(user_id),
        'video_title': await DARKXSIDE78.get_video(user_id),
        'audio_title': await DARKXSIDE78.get_audio(user_id),
        'subtitle': await DARKXSIDE78.get_subtitle(user_id),
        'encoded_by': await DARKXSIDE78.get_encoded_by(user_id),
        'custom_tag': await DARKXSIDE78.get_custom_tag(user_id)
    }

    cmd = [ffmpeg,
        '-i', input_path,
        '-metadata', f'title={metadata["title"]}',
        '-metadata', f'artist={metadata["artist"]}',
        '-metadata', f'author={metadata["author"]}',
        '-metadata:s:v', f'title={metadata["video_title"]}',
        '-metadata:s:a', f'title={metadata["audio_title"]}',
        '-metadata:s:s', f'title={metadata["subtitle"]}',
        '-metadata', f'title={metadata["encoded_by"]}',
        '-metadata', f'title={metadata["custom_tag"]}',
        '-map', '0',
        '-c', 'copy',
        '-loglevel', 'error',
        output_path]

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await process.communicate()

    if process.returncode != 0:
        raise RuntimeError(f"FFmpeg error: {stderr.decode()}")

@Client.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_rename_files(client, message: Message):
    user_id = message.from_user.id
    user = message.from_user

    file_id = None
    file_name = None
    download_path = None
    metadata_path = None
    thumb_path = None
    file_path = None

    is_admin = hasattr(Config, "ADMIN") and user_id in Config.ADMIN
    user_data = await DARKXSIDE78.col.find_one({"_id": user_id})
    is_premium = user_data.get("is_premium", False) if user_data else False
    use_tokens = user_data.get("use_tokens", False) if user_data else False
    premium_expiry = user_data.get("premium_expiry")
    if is_premium and premium_expiry:
        if datetime.now() < premium_expiry:
            is_premium = True
        else:
            await DARKXSIDE78.col.update_one(
                {"_id": user_id},
                {"$set": {"is_premium": False, "premium_expiry": None}}
            )
            is_premium = False

    if not is_premium:
        current_tokens = user_data.get("token", Config.DEFAULT_TOKEN)
        if current_tokens <= 0:
            await message.reply_text("**Yᴏᴜ'ᴠᴇ ʀᴜɴ ᴏᴜᴛ ᴏғ ᴛᴏᴋᴇɴs!\nGᴇɴᴇʀᴀᴛᴇ ᴍᴏʀᴇ ʙʏ ᴜsɪɴɢ /gentoken ᴄᴍᴅ.**")
            return
    
    if PREMIUM_MODE and not is_premium:
        current_tokens = user_data.get("token", 0)
        if current_tokens <= 0:
            return await message.reply_text("**Yᴏᴜ'ᴠᴇ ʀᴜɴ ᴏᴜᴛ ᴏғ ᴛᴏᴋᴇɴs!\nGᴇɴᴇʀᴀᴛᴇ ᴍᴏʀᴇ ʙʏ ᴜsɪɴɢ /gentoken ᴄᴍᴅ.**")
        await DARKXSIDE78.col.update_one(
            {"_id": user_id},
            {"$inc": {"token": -1}}
        )

    concurrency_limit = Config.ADMIN_OR_PREMIUM_TASK_LIMIT if (is_admin or is_premium) else Config.NORMAL_TASK_LIMIT
    if user_id in USER_LIMITS:
        if USER_LIMITS[user_id] != concurrency_limit:
            USER_SEMAPHORES[user_id] = asyncio.Semaphore(concurrency_limit)
            USER_LIMITS[user_id] = concurrency_limit
    else:
        USER_LIMITS[user_id] = concurrency_limit
        USER_SEMAPHORES[user_id] = asyncio.Semaphore(concurrency_limit)

    semaphore = USER_SEMAPHORES[user_id]

    async with semaphore:
        if user_id in active_sequences:
            if message.document:
                file_id = message.document.file_id
                file_name = message.document.file_name
            elif message.video:
                file_id = message.video.file_id
                file_name = f"{message.video.file_name}.mp4"
            elif message.audio:
                file_id = message.audio.file_id
                file_name = f"{message.audio.file_name}.mp3"

            file_info = {"file_id": file_id, "file_name": file_name if file_name else "Unknown"}
            active_sequences[user_id].append(file_info)
            await message.reply_text("Fɪʟᴇ ʀᴇᴄᴇɪᴠᴇᴅ ɪɴ sᴇǫᴜᴇɴᴄᴇ...\n\nEɴᴅ Sᴇǫᴜᴇɴᴄᴇ ʙʏ ᴜsɪɴɢ /esequence")
            return

        format_template = await DARKXSIDE78.get_format_template(user_id)
        media_preference = await DARKXSIDE78.get_media_preference(user_id)

        if not format_template:
            return await message.reply_text("**Aᴜᴛᴏ ʀᴇɴᴀᴍᴇ ғᴏʀᴍᴀᴛ ɴᴏᴛ sᴇᴛ\nPʟᴇᴀsᴇ sᴇᴛ ᴀ ʀᴇɴᴀᴍᴇ ғᴏʀᴍᴀᴛ ᴜsɪɴɢ /autorename**")

        if message.document:
            file_id = message.document.file_id
            file_name = message.document.file_name
            file_size = message.document.file_size
            media_type = "document"
        elif message.video:
            file_id = message.video.file_id
            file_name = message.video.file_name or "video"
            file_size = message.video.file_size
            media_type = "video"
        elif message.audio:
            file_id = message.audio.file_id
            file_name = message.audio.file_name or "audio"
            file_size = message.audio.file_size
            media_type = "audio"
        else:
            return await message.reply_text("**Uɴsᴜᴘᴘᴏʀᴛᴇᴅ ғɪʟᴇ ᴛʏᴘᴇ**")

        if file_id in renaming_operations:
            elapsed_time = (datetime.now() - renaming_operations[file_id]).seconds
            if elapsed_time < 10:
                return

        renaming_operations[file_id] = datetime.now()

        try:
            season, episode = extract_season_episode(file_name)
            quality = extract_quality(file_name)

            audio_label = ""
            if media_type == "video":
                try:
                    audio_info = await detect_audio_info(file_path)
                    audio_label = get_audio_label(audio_info)
                    logger.info(f"Audio detection results: {audio_info} -> {audio_label}")
                except Exception as e:
                    logger.error(f"Audio detection failed: {e}")

            ext = os.path.splitext(file_name)[1] or ('.mp4' if media_type == 'video' else '.mp3')
            download_path  = f"downloads/{file_name}"
            metadata_path  = f"metadata/{file_name}"
            os.makedirs(os.path.dirname(download_path), exist_ok=True)
            os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

            msg = await message.reply_text("**Dᴏᴡɴʟᴏᴀᴅɪɴɢ...**")
            try:
                file_path = await client.download_media(
                    message,
                    file_name=download_path,
                    progress=progress_for_pyrogram,
                    progress_args=("**Dᴏᴡɴʟᴏᴀᴅɪɴɢ...**", msg, time.time())
                )
            except Exception as e:
                await msg.edit(f"Dᴏᴡɴʟᴏᴀᴅ ғᴀɪʟᴇᴅ: {e}")
                raise
            await asyncio.sleep(0.5)
            audio_info = await detect_audio_info(file_path)
            audio_label = get_audio_label(audio_info)

            replacements = {
                '{season}': season   or 'XX',
                '{episode}':episode  or 'XX',
                '{quality}':quality,
                '{audio}':  audio_label,
                '{Season}': season   or 'XX',
                '{Episode}':episode  or 'XX',
                '{Quality}':quality,
                '{Audio}':  audio_label,
                '{SEASON}': season   or 'XX',
                '{EPISODE}':episode  or 'XX',
                '{QUALITY}':quality,
                '{AUDIO}':  audio_label,
                'Season':   season   or 'XX',
                'Episode':  episode  or 'XX',
                'Quality':  quality,
                'SEASON':   season   or 'XX',
                'EPISODE':  episode  or 'XX',
                'QUALITY':  quality,
                'season':   season   or 'XX',
                'episode':  episode  or 'XX',
                'quality':  quality,
                'AUDIO':    audio_label,
                'Audio':    audio_label
            }
            for ph,val in replacements.items():
                format_template = format_template.replace(ph, val)

            new_filename = f"{format_template.format(**replacements)}{ext}"
            new_download = os.path.join("downloads", new_filename)
            new_metadata = os.path.join("metadata", new_filename)

            os.rename(download_path, new_download)
            download_path = new_download
            metadata_path = new_metadata

            await msg.edit("**Aᴅᴅɪɴɢ ᴍᴇᴛᴀᴅᴀᴛᴀ...**")
            try:
                await add_metadata(download_path, metadata_path, user_id)
                file_path = metadata_path
            except Exception as e:
                await msg.edit(f"Mᴇᴛᴀᴅᴀᴛᴀ ғᴀɪʟᴇᴅ: {e}")
                raise

            await msg.edit("**Pʀᴇᴘᴀʀɪɴɢ ᴜᴘʟᴏᴀᴅ...**")
            await DARKXSIDE78.col.update_one(
                {"_id": user_id},
                {
                    "$inc": {
                        "rename_count": 1,
                        "total_renamed_size": file_size,
                        "daily_count": 1
                    },
                    "$max": {
                        "max_file_size": file_size
                    }
                }
            )

            caption = await DARKXSIDE78.get_caption(message.chat.id) or f"**{new_filename}**"
            thumb = await DARKXSIDE78.get_thumbnail(message.chat.id)
            thumb_path = None

            if thumb:
                thumb_path = await client.download_media(thumb)
            elif media_type == "video" and message.video.thumbs:
                thumb_path = await client.download_media(message.video.thumbs[0].file_id)

            await msg.edit("**Uᴘʟᴏᴀᴅɪɴɢ...**")
            try:
                upload_params = {
                    'chat_id': message.chat.id,
                    'caption': caption,
                    'thumb': thumb_path,
                    'progress': progress_for_pyrogram,
                    'progress_args': ("Uᴘʟᴏᴀᴅɪɴɢ...", msg, time.time())
                }

                if media_type == "document":
                    await client.send_document(document=file_path, **upload_params)
                elif media_type == "video":
                    await client.send_video(video=file_path, **upload_params)
                elif media_type == "audio":
                    await client.send_audio(audio=file_path, **upload_params)

                await msg.delete()
            except Exception as e:
                await msg.edit(f"Uᴘʟᴏᴀᴅ ғᴀɪʟᴇᴅ: {e}")
                raise

        except Exception as e:
            logger.error(f"Processing error: {e}")
            await message.reply_text(f"Eʀʀᴏʀ: {str(e)}")
        finally:
            await cleanup_files(download_path, metadata_path, thumb_path)
            renaming_operations.pop(file_id, None)
            
@Client.on_message(filters.command("renamed") & (filters.group | filters.private))
async def renamed_stats(client, message: Message):
    try:
        args = message.command[1:] if len(message.command) > 1 else []
        target_user = None
        requester_id = message.from_user.id
        
        requester_data = await DARKXSIDE78.col.find_one({"_id": requester_id})
        is_premium = requester_data.get("is_premium", False) if requester_data else False
        is_admin = requester_id in Config.ADMIN if Config.ADMIN else False

        if is_premium and requester_data.get("premium_expiry"):
            if datetime.now() > requester_data["premium_expiry"]:
                is_premium = False
                await DARKXSIDE78.col.update_one(
                    {"_id": requester_id},
                    {"$set": {"is_premium": False}}
                )

        if args:
            try:
                if args[0].startswith("@"):
                    user = await client.get_users(args[0])
                    target_user = user.id
                else:
                    target_user = int(args[0])
            except:
                await message.reply_text("**Iɴᴠᴀʟɪᴅ ғᴏʀᴍᴀᴛ! Usᴇ /renamed [@username|user_id]**")
                return

        if target_user and not (is_admin or is_premium):
            return await message.reply_text("**Pʀᴇᴍɪᴜᴍ ᴏʀ ᴀᴅᴍɪɴ ʀᴇǫᴜɪʀᴇᴅ ᴛᴏ ᴠɪᴇᴡ ᴏᴛʜᴇʀs' sᴛᴀᴛs!**")

        if target_user:
            user_data = await DARKXSIDE78.col.find_one({"_id": target_user})
            if not user_data:
                return await message.reply_text("**Usᴇʀ ɴᴏᴛ ғᴏᴜɴᴅ!**")

            response = [
                f"**┌─── ∘° Pʀᴇᴍɪᴜᴍ Usᴇʀ Sᴛᴀᴛs °∘ ───┐**" if is_premium else "**┌─── ∘° Aᴅᴍɪɴ Usᴇʀ Sᴛᴀᴛs °∘ ───┐**",
                f"**➤ Usᴇʀ: {target_user}**",
                f"**➤ Tᴏᴛᴀʟ Rᴇɴᴀᴍᴇs: {user_data.get('rename_count', 0)}**",
                f"**➤ Tᴏᴛᴀʟ Sɪᴢᴇ: {humanbytes(user_data.get('total_renamed_size', 0))}**",
                f"**➤ Mᴀx Fɪʟᴇ Sɪᴢᴇ: {humanbytes(user_data.get('max_file_size', 0))}**",
                f"**➤ Pʀᴇᴍɪᴜᴍ Sᴛᴀᴛᴜs: {'Active' if user_data.get('is_premium') else 'Inactive'}**"
            ]
            
            if is_admin or is_premium:
                response.append(f"**➤ Tᴏᴋᴇɴs: {user_data.get('token', 0)}**")
                response.append(f"**└───────── °∘ ❉ ∘° ───────┘**")

        else:
            user_data = await DARKXSIDE78.col.find_one({"_id": requester_id})
            if not user_data:
                user_data = {}
            response = [
                f"**┌─── ∘° Yᴏᴜʀ Rᴇɴᴀᴍᴇ Sᴛᴀᴛs °∘ ───┐**",
                f"**➤ Tᴏᴛᴀʟ Rᴇɴᴀᴍᴇs: {user_data.get('rename_count', 0)}**",
                f"**➤ Tᴏᴛᴀʟ Sɪᴢᴇ: {humanbytes(user_data.get('total_renamed_size', 0))}**",
                f"**➤ Mᴀx Fɪʟᴇ Sɪᴢᴇ: {humanbytes(user_data.get('max_file_size', 0))}**",
                f"**➤ Pʀᴇᴍɪᴜᴍ Sᴛᴀᴛᴜs: {'Active' if is_premium else 'Inactive'}**",
                f"**➤ Rᴇᴍᴀɪɴɪɴɢ Tᴏᴋᴇɴs: {user_data.get('token', 0)}**",
                f"**└──────── °∘ ❉ ∘° ─────────┘**"
            ]

            if is_admin or is_premium:
                pipeline = [{"$group": {
                    "_id": None,
                    "total_renames": {"$sum": "$rename_count"},
                    "total_size": {"$sum": "$total_renamed_size"},
                    "max_size": {"$max": "$max_file_size"},
                    "user_count": {"$sum": 1}
                }}]
                stats = (await DARKXSIDE78.col.aggregate(pipeline).to_list(1))[0]
                
                response.extend([
                    f"\n<blockquote>**┌─── ∘° Gʟᴏʙᴀʟ  Sᴛᴀᴛs °∘ ───┐**</blockquote>",
                    f"**➤ Tᴏᴛᴀʟ Usᴇʀs: {stats['user_count']}**",
                    f"**➤ Tᴏᴛᴀʟ Fɪʟᴇs: {stats['total_renames']}**",
                    f"**➤ Tᴏᴛᴀʟ Sɪᴢᴇ: {humanbytes(stats['total_size'])}**",
                    f"**➤ Lᴀʀɢᴇsᴛ Fɪʟᴇ: {humanbytes(stats['max_size'])}**",
                    f"**<blockquote>**└─────── °∘ ❉ ∘° ────────┘**</blockquote>**"
                ])

        reply = await message.reply_text("\n".join(response))
        
        if message.chat.type != "private":
            await asyncio.sleep(Config.RENAMED_DELETE_TIMER)
            await reply.delete()
            await message.delete()

    except Exception as e:
        error_msg = await message.reply_text(f"❌ Error: {str(e)}")
        await asyncio.sleep(30)
        await error_msg.delete()
        logger.error(f"Stats error: {e}", exc_info=True)

@Client.on_message(filters.command("info") & (filters.group | filters.private))
async def system_info(client, message: Message):
    try:
        import psutil
        from platform import python_version, system, release

        total_users = await DARKXSIDE78.col.count_documents({})
        active_users = await DARKXSIDE78.col.count_documents({
            "last_active": {"$gte": datetime.now() - timedelta(days=30)}
        })
        
        storage_pipeline = [
            {"$group": {
                "_id": None,
                "total_size": {"$sum": "$total_renamed_size"},
                "total_files": {"$sum": "$rename_count"}
            }}
        ]
        storage_stats = await DARKXSIDE78.col.aggregate(storage_pipeline).to_list(1)
        
        cpu_usage = psutil.cpu_percent()
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        boot_time = datetime.fromtimestamp(psutil.boot_time()).strftime("%Y-%m-%d %H:%M:%S")
        
        response = f"""
<blockquote>**◈ - [Sʏsᴛᴇᴍ Iɴғᴏʀᴍᴀᴛɪᴏɴ] - ◈**</blockquote>


<blockquote>**[Usᴇʀ Sᴛᴀᴛɪsᴛɪᴄs]**
**Tᴏᴛᴀʟ Usᴇʀs = {total_users}**
**Aᴄᴛɪᴠᴇ Usᴇʀs (30ᴅ) = {active_users}**
**Iɴᴀᴄᴛɪᴠᴇ Usᴇʀs = {total_users - active_users}**
**Tᴏᴛᴀʟ Fɪʟᴇs Rᴇɴᴀᴍᴇᴅ = {storage_stats[0].get('total_files', 0) if storage_stats else 0}**
**Tᴏᴛᴀʟ Sᴛᴏʀᴀɢᴇ Usᴇᴅ = {humanbytes(storage_stats[0].get('total_size', 0)) if storage_stats else '0 B'}**</blockquote>

<blockquote>**[Sʏsᴛᴇᴍ Iɴғᴏʀᴍᴀᴛɪᴏɴ]**
**OS Vᴇʀsɪᴏɴ = {system()} {release()}**
**Pʏᴛʜᴏɴ Vᴇʀsɪᴏɴ = {python_version()}**
**CPU Usᴀɢᴇ = {cpu_usage}%**
**Mᴇᴍᴏʀʏ Usᴀɢᴇ = {humanbytes(mem.used)} / {humanbytes(mem.total)}**
**Dɪsᴋ Usᴀɢᴇ = {humanbytes(disk.used)} / {humanbytes(disk.total)}**
**Uᴘᴛɪᴍᴇ = {datetime.now() - datetime.fromtimestamp(psutil.boot_time())}**</blockquote>

<blockquote>**[Vᴇʀsɪᴏɴ Iɴғᴏʀᴍᴀᴛɪᴏɴ]**
**Bᴏᴛ Vᴇʀsɪᴏɴ = ****{Config.VERSION}**
**Lᴀsᴛ Uᴘᴅᴀᴛᴇᴅ = ****{Config.LAST_UPDATED}**
**Dᴀᴛᴀʙᴀsᴇ Vᴇʀsɪᴏɴ =** **{Config.DB_VERSION}**</blockquote>
    """
        await message.reply_text(response)

    except Exception as e:
        await message.reply_text(f"Eʀʀᴏʀ: {str(e)}")
        logger.error(f"System info error: {e}", exc_info=True)

@Client.on_message(filters.command("dc") & (filters.group | filters.private))
async def dc_stats(client, message: Message):
    args       = message.command[1:] if len(message.command) > 1 else []
    is_admin   = False
    is_premium = False

    if message.chat.type == "private":
        is_admin = message.from_user.id in getattr(Config, "ADMINS", [])
    else:
        try:
            member   = await client.get_chat_member(message.chat.id, message.from_user.id)
            is_admin = member.status in ["creator", "administrator"] \
                       or message.from_user.id in getattr(Config, "ADMINS", [])
        except:
            is_admin = False

    is_premium = message.from_user.id in getattr(Config, "PREMIUM_USERS", [])

    target = message.from_user.id
    if args:
        a = args[0].lower()
        if a in ("me", "@me"):
            target = message.from_user.id
        else:
            if not is_admin:
                return await message.reply_text("<blockquote>**Aᴅᴍɪɴs ᴏɴʟʏ ᴄᴀɴ ᴄʜᴇᴄᴋ ᴏᴛʜᴇʀs’ DC!**</blockquote>")
            try:
                target = int(a) if not a.startswith("@") else (await client.get_users(a)).id
            except:
                return await message.reply_text("<blockquote>**Iɴᴠᴀʟɪᴅ ᴜsᴇʀ ID ᴏʀ ᴜsᴇʀɴᴀᴍᴇ...**</blockquote>")

    if target == message.from_user.id and not (is_admin or is_premium):
        return await message.reply_text("<blockquote>**Yᴏᴜ ɴᴇᴇᴅ ᴛᴏ ʙᴇ ᴘʀᴇᴍɪᴜᴍ (ᴏʀ ᴀᴅᴍɪɴ) ᴛᴏ ᴄʜᴇᴄᴋ ʏᴏᴜʀ DC!!!**</blockquote>")

    user = await DARKXSIDE78.col.find_one({"_id": target})
    dc    = user.get("daily_count", 0) if user else 0

    if target == message.from_user.id:
        text = f"<blockquote><b>➤ Usᴇʀ ID: <code>{target}</code></b>\n<b>➤ Yᴏᴜʀ Dᴀɪʟʏ Cᴏᴜɴᴛ (DC):</b> <code>{dc}</code></blockquote>"
    else:
        text = (
            f"➤ <b>Usᴇʀ ID:</b> <code>{target}</code>\n"
            f"➤ <b>Yᴏᴜʀ Dᴀɪʟʏ Cᴏᴜɴᴛ (DC):</b> <code>{dc}</code>"
        )

    await message.reply_text(text, parse_mode=ParseMode.HTML)
