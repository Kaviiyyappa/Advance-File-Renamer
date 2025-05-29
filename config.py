import re, os, time
from os import environ, getenv
id_pattern = re.compile(r'^.\d+$') 


class Config(object):
    API_ID    = os.environ.get("API_ID", "21939922")
    API_HASH  = os.environ.get("API_HASH", "bd2d18dd26b200480bda4cbdf2c2da30")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "") 

    DB_NAME = os.environ.get("DB_NAME","RenamerBot")     
    DB_URL  = os.environ.get("DB_URL","mongodb+srv://ikbryt666:YJIcbUwfBRsJVelb@cluster0.2odc3wb.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    PORT = os.environ.get("PORT", "8080")
 
    BOT_UPTIME  = time.time()
    START_PIC   = os.environ.get("START_PIC", "https://iili.io/3KDcyZB.md.jpg")
    START_STICKER   = "CAACAgUAAyEFAASUwGgHAAIQwWgEbJZ5TgLj7QXhvMhGtWF2L30EAAIzEwACTrLwVSdrhgIPuK1TNgQ"
    FORCE_PIC   = os.environ.get("FORCE_PIC", "https://www.wallpaperflare.com/static/591/238/424/naruto-shippuuden-uzumaki-naruto-uchiha-sasuke-rinnegan-wallpaper.jpg")
    ADMINS       = [int(admins) if id_pattern.search(admins) else admins for admins in os.environ.get('ADMINS', '67570141467').split()]
    ADMIN       = [int(admin) if id_pattern.search(admin) else admin for admin in os.environ.get('ADMIN', '67570141467').split()]
    FORCE_SUB_CHANNELS = os.environ.get('FORCE_SUB_CHANNELS', '@IKBRYTLinkz').split(', ')
    LOG_CHANNEL = os.environ.get("LOG_CHANNEL", "-1002586186825")
    DUMP_CHANNEL = os.environ.get("DUMP_CHANNEL", "-1002586186825")
    BOT_CHANNEL_NAME = os.environ.get("BOT_CHANNEL_NAME", "IKBRYTLinkz")
    BOT_CHANNEL_USERNAME = os.environ.get("BOT_CHANNEL_USERNAME", "IKBRYTLinkz")
    SUPPORT_CHANNEL_NAME = os.environ.get("SUPPORT_CHANNEL_NAME", "IKBRYTLinkz")
    SUPPORT_CHANNEL_USERNAME = os.environ.get("SUPPORT_CHANNEL_USERNAME", "IKBRYTLinkz")
    BOT_USERNAME = os.environ.get("BOT_USERNAME", "IKBRTY_STORAGEBOT")
    BOT_NAME = os.environ.get("BOT_NAME", "FILE_STORAGE")
    OWNER_NAME = os.environ.get("OWNER_NAME", "𝐈𝐊𝐁𝐑𝐘𝐓_𝐒𝐔𝐏𝐏𝐎𝐑𝐓")
    OWNER_USERNAME = os.environ.get("OWNER_USERNAME", "IKBRYT_SUPPORT")
    DEVELOPER_USERNAME = os.environ.get("DEVELOPER_USERNAME", "IKBRYT_SUPPORT")
    DEVELOPER_NAME = os.environ.get("DEVELOPER_NAME", "IKBRYT_SUPPORT")
    #TOKEN_API = "https://instantearn.in/api?api=fa0dc64a5224ed38ec7b25c70f40922a1f8aeb15&format=text"
    TOKEN_API = ""
    SHORTENER_URL = ""
    TOKEN_ID_LENGTH = 8
    SHORTENER_TOKEN_GEN = 100
    VERSION = "4.1.2"
    LAST_UPDATED = "2024-04-20"
    DB_VERSION = "1.4.2"
    FLOODWAIT_RETRIES = 10
    FLOODWAIT_WAIT = 15
    DEFAULT_TOKEN = 100
    LEADERBOARD_DELETE_TIMER = 30
    RENAMED_DELETE_TIMER = 120
    ADMIN_OR_PREMIUM_TASK_LIMIT = 6
    NORMAL_TASK_LIMIT = 3
    MAX_CONCURRENT_TASKS = 25
    GLOBAL_TOKEN_MODE = False
    GLOBAL_TOKEN_EXPIRY = None
    SESSION_NAME = "DARKXSIDE78"
    WEBHOOK = bool(os.environ.get("WEBHOOK", "True"))
    WEBHOOK_PORT = os.environ.get("WEBHOOK_PORT", "8080")


class Txt(object):
        
    START_TXT = """<b>ʜᴇʏ! {}  

» ɪ ᴀᴍ ᴀᴅᴠᴀɴᴄᴇᴅ ʀᴇɴᴀᴍᴇ ʙᴏᴛ! ᴡʜɪᴄʜ ᴄᴀɴ ᴀᴜᴛᴏʀᴇɴᴀᴍᴇ ʏᴏᴜʀ ғɪʟᴇs ᴡɪᴛʜ ᴄᴜsᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ ᴀɴᴅ ᴛʜᴜᴍʙɴᴀɪʟ ᴀɴᴅ ᴀʟsᴏ sᴇǫᴜᴇɴᴄᴇ ᴛʜᴇᴍ ᴘᴇʀғᴇᴄᴛʟʏ</b>"""
    
    FILE_NAME_TXT = f"""<b>» <u>sᴇᴛᴜᴘ ᴀᴜᴛᴏ ʀᴇɴᴀᴍᴇ ғᴏʀᴍᴀᴛ</u></b>

<b>ᴠᴀʀɪᴀʙʟᴇꜱ :</b>
➲ ᴇᴘɪꜱᴏᴅᴇ - ᴛᴏ ʀᴇᴘʟᴀᴄᴇ ᴇᴘɪꜱᴏᴅᴇ ɴᴜᴍʙᴇʀ  
➲ ǫᴜᴀʟɪᴛʏ - ᴛᴏ ʀᴇᴘʟᴀᴄᴇ ǫᴜᴀʟɪᴛʏ  
➲ sᴇᴀsᴏɴ - ᴛᴏ ʀᴇᴘʟᴀᴄᴇ sᴇᴀsᴏɴ  
➲ ᴀᴜᴅɪᴏ - ᴛᴏ ʀᴇᴘʟᴀᴄᴇ ᴀᴜᴅɪᴏ  

<b>‣ ᴇxᴀᴍᴘʟᴇ :</b>  
<code>/autorename [Sseason-episode] One Piece [quality] [audio] @GenAnimeOfc</code>

<b>‣ /autorename:</b>  
ʀᴇɴᴀᴍᴇ ʏᴏᴜʀ ᴍᴇᴅɪᴀ ꜰɪʟᴇꜱ ʙʏ ᴜꜱɪɴɢ 'ᴇᴘɪꜱᴏᴅᴇ' ᴀɴᴅ 'ǫᴜᴀʟɪᴛʏ' ᴠᴀʀɪᴀʙʟᴇꜱ ɪɴ ʏᴏᴜʀ ꜰᴏʀᴍᴀᴛ ᴛᴇxᴛ.  
ᴛʜᴇʏ ᴡɪʟʟ ʙᴇ ᴇxᴛʀᴀᴄᴛᴇᴅ ꜰʀᴏᴍ ᴛʜᴇ ᴏʀɪɢɪɴᴀʟ ꜰɪʟᴇɴᴀᴍᴇ ᴀɴᴅ ᴜꜱᴇᴅ ɪɴ ʀᴇɴᴀᴍɪɴɢ.
"""

    ABOUT_TXT = f"""<b>❍ ᴍʏ ɴᴀᴍᴇ : <a href="https://t.me/{Config.BOT_USERNAME}">{Config.BOT_NAME}</a>
❍ ᴅᴇᴠᴇʟᴏᴩᴇʀ : <a href="https://t.me/{Config.DEVELOPER_USERNAME}">{Config.DEVELOPER_NAME}</a>
❍ ᴏᴡɴᴇʀ : <a href="https://t.me/{Config.OWNER_USERNAME}">{Config.OWNER_NAME}</a>
❍ ʟᴀɴɢᴜᴀɢᴇ : <a href="https://www.python.org/">ᴘʏᴛʜᴏɴ</a>
❍ ᴅᴀᴛᴀʙᴀꜱᴇ : <a href="https://www.mongodb.com/">ᴍᴏɴɢᴏ ᴅʙ</a>
❍ ʜᴏꜱᴛᴇᴅ ᴏɴ : <a href="https://t.me/{Config.DEVELOPER_USERNAME}">ᴠᴘs</a>
❍ ᴍᴀɪɴ ᴄʜᴀɴɴᴇʟ : <a href="https://t.me/{Config.BOT_CHANNEL_USERNAME}">{Config.BOT_CHANNEL_NAME}</a>
❍ ʜᴇʟᴘ ᴄʜᴀɴɴᴇʟ : <a href="https://t.me/{Config.SUPPORT_CHANNEL_USERNAME}">{Config.SUPPORT_CHANNEL_NAME}</a>

➻ ᴄʟɪᴄᴋ ᴏɴ ᴛʜᴇ ʙᴜᴛᴛᴏɴs ɢɪᴠᴇɴ ʙᴇʟᴏᴡ ғᴏʀ ɢᴇᴛᴛɪɴɢ ʙᴀsɪᴄ ʜᴇʟᴩ ᴀɴᴅ ɪɴғᴏ ᴀʙᴏᴜᴛ ᴍᴇ.</b>"""

    
    THUMBNAIL_TXT = """<b><u>» ᴛᴏ ꜱᴇᴛ ᴄᴜꜱᴛᴏᴍ ᴛʜᴜᴍʙɴᴀɪʟ</u></b>
    
➲ /start: ꜱᴇɴᴅ ᴀɴʏ ᴘʜᴏᴛᴏ ᴛᴏ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ꜱᴇᴛ ɪᴛ ᴀꜱ ᴀ ᴛʜᴜᴍʙɴᴀɪʟ..
➲ /del_thumb: ᴜꜱᴇ ᴛʜɪꜱ ᴄᴏᴍᴍᴀɴᴅ ᴛᴏ ᴅᴇʟᴇᴛᴇ ʏᴏᴜʀ ᴏʟᴅ ᴛʜᴜᴍʙɴᴀɪʟ.
➲ /view_thumb: ᴜꜱᴇ ᴛʜɪꜱ ᴄᴏᴍᴍᴀɴᴅ ᴛᴏ ᴠɪᴇᴡ ʏᴏᴜʀ ᴄᴜʀʀᴇɴᴛ ᴛʜᴜᴍʙɴᴀɪʟ.

ɴᴏᴛᴇ: ɪꜰ ɴᴏ ᴛʜᴜᴍʙɴᴀɪʟ ꜱᴀᴠᴇᴅ ɪɴ ʙᴏᴛ ᴛʜᴇɴ, ɪᴛ ᴡɪʟʟ ᴜꜱᴇ ᴛʜᴜᴍʙɴᴀɪʟ ᴏꜰ ᴛʜᴇ ᴏʀɪɢɪɴɪᴀʟ ꜰɪʟᴇ ᴛᴏ ꜱᴇᴛ ɪɴ ʀᴇɴᴀᴍᴇᴅ ꜰɪʟᴇ"""

    CAPTION_TXT = """<b><u>» ᴛᴏ ꜱᴇᴛ ᴄᴜꜱᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ ᴀɴᴅ ᴍᴇᴅɪᴀ ᴛʏᴘᴇ</u></b>
    
<b>ᴠᴀʀɪᴀʙʟᴇꜱ :</b>         
ꜱɪᴢᴇ: {filesize}
ᴅᴜʀᴀᴛɪᴏɴ: {duration}
ꜰɪʟᴇɴᴀᴍᴇ: {filename}

➲ /set_caption: ᴛᴏ ꜱᴇᴛ ᴀ ᴄᴜꜱᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ.
➲ /see_caption: ᴛᴏ ᴠɪᴇᴡ ʏᴏᴜʀ ᴄᴜꜱᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ.
➲ /del_caption: ᴛᴏ ᴅᴇʟᴇᴛᴇ ʏᴏᴜʀ ᴄᴜꜱᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ.

» ꜰᴏʀ ᴇx:- /set_caption ꜰɪʟᴇ ɴᴀᴍᴇ: {filesize}"""

    PROGRESS_BAR = """\n
<b>» Size</b> : {1} | {2}
<b>» Done</b> : {0}%
<b>» Speed</b> : {3}/s
<b>» ETA</b> : {4} """
    
    
    DONATE_TXT = f"""<blockquote> ᴛʜᴀɴᴋs ғᴏʀ sʜᴏᴡɪɴɢ ɪɴᴛᴇʀᴇsᴛ ɪɴ ᴅᴏɴᴀᴛɪᴏɴ</blockquote>

<b><i>💞  ɪꜰ ʏᴏᴜ ʟɪᴋᴇ ᴏᴜʀ ʙᴏᴛ ꜰᴇᴇʟ ꜰʀᴇᴇ ᴛᴏ ᴅᴏɴᴀᴛᴇ ᴀɴʏ ᴀᴍᴏᴜɴᴛ ₹𝟷𝟶, ₹𝟸𝟶, ₹𝟻𝟶, ₹𝟷𝟶𝟶, ᴇᴛᴄ.</i></b>

ᴅᴏɴᴀᴛɪᴏɴs ᴀʀᴇ ʀᴇᴀʟʟʏ ᴀᴘᴘʀᴇᴄɪᴀᴛᴇᴅ ɪᴛ ʜᴇʟᴘs ɪɴ ʙᴏᴛ ᴅᴇᴠᴇʟᴏᴘᴍᴇɴᴛ

 <u>ʏᴏᴜ ᴄᴀɴ ᴀʟsᴏ ᴅᴏɴᴀᴛᴇ ᴛʜʀᴏᴜɢʜ ᴜᴘɪ</u>

 ᴜᴘɪ ɪᴅ : <code>ramkamo3363-3@oksbi</code>

ɪғ ʏᴏᴜ ᴡɪsʜ ʏᴏᴜ ᴄᴀɴ sᴇɴᴅ ᴜs ss
ᴏɴ - @{Config.OWNER_USERNAME}"""

    PREMIUM_TXT = """<b>ᴜᴘɢʀᴀᴅᴇ ᴛᴏ ᴏᴜʀ ᴘʀᴇᴍɪᴜᴍ sᴇʀᴠɪᴄᴇ ᴀɴᴅ ᴇɴJᴏʏ ᴇxᴄʟᴜsɪᴠᴇ ғᴇᴀᴛᴜʀᴇs:
○ ᴜɴʟɪᴍɪᴛᴇᴅ Rᴇɴᴀᴍɪɴɢ.
○ ɴᴏ ᴀᴅꜱ.
○ ᴇᴀʀʟʏ Aᴄᴄᴇss.
○ ᴍᴏʀᴇ ᴘʀɪᴏʀɪᴛʏ

• ᴜꜱᴇ /plan ᴛᴏ ꜱᴇᴇ ᴀʟʟ ᴏᴜʀ ᴘʟᴀɴꜱ ᴀᴛ ᴏɴᴄᴇ.

➲ ғɪʀsᴛ sᴛᴇᴘ : ᴘᴀʏ ᴛʜᴇ ᴀᴍᴏᴜɴᴛ ᴀᴄᴄᴏʀᴅɪɴɢ ᴛᴏ ʏᴏᴜʀ ғᴀᴠᴏʀɪᴛᴇ ᴘʟᴀɴ ᴛᴏ ᴛʜᴇ ᴜᴘɪ ɪᴅ ᴏʀ Qʀ.
➲ secoɴᴅ sᴛᴇᴘ : ᴛᴀᴋᴇ ᴀ sᴄʀᴇᴇɴsʜᴏᴛ ᴏғ ʏᴏᴜʀ ᴘᴀʏᴍᴇɴᴛ ᴀɴᴅ sʜᴀʀᴇ ɪᴛ ᴅɪʀᴇᴄᴛʟʏ ʜᴇʀᴇ: @darkxside78, @Jas_Mehra ᴏʀ @Blakite_Ravii
➲ ᴀʟᴛᴇʀɴᴀᴛɪᴠᴇ sᴛᴇᴘ : ᴏʀ ᴜᴘʟᴏᴀᴅ ᴛʜᴇ sᴄʀᴇᴇɴsʜᴏᴛ ʜᴇʀᴇ ᴀɴᴅ ʀᴇᴘʟʏ ᴡɪᴛʜ ᴛʜᴇ /bought ᴄᴏᴍᴍᴀɴᴅ. [ᴍᴀʏ ɴᴏᴛ ᴡᴏʀᴋ ᴘʀᴏᴘᴇʀʟʏ]

Yᴏᴜʀ ᴘʀᴇᴍɪᴜᴍ ᴘʟᴀɴ ᴡɪʟʟ ʙᴇ ᴀᴄᴛɪᴠᴀᴛᴇᴅ ᴀғᴛᴇʀ ᴠᴇʀɪғɪᴄᴀᴛɪᴏɴ</b>"""

    PREPLANS_TXT = """<b>👋 ʜᴇʟʟᴏ ʙʀᴏ,
    
🎖️ <u>ᴀᴠᴀɪʟᴀʙʟᴇ ᴘʟᴀɴs</u> :

Pʀɪᴄɪɴɢ:
➜ ᴅᴀɪʟʏ ᴘʀᴇᴍɪᴜᴍ: ₹10/ᴅᴀʏ
➜ ᴍᴏɴᴛʜʟʏ ᴘʀᴇᴍɪᴜᴍ: ₹80/ᴍᴏɴᴛʜ
➜ ʟɪꜰᴇᴛɪᴍᴇ ᴘʀᴇᴍɪᴜᴍ: ₹459
➜ ғᴏʀ ʙᴏᴛ ʜᴏsᴛɪɴɢ: [ᴄᴏɴᴛᴀᴄᴛ ᴜꜱ](https://t.me/{Config.DEVELOPER_USERNAME})

➲ ᴜᴘɪ ɪᴅ - <code>ramkamo3363-3@oksbi</code>

‼️ᴜᴘʟᴏᴀᴅ ᴛʜᴇ ᴘᴀʏᴍᴇɴᴛ sᴄʀᴇᴇɴsʜᴏᴛ ʜᴇʀᴇ ᴀɴᴅ ʀᴇᴘʟʏ ᴡɪᴛʜ ᴛʜᴇ /bought ᴄᴏᴍᴍᴀɴᴅ.</b>"""
    
    HELP_TXT = """<b>ʜᴇʀᴇ ɪꜱ ʜᴇʟᴘ ᴍᴇɴᴜ ɪᴍᴘᴏʀᴛᴀɴᴛ ᴄᴏᴍᴍᴀɴᴅꜱ:

ᴀᴡᴇsᴏᴍᴇ ғᴇᴀᴛᴜʀᴇs🫧

ʀᴇɴᴀᴍᴇ ʙᴏᴛ ɪꜱ ᴀ ʜᴀɴᴅʏ ᴛᴏᴏʟ ᴛʜᴀᴛ ʜᴇʟᴘꜱ ʏᴏᴜ ʀᴇɴᴀᴍᴇ ᴀɴᴅ ᴍᴀɴᴀɢᴇ ʏᴏᴜʀ ꜰɪʟᴇꜱ ᴇꜰꜰᴏʀᴛʟᴇꜱꜱʟʏ.

➲ /autorename: ᴀᴜᴛᴏ ʀᴇɴᴀᴍᴇ ʏᴏᴜʀ ꜰɪʟᴇꜱ.
➲ /metadata: ᴄᴏᴍᴍᴀɴᴅꜱ ᴛᴏ ᴛᴜʀɴ ᴏɴ ᴏғғ ᴍᴇᴛᴀᴅᴀᴛᴀ.
➲ /help: ɢᴇᴛ ǫᴜɪᴄᴋ ᴀꜱꜱɪꜱᴛᴀɴᴄᴇ.</b>"""

    SEND_METADATA = """
<b>--Metadata Settings:--</b>

➜ /metadata: Turn on or off metadata.

<b>Description</b> : Metadata will change MKV video files including all audio, streams, and subtitle titles."""


    SOURCE_TXT = f"""
<b>ʜᴇʏ,
ᴛʜɪs ɪs ᴀɴ ᴀᴜᴛᴏ ʀᴇɴᴀᴍᴇ ʙᴏᴛ,
ᴄʀᴇᴀᴛᴇᴅ ʙʏ [{Config.BOT_CHANNEL_NAME}](https://t.me/{Config.BOT_CHANNEL_USERNAME}).</b>

ᴡʀɪᴛᴛᴇɴ ɪɴ ᴩʏᴛʜᴏɴ ᴡɪᴛʜ ᴛʜᴇ ʜᴇʟᴩ ᴏғ :
[ᴩʏʀᴏɢʀᴀᴍ](https://github.com/pyrogram/pyrogram)
[ᴩʏᴛʜᴏɴ-ᴛᴇʟᴇɢʀᴀᴍ-ʙᴏᴛ](https://github.com/python-telegram-bot/python-telegram-bot)
ᴀɴᴅ ᴜsɪɴɢ [ᴍᴏɴɢᴏ](https://cloud.mongodb.com) ᴀs ᴅᴀᴛᴀʙᴀsᴇ."""

    META_TXT = """
**ᴍᴀɴᴀɢɪɴɢ ᴍᴇᴛᴀᴅᴀᴛᴀ ғᴏʀ ʏᴏᴜʀ ᴠɪᴅᴇᴏs ᴀɴᴅ ғɪʟᴇs**

**ᴠᴀʀɪᴏᴜꜱ ᴍᴇᴛᴀᴅᴀᴛᴀ:**

- **ᴛɪᴛʟᴇ**: Descriptive title of the media.
- **ᴀᴜᴛʜᴏʀ**: The creator or owner of the media.
- **ᴀʀᴛɪꜱᴛ**: The artist associated with the media.
- **ᴀᴜᴅɪᴏ**: Title or description of audio content.
- **ꜱᴜʙᴛɪᴛʟᴇ**: Title of subtitle content.
- **ᴠɪᴅᴇᴏ**: Title or description of video content.
- **ᴇɴᴄᴏᴅᴇ ʙʏ**: The person who encoded the video.
- **ᴄᴜꜱᴛᴏᴍ ᴛᴀɢ**: Any Title.
 

**ᴄᴏᴍᴍᴀɴᴅꜱ ᴛᴏ ᴛᴜʀɴ ᴏɴ ᴏғғ ᴍᴇᴛᴀᴅᴀᴛᴀ:**
➜ /metadata: Turn on or off metadata.

**ᴄᴏᴍᴍᴀɴᴅꜱ ᴛᴏ ꜱᴇᴛ ᴍᴇᴛᴀᴅᴀᴛᴀ:**

➜ /settitle: Set a custom title of media.
➜ /setauthor: Set the author.
➜ /setartist: Set the artist.
➜ /setaudio: Set audio title.
➜ /setsubtitle: Set subtitle title.
➜ /setvideo: Set video title.
➜ /setencoded_by: Set encoded by title.
➜ /setcustom_tag: Set custom tag title.

**ᴇxᴀᴍᴘʟᴇ:** /settitle Your Title Here

**ᴜꜱᴇ ᴛʜᴇꜱᴇ ᴄᴏᴍᴍᴀɴᴅꜱ ᴛᴏ ᴇɴʀɪᴄʜ ʏᴏᴜʀ ᴍᴇᴅɪᴀ ᴡɪᴛʜ ᴀᴅᴅɪᴛɪᴏɴᴀʟ ᴍᴇᴛᴀᴅᴀᴛᴀ ɪɴꜰᴏʀᴍᴀᴛɪᴏɴ!**
"""
