Build a complete advanced Telegram bot system using Python and Telethon.

The system consists of:
1. A MASTER BOT (controller bot)
2. Multiple ASSISTANT BOTS (user session-based bots)

Database:
- Use HuggingFace Dataset as database storage.
- HF_TOKEN will be provided via environment variables.
- On every startup, sync and load full data from HF dataset.
- Provide a SYNC system to push updates back to HF dataset manually and automatically when needed.

Session Handling:
- MASTER bot session (master.session) is stored in HF dataset.
- Assistant bots are created dynamically using uploaded .session files.

--------------------------------------------------

🔷 MASTER BOT FEATURES

UI:
When user starts master bot, show 4 inline buttons:

1. CONNECT BOT
2. MY ALL BOTS
3. DISCONNECT
4. ADMIN PANEL

----------------------------------------

1. CONNECT BOT FLOW

- When user clicks "Connect Bot":
  - Ask user to upload a .session file.
  - After receiving session:
    - Validate session.
    - Connect Telethon client using that session.

  - Ask user:
    "Add this assistant bot to a group and make it admin for logs"

  - Provide SKIP option:
    - If skipped → logs go to owner's DM
    - Else → logs go to group

  - Save:
    - session
    - owner id
    - log group id (if provided)
    - assistant metadata

  - Activate assistant bot.

----------------------------------------

2. MY ALL BOTS

- Show all connected assistant bots as inline buttons.

- On clicking any bot:
  Show detailed stats:
  - Total users
  - Premium users
  - Non-premium users
  - Blocked users
  - Total /start count
  - Total messages received
  - Admin count
  - Creation date
  - Last active time

----------------------------------------

3. DISCONNECT

- Show list of assistant bots (inline buttons).
- On selecting a bot:
  - Stop assistant bot
  - Delete session
  - Remove all stored data of that assistant
  - Confirm deletion

----------------------------------------

4. ADMIN PANEL

Owner ID: 8413365423 (Super Admin)

Admin Features:
- /ban user (block from using master bot)
- /unban user
- /promote user (make admin)
- /demote user

Admin Panel Buttons:
- List all assistant bots (inline)
- Click bot → show:
    - Owner info
    - Stats
    - Control options:
        - Disconnect bot
        - Wipe all data
        - Upload/Replace session
- Global SYNC button (sync HF dataset)
- View total users of master bot
- View total assistant bots count

NOTE:
- 8413365423 must be admin in ALL assistant bots automatically with full rights.

--------------------------------------------------

🔷 ASSISTANT BOT FEATURES

Each assistant bot runs independently using user session.

----------------------------------------

1. MESSAGE LOGGING

- Log EVERY message received by assistant bot.
- Send logs to:
  - Group (if configured)
  - Else → Owner DM

----------------------------------------

2. OWNER COMMANDS

Owner/Admin can use:

- /ban → ban user from assistant bot
- /unban → unban user
- /promote → add admin
- /demote → remove admin

----------------------------------------

3. REPLY SYSTEM (VERY IMPORTANT)

- When user sends message:
  → bot forwards/logs it

- If owner/admin replies to that forwarded message:
  → bot sends that reply back to original user

(This should behave like Livegram reply system)

----------------------------------------

4. /menu COMMAND

Only for owner/admin.

Show buttons:

----------------------------------------

4.1 SET START POST

- Ask admin to send a message
- Include "Cancel" button
- Save message (store in dataset)

- When any user sends /start:
  → bot replies with saved STARTPOST

----------------------------------------

4.2 SET MESSAGE (SETMSG)

- Same process as STARTPOST

- When user sends ANY message (except /start):
  → bot replies with SETMSG

----------------------------------------

4.3 STATS BUTTON

- On click:
  → show "Processing..."
  → fetch stats
  → display:

- Total users
    - Premium users
    - Non-premium users
    - Blocked users
    - Total admins (with IDs)
    - Total /start count
    - Total messages count

----------------------------------------

4.4 BROADCAST FEATURE 🚀

When admin clicks "BROADCAST":

FLOW:

1. Ask admin to send a message
   - Support: text, photo, video, document, etc.
   - Add "Cancel" button

2. After receiving message:
   - Show confirmation:
     "Are you sure you want to broadcast to all users?"
     Buttons: YES / CANCEL

3. On confirmation:
   - Start broadcasting to ALL users of that assistant bot

----------------------------------------

📊 PROGRESS SYSTEM (IMPORTANT)

- Send initial message:
  "Broadcast started..."

- Then EDIT the SAME message at stages:
  
  ✔ 25% completed  
  ✔ 50% completed  
  ✔ 75% completed  
  ✔ 100% completed  

- DO NOT update for every user (avoid flood limits)

----------------------------------------

⏱ ETA SYSTEM

- While updating progress, show:
  - Sent users count
  - Remaining users
  - Estimated time remaining (ETA)
  - Speed (messages/sec)

Example:
"50% completed  
Sent: 500/1000  
ETA: 20s  
Speed: 25 msg/sec"

----------------------------------------

🚫 ERROR HANDLING DURING BROADCAST

- Handle:
  - Blocked users
  - Deactivated accounts
  - Flood waits
  - Privacy errors

- Maintain counters:
  - success_count
  - failed_count
  - blocked_count

----------------------------------------

✅ FINAL RESULT MESSAGE

After completion:

Show summary:

- Total users
- Successfully sent
- Failed deliveries
- Blocked users
- Total time taken

Example:

"Broadcast Completed ✅

Total Users: 1200  
Sent: 950  
Failed: 100  
Blocked: 150  
Time Taken: 48 seconds"

----------------------------------------

🔷 IMPORTANT TECH NOTES

- Use asyncio for broadcasting queue
- Implement rate limit protection (sleep handling)
- Use batch sending if needed
- Use try/except for each send
- Store updated blocked users count in database

- Broadcast should NOT crash bot
- If interrupted → allow resume (optional advanced)

🔷 ADDITIONAL REQUIREMENTS

- Use Telethon (not Pyrogram)
- Modular code structure
- Multi-session handling (multiple assistant bots simultaneously)
- Proper error handling
- Logging system
- Efficient async handling

- Maintain mapping:
  user_id ↔ assistant bot

- Ensure:
  - No data loss on restart
  - Auto reload sessions from HF dataset
  - Background sync system

----------------------------------------

🔷 BONUS (Optional but Recommended)

- Rate limit handling
- Flood wait handling
- Session validation before activation
- Auto restart assistant on crash
- Inline pagination for large bot lists

----------------------------------------

OUTPUT REQUIREMENTS:

- Full working code (not pseudo code)
- Include:
  - master bot
  - assistant bot handler
  - HF dataset integration
  - session manager
  - command handlers
  - inline button handlers
- Dockerfile for deployment (HuggingFace Spaces compatible)
- .env example
