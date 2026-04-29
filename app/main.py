import asyncio
import base64
import logging
import sys
from pathlib import Path
from aiohttp import web

from app.bots.master import MasterController
from app.config import Settings
from app.session_manager import SessionManager
from app.storage.hf_dataset import HFDataStore

# Set up specific logger for this file
logger = logging.getLogger("MAIN")

async def health_check(request):
    return web.Response(text="Bot is running and healthy!")

async def start_dummy_server():
    try:
        logger.info("Starting dummy web server on port 7860...")
        app = web.Application()
        app.router.add_get('/', health_check)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 7860)
        await site.start()
        logger.info("Dummy web server started SUCCESSFULLY.")
    except Exception as e:
        logger.error(f"Failed to start dummy server: {e}", exc_info=True)
        raise

async def ensure_master_session(settings: Settings, store: HFDataStore) -> None:
    logger.info("Checking master session state...")
    session_file = Path(settings.master_session_file)
    master = store.get_data().setdefault("master", {})

    if session_file.exists():
        logger.info(f"Local session file found at {session_file}. Backing up to store...")
        master["session_b64"] = base64.b64encode(session_file.read_bytes()).decode("utf-8")
        store.mark_dirty()
        return

    logger.info("No local session file found. Attempting to restore from HF store...")
    raw_b64 = master.get("session_b64")
    if raw_b64:
        logger.info("Base64 session found in store. Decoding and saving to file...")
        session_file.write_bytes(base64.b64decode(raw_b64.encode("utf-8")))
        logger.info("Session restored successfully.")
    else:
        logger.warning("WARNING: No session found in store or locally. A NEW session will be created. (If this is a userbot, it might hang here waiting for a login code!)")

async def main() -> None:
    # Set level to DEBUG and ensure it prints instantly to stdout
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    logger.info("=== Application Startup Sequence Initiated ===")

    # 1. Start the dummy server FIRST to satisfy Hugging Face
    await start_dummy_server()

    try:
        logger.info("Step 1: Loading environment variables...")
        settings = Settings.from_env()
        if not settings.api_id or not settings.api_hash:
            logger.error("CRITICAL: Missing API_ID or API_HASH")
            raise RuntimeError("API_ID and API_HASH are required")
        if not settings.hf_token or not settings.hf_repo_id:
            logger.error("CRITICAL: Missing HF_TOKEN or HF_REPO_ID")
            raise RuntimeError("HF_TOKEN and HF_REPO_ID are required")
        logger.info("Environment variables loaded successfully.")

        logger.info("Step 2: Initializing HFDataStore...")
        store = HFDataStore(settings)
        await store.initialize()
        logger.info("HFDataStore initialized.")

        logger.info("Step 3: Ensuring master session...")
        await ensure_master_session(settings, store)

        logger.info("Step 4: Initializing SessionManager and loading all sessions...")
        sessions = SessionManager(settings, store)
        await sessions.load_all()
        logger.info("All sessions loaded.")

        logger.info("Step 5: Initializing MasterController...")
        master = MasterController(settings, store, sessions)
        
        logger.info("Step 6: Starting MasterController (Connecting to Telegram)...")
        # If it hangs, it will be exactly here.
        await master.start()
        logger.info("MasterController connected to Telegram successfully.")
        
        logger.info("Step 7: Starting HFDataStore auto-sync...")
        await store.start_auto_sync()
        logger.info("Auto-sync started.")

        logger.info("=== SUCCESS: Entering main bot loop. Application is now fully RUNNING. ===")
        await master.run()
        
    except BaseException as e:
        logger.critical(f"FATAL ERROR during startup or execution: {e}", exc_info=True)
    finally:
        logger.info("=== Application Shutting Down ===")
        try:
            await store.sync(force=True)
            await store.stop_auto_sync()
            await sessions.shutdown()
            await master.stop()
            logger.info("Cleanup completed successfully.")
        except Exception as cleanup_error:
            logger.error(f"Error during cleanup: {cleanup_error}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())
    
