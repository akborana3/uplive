import os
from dataclasses import dataclass

# Required by product spec: this Telegram user is always super admin.
REQUIRED_SUPER_ADMIN_ID = 8413365423


@dataclass(slots=True)
class Settings:
    api_id: int
    api_hash: str
    hf_token: str
    hf_repo_id: str
    hf_data_path: str
    master_session_file: str
    super_admin_id: int
    auto_sync_interval: int


    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            api_id=int(os.getenv("API_ID", "0")),
            api_hash=os.getenv("API_HASH", ""),
            hf_token=os.getenv("HF_TOKEN", ""),
            hf_repo_id=os.getenv("HF_REPO_ID", ""),
            hf_data_path=os.getenv("HF_DATA_PATH", "database.json"),
            master_session_file=os.getenv("MASTER_SESSION_FILE", "master.session"),
            super_admin_id=REQUIRED_SUPER_ADMIN_ID,
            auto_sync_interval=int(os.getenv("AUTO_SYNC_INTERVAL", "300")),
        )
