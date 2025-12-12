import os


class Settings:
    def __init__(self) -> None:
        self.model_name = os.getenv("MODEL_NAME", "BAAI/bge-m3")
        self.device = os.getenv("DEVICE", "cpu")
        self.max_length = int(os.getenv("MAX_TOKEN_LENGTH", "256"))
        self.batch_size = int(os.getenv("BATCH_SIZE", "16"))
        self.nsfw_keywords = os.getenv(
            "NSFW_KEYWORDS",
            "色情,性,裸体,成人,av,sm,淫,约炮,开房,强奸,rape,porn,nude",
        ).split(",")
        self.seed = int(os.getenv("MODEL_SEED", "42"))


settings = Settings()
