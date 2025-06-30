class Config:
    def __init__(self):
        self.OPENAI_API_KEY = "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"
        self.PINECONE_API_KEY = "pcsk_5Wcu2A_7QAdTAjfmSYxwxc2sfiZ7G1bhmi9qy6J2KXL1hUfNcoLQ3xGdavA7S4E9DEqpmH"
        self.MODEL = "gpt-4-turbo"
        self.OUTPUT_PATH = "./output"
        
        # Pinecone 설정
        self.PINECONE_INDEX_NAME = "skore-20250624-144422"
        
        # MariaDB 설정
        self.DB_CONFIG = {
            'host': '13.209.110.151',
            'port': 3306,
            'user': 'root',
            'password': 'root',
            'database': 'skala',
            'charset': 'utf8mb4'
        }
