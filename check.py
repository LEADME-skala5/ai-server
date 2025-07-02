import os
from dotenv import load_dotenv
from pinecone import Pinecone, ServerlessSpec

load_dotenv()

api_key = os.getenv("PINECONE_API_KEY")
if not api_key:
    raise ValueError("❌ PINECONE_API_KEY 환경변수가 없습니다")

# Pinecone 객체 생성
pc = Pinecone(api_key=api_key)

# 인덱스 확인 (예: skore-20250624-144422가 존재하는지)
index_name = os.getenv("PINECONE_INDEX_NAME")
indexes = pc.list_indexes().names()
print("📦 존재하는 인덱스 목록:", indexes)

if index_name not in indexes:
    print(f"❌ 인덱스 '{index_name}'가 존재하지 않습니다.")
else:
    print(f"✅ 인덱스 '{index_name}'가 존재합니다.")
