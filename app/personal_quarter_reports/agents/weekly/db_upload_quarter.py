import os
import json
from pymongo import MongoClient
from collections import defaultdict

# MongoDB 연결 설정
mongo_uri = "mongodb://root:root@13.209.110.151:27017/"
db_name = "skala"
collection_name = "weekly_evaluation_results"
document_key = {"data_type": "personal-quarter"}  # 하나의 document로 통합 저장

# JSON 파일이 저장된 폴더
json_dir = r"D:\Github\ai-server\app\personal_quarter_reports\agents\weekly\output"

# MongoDB 연결
client = MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]

# 모든 사용자 데이터 통합 저장 구조
combined_data = {
    "data_type": "personal-quarter",
    "users": {}
}

# JSON 파일 순회
for filename in os.listdir(json_dir):
    if not filename.endswith(".json"):
        continue

    try:
        file_path = os.path.join(json_dir, filename)
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # 파일명에서 user_id, quarter 추출
        parts = filename.split("_")
        user_id = parts[1]
        quarter = parts[2]

        # 해당 user_id가 없다면 초기화
        if user_id not in combined_data["users"]:
            combined_data["users"][user_id] = {
                "user_id": user_id,
                "name": data.get("name", f"User_{user_id}"),
                "employee_number": data.get("employee_number", user_id),
                "total_activities": 0,
                "quarters": {}
            }

        # 분기 데이터 삽입
        combined_data["users"][user_id]["quarters"][quarter] = data
        combined_data["users"][user_id]["total_activities"] += data.get("total_activities", 0)

        print(f"✅ 처리 완료: {filename}")

    except Exception as e:
        print(f"❌ 처리 실패: {filename} - {e}")

# MongoDB 컬렉션에 단일 문서로 저장
try:
    result = collection.update_one(
        {"data_type": "personal-quarter"},
        {"$set": combined_data},
        upsert=True
    )
    print(f"\n🚀 MongoDB에 단일 통합 document 저장 완료. (matched: {result.matched_count}, modified: {result.modified_count})")
except Exception as e:
    print(f"❌ MongoDB 저장 실패: {e}")

# 연결 종료
client.close()
