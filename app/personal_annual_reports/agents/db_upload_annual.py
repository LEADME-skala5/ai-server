import os
import json
from pymongo import MongoClient

# MongoDB 연결 설정
mongo_uri = "mongodb://root:root@13.209.110.151:27017/"
db_name = "skala"
collection_name = "weekly_evaluation_results"

# JSON 파일이 저장된 폴더
json_dir = r"D:\Github\ai-server\app\personal_annual_reports\agents\output"

# MongoDB 연결
client = MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]

# 모든 사용자 데이터 통합 저장 구조
combined_data = {
    "data_type": "personal-annual",
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

        user_id = str(data.get("user_id"))
        evaluation_year = data.get("evaluation_year", "unknown")
        total_activities = data.get("overall_assessment", {}).get("total_activities", 0)

        # 사용자 정보가 없으면 초기화
        if user_id not in combined_data["users"]:
            combined_data["users"][user_id] = {
                "user_id": user_id,
                "name": data.get("employee_name", f"User_{user_id}"),
                "evaluation_year": evaluation_year,
                "total_activities": 0,
                "annual_report": data  # 전체 json 저장
            }

        # 총 활동 수 누적
        combined_data["users"][user_id]["total_activities"] += total_activities

        print(f"✅ 처리 완료: {filename}")

    except Exception as e:
        print(f"❌ 처리 실패: {filename} - {e}")

# MongoDB에 저장
try:
    result = collection.update_one(
        {"data_type": "personal-annual"},  # 고정 키
        {"$set": combined_data},
        upsert=True
    )
    print(f"\n🚀 MongoDB에 단일 통합 document 저장 완료. (matched: {result.matched_count}, modified: {result.modified_count})")
except Exception as e:
    print(f"❌ MongoDB 저장 실패: {e}")

client.close()
