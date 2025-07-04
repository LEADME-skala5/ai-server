import os
import pymysql
import pandas as pd
import json
from openai import OpenAI
from dotenv import load_dotenv
from sqlalchemy import create_engine
from pathlib import Path
from pymongo import MongoClient
from datetime import datetime

# .env 파일에서 API 키 로드
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# MariaDB 연결 설정
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_NAME"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": True
}

# MongoDB 연결 설정
MONGO_CONFIG = {
    "host": os.getenv("MONGO_HOST"),
    "port": int(os.getenv("MONGO_PORT")),
    "username": os.getenv("MONGO_USER"),
    "password": os.getenv("MONGO_PASSWORD"),
    "db_name": os.getenv("MONGO_DB_NAME")
}

# SQLAlchemy 엔진 생성
SQLALCHEMY_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}?charset={DB_CONFIG['charset']}"
engine = create_engine(SQLALCHEMY_URL)

# Grade → 점수 매핑
GRADE_MAP = {"A": 5, "B": 4, "C": 3, "D": 2, "E": 1}

# DB 컬럼명 → 자연스러운 업무 영역명 매핑
COLUMN_MAPPING = {
    'cross_dept_collab': '부서간 협업',
    'overtime': '업무 몰입도',
    'self_dev': '자기계발',
    'presentation': '발표 및 소통',
    'customer_feedback': '고객 응대',
    'customer_satisfaction': '고객 만족도',
    'internal_training': '사내 교육 참여',
    'mentoring': '멘토링 활동',
    'internal_event': '조직 활동 참여'
    # commit, chat_message, business_trip, overtime 등 정량 지표는 제외
}

# 제외할 정량 지표 컬럼들
EXCLUDED_COLUMNS = ['commit', 'chat_message', 'business_trip']

class MongoDBManager:
    """MongoDB 연결 및 관리 클래스"""
    
    def __init__(self):
        self.mongodb_uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
        self.database_name = MONGO_CONFIG["db_name"]
        self.collection_name = "qualitative_evaluation_results"
        self.client = None
        
        print(f"📋 MongoDB 설정 로드 완료: {MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/{self.database_name}")
    
    def connect(self):
        """MongoDB 연결"""
        try:
            self.client = MongoClient(self.mongodb_uri)
            self.client.admin.command('ping')
            print("✅ MongoDB 연결 성공!")
            return True
        except Exception as e:
            print(f"❌ MongoDB 연결 실패: {e}")
            return False
    
    def add_user_to_quarter_document(self, user_data: dict) -> bool:
        """분기별 문서에 사용자 데이터 추가"""
        try:
            if not self.client:
                if not self.connect():
                    return False
            
            db = self.client[self.database_name]
            collection = db[self.collection_name]
            
            # 해당 분기 문서가 존재하는지 확인
            existing_doc = collection.find_one({
                "type": "personal-quarter",
                "evaluated_year": user_data['year'],
                "evaluated_quarter": user_data['quarter']
            })
            
            if existing_doc:
                # 기존 문서에 사용자 데이터 추가
                collection.update_one(
                    {
                        "type": "personal-quarter",
                        "evaluated_year": user_data['year'],
                        "evaluated_quarter": user_data['quarter']
                    },
                    {
                        "$push": {"users": user_data},
                        "$set": {"updated_at": datetime.now()},
                        "$inc": {"user_count": 1}
                    }
                )
                print(f"✅ 기존 분기 문서에 사용자 ID {user_data['user_id']} 추가 완료")
            else:
                # 새로운 분기 문서 생성
                quarter_document = {
                    "type": "personal-quarter",
                    "evaluated_year": user_data['year'],
                    "evaluated_quarter": user_data['quarter'],
                    "user_count": 1,
                    "users": [user_data],
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                }
                
                result = collection.insert_one(quarter_document)
                print(f"✅ 새로운 분기 문서 생성 및 사용자 ID {user_data['user_id']} 추가 완료 - Document ID: {result.inserted_id}")
            
            return True
            
        except Exception as e:
            print(f"❌ MongoDB 사용자 데이터 추가 실패 (사용자 ID: {user_data.get('user_id', 'unknown')}): {e}")
            return False
    
    def close(self):
        """MongoDB 연결 종료"""
        if self.client:
            self.client.close()
            print("MongoDB 연결 종료")

def map_column_names_to_korean(row):
    """DB 컬럼명을 자연스러운 한국어 업무 영역명으로 매핑"""
    grade_cols = [c for c in row.index if c.endswith('_grade')]
    mapped_grades = []
    
    for col in grade_cols:
        base_col = col.replace('_grade', '')
        
        # 제외할 컬럼은 건너뛰기
        if base_col in EXCLUDED_COLUMNS:
            continue
            
        # 매핑된 한국어명이 있으면 사용, 없으면 원본 사용
        korean_name = COLUMN_MAPPING.get(base_col, base_col)
        grade = row[col]
        
        if grade and grade in GRADE_MAP:
            mapped_grades.append(f"{korean_name}: {grade}")
    
    return "\n".join([f"- {grade}" for grade in mapped_grades])

# 점수 산정 함수
def calculate_total_score(row):
    grade_cols = [c for c in row.index if c.endswith('_grade')]
    grades = [GRADE_MAP.get(row[c], None) for c in grade_cols]
    grades = [g for g in grades if g is not None]
    if not grades:
        return None
    avg_grade = sum(grades) / len(grades)

    diff = row.get('project_difficulty', 3) or 3
    difficulty_boost = 0.95 + 0.05 * (1 - 0.05 * (5 - diff))

    comp = row.get('deadline_compliance', 0) or 0
    compliance_boost = 0.9 + (comp / 100) * 0.1

    return round(avg_grade * difficulty_boost * compliance_boost, 2)

# user_quarter_scores 테이블에 점수 업데이트 또는 추가하는 함수
def update_or_insert_final_score(conn, user_id, eval_year, eval_quarter, qualitative_score):
    with conn.cursor() as cur:
        # 기존 데이터 확인
        cur.execute(
            """SELECT id FROM user_quarter_scores 
               WHERE user_id = %s AND evaluation_year = %s AND evaluation_quarter = %s""",
            (user_id, eval_year, eval_quarter)
        )
        existing_record = cur.fetchone()
        
        if existing_record:
            # 기존 데이터 업데이트
            cur.execute(
                """UPDATE user_quarter_scores 
                   SET qualitative_score = %s 
                   WHERE user_id = %s AND evaluation_year = %s AND evaluation_quarter = %s""",
                (qualitative_score, user_id, eval_year, eval_quarter)
            )
            print(f"✅ user_quarter_scores 업데이트 완료: 사용자 ID {user_id}")
        else:
            # 새 데이터 추가
            cur.execute(
                """INSERT INTO user_quarter_scores (user_id, evaluation_year, evaluation_quarter, qualitative_score)
                   VALUES (%s, %s, %s, %s)""",
                (user_id, eval_year, eval_quarter, qualitative_score)
            )
            print(f"✅ user_quarter_scores 새 데이터 추가 완료: 사용자 ID {user_id}")

# JSON 형태로 결과를 저장하는 함수
def save_evaluation_as_json(user_id, eval_year, eval_quarter, qualitative_score, work_attitude, output_dir="output"):
    # 출력 디렉토리 생성
    Path(output_dir).mkdir(exist_ok=True)
    
    # JSON 데이터 구성
    evaluation_data = {
        "data": {
            "user_id": user_id,
            "year": eval_year,
            "quarter": eval_quarter,
            "qualitative_score": qualitative_score
        },
        "workAttitude": work_attitude
    }
    
    # 파일명 생성
    filename = f"{output_dir}/evaluation_user_{user_id}_{eval_year}Q{eval_quarter}.json"
    
    # JSON 파일로 저장
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(evaluation_data, f, ensure_ascii=False, indent=2)
    
    print(f"📄 JSON 파일 저장 완료: {filename}")
    
    return filename

def process_single_quarter_qualitative(mongodb_manager, eval_year, eval_quarter):
    """단일 분기 정성 평가 처리 - 개선된 프롬프트 적용"""
    print(f"\n=== {eval_year}년 {eval_quarter}분기 정성 평가 처리 시작 ===")
    print(f"개선사항: DB 컬럼명 → 한국어 매핑, 자연스러운 표현, LLM 종합 평가")
    print("=" * 50)
    
    # 1) DB에서 해당 년도 및 분기의 평가 데이터 로드
    df = pd.read_sql(
        f"SELECT * FROM user_qualitative_evaluations WHERE evaluation_year = {eval_year} AND evaluation_quarter = {eval_quarter}",
        engine
    )

    if df.empty:
        print(f"⚠️ {eval_year}년 {eval_quarter}분기 평가 데이터가 없습니다.")
        return {
            "quarter": eval_quarter,
            "successful_count": 0,
            "failed_count": 0,
            "average_score": 0
        }

    # 2) 총점 계산 및 업데이트
    df['total_score'] = df.apply(calculate_total_score, axis=1)
    conn = pymysql.connect(**DB_CONFIG)
    
    successful_count = 0
    failed_count = 0
    scores = []  # 통계용
    
    try:
        with conn.cursor() as cur:
            for _, row in df.dropna(subset=['total_score']).iterrows():
                cur.execute(
                    "UPDATE user_qualitative_evaluations SET total_score = %s WHERE id = %s",
                    (row['total_score'], row['id'])
                )

        # 3) 평가 생성 대상 필터링
        to_generate = df.dropna(subset=['total_score'])
        print(f"처리할 사용자 수: {len(to_generate)}명")

        # 4) LLM 호출 및 평가 생성 - 개별 처리
        for i, (_, row) in enumerate(to_generate.iterrows(), 1):
            # 진행률 표시 (매 10명마다)
            if i % 10 == 0 or i == len(to_generate):
                print(f"처리 진행률: {i}/{len(to_generate)} ({i/len(to_generate)*100:.1f}%)")
            
            try:
                # 컬럼명을 한국어로 매핑하여 프롬프트용 문자열 생성
                mapped_score_str = map_column_names_to_korean(row)

                # 개선된 프롬프트 (종합 평가 자유도 확대)
                prompt = f"""
다음은 한 직원의 업무 실행 및 태도 평가를 위한 데이터입니다.

■ 등급 기준:
- A: 상위 20% 이내 (우수)
- B: 상위 21~40% 이내 (양호)
- C: 상위 41~60% 이내 (보통)
- D: 상위 61~80% 이내 (개선 필요)
- E: 상위 81~100% 이내 (시급한 개선 필요)

■ 평가 항목별 등급：
{mapped_score_str}

■ 작성 규칙:
1. 등급별로 해당 항목들을 그룹화하여 문장 생성:
   - A등급: "상위 20% 이내로 탁월한 성과를 보입니다"
   - B등급: "상위 21~40% 이내로 우수한 수준을 보입니다"  
   - C등급: "상위 41~60% 이내로 보완이 필요합니다"
   - D등급: "상위 61~80% 이내로 개선이 필요합니다"
   - E등급: "상위 81~100% 이내로 시급한 개선이 필요합니다"

2. 존재하는 등급에 대해서만 문장을 생성하세요

3. 마지막에 정확히 2문장으로 평가를 완료하세요 (반드시 2문장만 작성):
   - 첫 번째 문장: 주요 강점을 창의적으로 요약하여 한 문장으로 표현 (단순 항목명 나열 금지)
     예: "고객 대응 역량과 학습 의지에서 탁월한 강점을 보입니다"
     예: "대인관계 중심 업무에서 우수한 성과를 발휘하고 있습니다"
   - 두 번째 문장: 반드시 "다만,"으로 시작하여 개선 영역 제시 후 "~할 것으로 보입니다" 또는 "~할 수 있을 것입니다"로 마무리
   - 추가 문장 작성 절대 금지 (정확히 2문장만)
   - 인사말이나 격려 멘트 추가 금지

4. 전체 평가문은 등급별 문장 + 마무리 2문장으로 구성하고, 격식 있는 종결형(~입니다)으로 작성하세요

5. 다음 사항은 금지됩니다:
   - 점수 수치 또는 A~E 등급 기호의 언급
   - "일부 항목", "전반적으로" 같은 불분명한 표현 (종합 평가 제외)

**결과는 각 문장을 줄 바꿈으로 구분하여 반환하십시오.**
"""

                response = client.chat.completions.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0
                )
                eval_text = response.choices[0].message.content.strip()
                
                # 평가문을 문장별로 분리 (리스트 형태)
                work_attitude = [sentence.strip() for sentence in eval_text.split('\n') if sentence.strip()]

                # 생성된 평가문 터미널에 출력
                print(f"\n=== 사용자 ID: {row['user_id']}, 평가 ID: {row['id']} ===")
                print(f"정성 평가 총점: {row['total_score']}점")
                print("생성된 정성 평가문:")
                print("-" * 50)
                print(eval_text)
                print("-" * 50)
                
                # 5) JSON 형태로 결과 저장
                json_filename = save_evaluation_as_json(
                    user_id=row['user_id'],
                    eval_year=eval_year,
                    eval_quarter=eval_quarter,
                    qualitative_score=row['total_score'],
                    work_attitude=work_attitude
                )
                
                # 6) user_quarter_scores 테이블에 점수 업데이트 또는 추가
                update_or_insert_final_score(
                    conn=conn,
                    user_id=row['user_id'],
                    eval_year=eval_year,
                    eval_quarter=eval_quarter,
                    qualitative_score=row['total_score']
                )
                
                # 7) MongoDB에 사용자 데이터 추가
                user_evaluation_data = {
                    "user_id": row['user_id'],
                    "year": eval_year,
                    "quarter": eval_quarter,
                    "qualitative_score": row['total_score'],
                    "work_attitude": work_attitude,
                    "evaluation_text": eval_text,
                    "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                mongodb_save_success = mongodb_manager.add_user_to_quarter_document(user_evaluation_data)
                
                if mongodb_save_success:
                    print(f"✅ 사용자 ID {row['user_id']} qualitative_evaluation_results 컬렉션에 추가 완료")
                else:
                    print(f"❌ 사용자 ID {row['user_id']} MongoDB 저장 실패 - JSON 백업만 유지")
                
                successful_count += 1
                scores.append(row['total_score'])
                print(f"✓ User {row['user_id']}: {row['total_score']:.2f}/5.0 → MongoDB 저장 완료")
                
            except Exception as e:
                print(f"❌ 사용자 ID {row['user_id']} 처리 실패: {e}")
                failed_count += 1

        # 통계 계산 및 출력
        print(f"\n=== {eval_quarter}분기 정성 평가 처리 완료 ===")
        print(f"성공: {successful_count}명 → qualitative_evaluation_results 컬렉션에 저장 완료")
        print(f"실패: {failed_count}명")
        
        avg_score = None
        if scores:
            avg_score = sum(scores) / len(scores)
            max_score = max(scores)
            min_score = min(scores)
            
            print(f"평균 점수: {avg_score:.2f}/5.0")
            print(f"최고 점수: {max_score:.2f}/5.0")
            print(f"최저 점수: {min_score:.2f}/5.0")
        
        # 실패한 사용자 개수만 출력
        if failed_count > 0:
            print(f"데이터가 없거나 처리 실패한 사용자: {failed_count}명")
        
        return {
            "quarter": eval_quarter,
            "successful_count": successful_count,
            "failed_count": failed_count,
            "average_score": round(avg_score, 2) if avg_score else 0
        }

    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        conn.rollback()
        return {
            "quarter": eval_quarter,
            "successful_count": 0,
            "failed_count": 0,
            "average_score": 0
        }
    finally:
        conn.close()

def main():
    print("🚀 개선된 정성 평가 처리 시작")
    print("=" * 60)
    print("개선사항:")
    print("✅ DB 컬럼명 → 자연스러운 한국어 업무 영역명 매핑")
    print("✅ 정량 지표(commit, chat_message, business_trip) 제외")
    print("✅ 자연스러운 표현으로 변경")
    print("✅ LLM 기반 전반적 종합 평가 추가")
    print("=" * 60)
    
    # MongoDB 매니저 초기화
    mongodb_manager = MongoDBManager()
    if not mongodb_manager.connect():
        print("❌ MongoDB 연결 실패. 로컬 JSON 저장만 진행합니다.")
    
    # 전체 결과 저장용
    all_quarters_results = {}
    
    print(f"\n=== 2024년 전체 분기 정성 평가 배치 처리 시작 ===")
    print(f"저장 방식: qualitative_evaluation_results 컬렉션에 type: 'personal-quarter'로 구분")
    print(f"저장 위치: MongoDB - {MONGO_CONFIG['db_name']}.qualitative_evaluation_results")
    print("=" * 60)
    
    # 4개 분기 모두 처리
    for quarter in [1, 2, 3, 4]:
        quarter_result = process_single_quarter_qualitative(mongodb_manager, 2024, quarter)
        all_quarters_results[f"Q{quarter}"] = quarter_result
        
        # 분기 간 구분을 위한 여백
        print("\n" + "=" * 60)
    
    # 전체 분기 통합 결과 출력
    print(f"\n=== 2024년 전체 분기 정성 평가 처리 완료 ===")
    
    total_processed = 0
    for quarter in [1, 2, 3, 4]:
        if f"Q{quarter}" in all_quarters_results:
            quarter_data = all_quarters_results[f"Q{quarter}"]
            successful = quarter_data["successful_count"]
            total_processed += successful
            print(f"Q{quarter}: 성공 {successful}명 → 개선된 프롬프트로 자연스러운 평가문 생성")
        else:
            print(f"Q{quarter}: 데이터 없음")
    
    print(f"\n🎉 처리 완료 요약:")
    print(f"  - 총 처리된 사용자: {total_processed}명")
    print(f"  - 주요 개선사항: DB 컬럼명 한국어 매핑, 자연스러운 표현, LLM 종합 평가")
    print(f"  - 저장 위치: {MONGO_CONFIG['db_name']}.qualitative_evaluation_results")
    print(f"  - MariaDB user_quarter_scores.qualitative_score 업데이트 완료")
    
    # MongoDB 연결 종료
    mongodb_manager.close()

if __name__ == '__main__':
    main()