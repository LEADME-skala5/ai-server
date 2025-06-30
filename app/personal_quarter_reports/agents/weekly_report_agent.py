import json
import pandas as pd
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime
import logging
import openai
import os
import pymysql
from pymongo import MongoClient

from weekly_evaluations import (
    get_average_grade,
    get_weighted_workload_score,
    calculate_final_score
)

try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ .env 파일 로드 완료")
except ImportError:
    print("⚠️ python-dotenv 패키지가 설치되지 않음 - pip install python-dotenv")
    pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoDBManager:
    """MongoDB 연결 및 관리 클래스"""
    
    def __init__(self):
        self.mongodb_uri = f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/"
        self.database_name = os.getenv("MONGO_DB_NAME")
        self.input_collection_name = "weekly_evaluation_results"  # 입력 컬렉션
        self.output_collection_name = "weekly_combination_results"  # 출력 컬렉션
        self.client = None
        
        print(f"📋 MongoDB 설정 로드 완료: {os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/{self.database_name}")
    
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
    
    def get_quarter_evaluation_data(self, year: int, quarter: int) -> List[Dict]:
        """특정 분기의 weekly_evaluation_results 데이터 조회"""
        try:
            if not self.client:
                if not self.connect():
                    return []
            
            db = self.client[self.database_name]
            collection = db[self.input_collection_name]
            
            # data_type이 "personal-quarter"인 문서 조회
            query = {"data_type": "personal-quarter"}
            
            document = collection.find_one(query)
            
            if document and "users" in document:
                users_data = []
                quarter_key = f"{year}Q{quarter}"
                
                # 각 사용자의 특정 분기 데이터 추출
                for user_id, user_info in document["users"].items():
                    if "quarters" in user_info and quarter_key in user_info["quarters"]:
                        quarter_data = user_info["quarters"][quarter_key]
                        
                        # 사용자 데이터 구성
                        user_data = {
                            "user_id": int(user_id),
                            "user_name": user_info.get("name", f"User_{user_id}"),
                            "year": year,
                            "quarter": quarter,
                            "quarter_data": quarter_data,
                            "team_goals": quarter_data.get("teamGoals", []),
                            "total_activities": user_info.get("total_activities", 0)
                        }
                        users_data.append(user_data)
                
                print(f"✅ {year}년 {quarter}분기 사용자 데이터 {len(users_data)}개 조회 완료")
                return users_data
            else:
                print(f"⚠️ {year}년 {quarter}분기 데이터 없음")
                return []
                
        except Exception as e:
            print(f"❌ MongoDB 데이터 조회 실패 ({year}년 {quarter}분기): {e}")
            return []
    
    def save_quarter_combination_results(self, year: int, quarter: int, users_data: List[Dict]) -> bool:
        """분기별 조합 결과를 weekly_combination_results에 저장"""
        try:
            if not self.client:
                if not self.connect():
                    return False
            
            db = self.client[self.database_name]
            collection = db[self.output_collection_name]
            
            # 기존 문서가 있는지 확인
            existing_doc = collection.find_one({
                "type": "personal-quarter",
                "evaluated_year": year,
                "evaluated_quarter": quarter
            })
            
            quarter_document = {
                "type": "personal-quarter",
                "evaluated_year": year,
                "evaluated_quarter": quarter,
                "user_count": len(users_data),
                "users": users_data,
                "updated_at": datetime.now()
            }
            
            if existing_doc:
                # 기존 문서 업데이트
                collection.replace_one(
                    {"type": "personal-quarter", "evaluated_year": year, "evaluated_quarter": quarter},
                    quarter_document
                )
                print(f"✅ {year}년 {quarter}분기 조합 결과 업데이트 완료 - {len(users_data)}명")
            else:
                # 새 문서 생성
                quarter_document["created_at"] = datetime.now()
                result = collection.insert_one(quarter_document)
                print(f"✅ {year}년 {quarter}분기 조합 결과 신규 생성 완료 - {len(users_data)}명, Document ID: {result.inserted_id}")
            
            return True
            
        except Exception as e:
            print(f"❌ MongoDB 저장 실패 ({year}년 {quarter}분기): {e}")
            return False
    
    def close(self):
        """MongoDB 연결 종료"""
        if self.client:
            self.client.close()
            print("MongoDB 연결 종료")

class WeeklyReportAgent:
    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(".env 파일에 OPENAI_API_KEY가 없습니다.")

        masked_key = api_key[:10] + "*" * (len(api_key) - 15) + api_key[-5:]
        print(f"🔑 OpenAI API 키 로드 완료: {masked_key}")
        self.client = openai.OpenAI(api_key=api_key)
        
        # MariaDB 연결 설정
        self.db_config = {
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT")),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "db": os.getenv("DB_NAME"),
            "charset": "utf8mb4",
            "cursorclass": pymysql.cursors.DictCursor,
            "autocommit": True
        }
        
        # MongoDB 매니저 초기화
        self.mongodb_manager = MongoDBManager()
    
    def update_weekly_score_in_db(self, user_id: int, evaluation_year: int, evaluation_quarter: int, weekly_score: float):
        """user_quarter_scores 테이블의 weekly_score 컬럼 업데이트"""
        conn = pymysql.connect(**self.db_config)
        try:
            with conn.cursor() as cur:
                # 기존 데이터 확인
                cur.execute(
                    """SELECT id FROM user_quarter_scores 
                       WHERE user_id = %s AND evaluation_year = %s AND evaluation_quarter = %s""",
                    (user_id, evaluation_year, evaluation_quarter)
                )
                existing_record = cur.fetchone()
                
                if existing_record:
                    # 기존 데이터 업데이트
                    cur.execute(
                        """UPDATE user_quarter_scores 
                           SET weekly_score = %s, updated_at = CURRENT_TIMESTAMP
                           WHERE user_id = %s AND evaluation_year = %s AND evaluation_quarter = %s""",
                        (weekly_score, user_id, evaluation_year, evaluation_quarter)
                    )
                    print(f"  💾 MariaDB 업데이트 완료: user_quarter_scores.weekly_score = {weekly_score}")
                else:
                    # 새 데이터 추가
                    cur.execute(
                        """INSERT INTO user_quarter_scores (user_id, evaluation_year, evaluation_quarter, weekly_score, created_at, updated_at)
                           VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""",
                        (user_id, evaluation_year, evaluation_quarter, weekly_score)
                    )
                    print(f"  📝 MariaDB 신규 추가 완료: user_quarter_scores에 user_id {user_id} 데이터 생성")
                    
        except Exception as e:
            print(f"  ❌ MariaDB 업데이트 오류 (user_id: {user_id}): {e}")
        finally:
            conn.close()
    
    def get_user_name(self, user_id: int) -> str:
        """MariaDB users 테이블에서 사용자 이름 조회"""
        conn = pymysql.connect(**self.db_config)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM users WHERE id = %s", (user_id,))
                result = cur.fetchone()
                
                if result:
                    return result['name']
                else:
                    print(f"⚠️ 사용자 ID {user_id}의 이름을 찾을 수 없음")
                    return f"User_{user_id}"
                    
        except Exception as e:
            print(f"❌ 사용자 이름 조회 오류 (ID: {user_id}): {e}")
            return f"User_{user_id}"
        finally:
            conn.close()

    def generate_enhanced_activity_summary(self, user_data: Dict, user_name: str) -> str:
        """MongoDB의 활동 데이터를 기반으로 강화된 요약문 생성"""
        try:
            # 사용자 데이터에서 활동 정보 추출
            quarter_data = user_data.get('quarter_data', {})
            team_goals = user_data.get('team_goals', [])
            total_activities = user_data.get('total_activities', 0)
            
            # 팀 목표별 기여도 분석
            active_goals = []
            total_contributions = 0
            
            for goal in team_goals:
                if goal.get('contributionCount', 0) > 0:
                    goal_name = goal.get('goalName', '')
                    count = goal.get('contributionCount', 0)
                    contents = goal.get('contents', [])
                    
                    # 각 content의 reference 정보 추출
                    goal_details = []
                    for content in contents:
                        description = content.get('description', '')
                        references = content.get('reference', [])
                        
                        # reference 정보를 문자열로 변환
                        ref_labels = []
                        for ref in references:
                            label = ref.get('label', '')
                            if label:
                                ref_labels.append(label)
                        
                        goal_details.append({
                            'description': description,
                            'references': ref_labels
                        })
                    
                    active_goals.append({
                        'name': goal_name,
                        'count': count,
                        'details': goal_details
                    })
                    total_contributions += count
            
            # 부서 정보
            department = quarter_data.get('user', {}).get('department', '부서 미지정')
            
            prompt = f"""
다음은 {user_name} 직원의 {user_data.get('year', 2024)}년 {user_data.get('quarter', 1)}분기 업무 활동 데이터입니다.

■ 기본 정보:
- 소속: {department}
- 총 활동 수: {total_activities}건
- 목표 기여 총합: {total_contributions}건

■ 팀 목표별 기여 현황:
"""
            
            if active_goals:
                for goal in active_goals:
                    prompt += f"- {goal['name']}: {goal['count']}건\n"
                    for detail in goal['details']:
                        desc = detail.get('description', '')
                        refs = detail.get('references', [])
                        if desc:
                            prompt += f"  · {desc}\n"
                            if refs:
                                prompt += f"    (출처: {', '.join(refs)})\n"
            else:
                prompt += "- 활성화된 목표 없음\n"
            
            prompt += f"""
위 데이터를 바탕으로 다음 요구사항에 맞는 업무 요약문을 작성해주세요:

**작성 요구사항:**
1. {user_name} 직원의 분기별 주요 업무 성과와 활동을 구체적으로 기술
2. 팀 목표 기여도와 구체적인 업무 내용을 반영하되, **각 기여의 출처(주차별 보고서 등)를 반드시 포함**
3. 업무 수행 역량과 성과에 대한 종합적 평가 포함
4. 전문적이고 객관적인 어조로 작성 (200-250자 내외)
5. "{user_name} 직원은 {user_data.get('year', 2024)}년 {user_data.get('quarter', 1)}분기 동안..." 형식으로 시작

**중요 지침:**
- 각 목표별 기여 건수를 언급할 때 반드시 해당 업무의 출처 정보를 함께 기재
- 예: "CSP 파트너쉽 강화에 3건 기여(10월 1주차, 10월 4주차 보고서 기반)"
- 구체적인 업무 내용과 그 근거가 되는 보고서를 명확히 연결

**금지사항:**
- 구체적인 점수나 등급 언급 금지
- 추상적이거나 모호한 표현 지양
- 출처 없는 기여도 언급 금지
"""

            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "당신은 HR 전문가로서 직원 평가 요약문을 작성하는 전문가입니다."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=500
            )
            
            summary_text = response.choices[0].message.content.strip()
            print(f"✅ {user_name} (ID: {user_data.get('user_id')}) 요약문 생성 완료")
            print(f"📄 요약문 미리보기: {summary_text[:100]}{'...' if len(summary_text) > 100 else ''}")
            return summary_text
            
        except Exception as e:
            print(f"❌ 요약문 생성 실패 (사용자 ID: {user_data.get('user_id')}): {e}")
            year = user_data.get('year', 2024)
            quarter = user_data.get('quarter', 1)
            return f"{user_name} 직원은 {year}년 {quarter}분기 동안 {user_data.get('total_activities', 0)}건의 업무를 수행했습니다."

    def process_single_quarter_combination(self, year: int, quarter: int) -> Dict:
        """단일 분기 조합 결과 처리"""
        print(f"\n=== {year}년 {quarter}분기 주간 평가 조합 처리 시작 ===")
        print(f"입력: MongoDB weekly_evaluation_results (data_type: personal-quarter)")
        print(f"출력: MongoDB weekly_combination_results")
        print("=" * 50)
        
        # 1. MongoDB에서 해당 분기 데이터 조회
        quarter_users_data = self.mongodb_manager.get_quarter_evaluation_data(year, quarter)
        
        if not quarter_users_data:
            print(f"⚠️ {year}년 {quarter}분기 데이터가 없습니다.")
            return {
                "quarter": quarter,
                "successful_count": 0,
                "failed_count": 0,
                "average_score": 0
            }
        
        print(f"처리할 사용자 수: {len(quarter_users_data)}명")
        
        # 2. 각 사용자별로 처리
        processed_users = []
        successful_count = 0
        failed_count = 0
        weekly_scores = []
        
        for i, user_data in enumerate(quarter_users_data, 1):
            # 진행률 표시 (매 10명마다)
            if i % 10 == 0 or i == len(quarter_users_data):
                print(f"처리 진행률: {i}/{len(quarter_users_data)} ({i/len(quarter_users_data)*100:.1f}%)")
            
            try:
                user_id = user_data.get('user_id')
                if not user_id:
                    print(f"❌ 사용자 ID가 없는 데이터 건너뜀")
                    failed_count += 1
                    continue
                
                # 3. 사용자 이름 조회 (MariaDB에서 실제 이름 가져오기)
                user_name = self.get_user_name(user_id)
                
                # 4. weekly_score 계산 (weekly_evaluations.py 사용)
                try:
                    print(f"🔍 점수 계산 시작: user_id={user_id}, year={year}, quarter={quarter}")
                    
                    avg_score = get_average_grade(user_id, year, quarter)
                    print(f"  - 평균 점수: {avg_score} (타입: {type(avg_score)})")
                    
                    workload_score = get_weighted_workload_score(user_id, year, quarter)
                    print(f"  - 업무량 점수: {workload_score} (타입: {type(workload_score)})")
                    
                    weekly_score = calculate_final_score(avg_score, workload_score)
                    print(f"  - 최종 점수: {weekly_score} (타입: {type(weekly_score)})")
                    
                    # Decimal을 float로 변환
                    avg_score = float(avg_score) if avg_score is not None else 0.0
                    workload_score = float(workload_score) if workload_score is not None else 0.0
                    weekly_score = float(weekly_score) if weekly_score is not None else 0.0
                    
                    print(f"  - 변환된 최종 점수: {weekly_score}")
                    
                    if weekly_score == 0.0:
                        print(f"  ⚠️ 점수가 0 - weekly_evaluations 테이블에 user_id {user_id}의 {year}년 {quarter}분기 데이터가 없을 수 있습니다")
                    
                except Exception as score_error:
                    print(f"❌ 사용자 ID {user_id} 점수 계산 실패: {score_error}")
                    import traceback
                    traceback.print_exc()
                    weekly_score = 0.0
                
                # 5. AI 강화된 요약문 생성 (실제 이름 사용)
                weekly_summary_text = self.generate_enhanced_activity_summary(user_data, user_name)
                
                # 6. 결과 데이터 구성
                processed_user = {
                    "user_id": user_id,
                    "user_name": user_name,
                    "year": year,
                    "quarter": quarter,
                    "weekly_score": weekly_score,
                    "weekly_summary_text": weekly_summary_text
                }
                
                # 7. MariaDB user_quarter_scores 테이블에 weekly_score 업데이트
                self.update_weekly_score_in_db(user_id, year, quarter, weekly_score)
                
                # 생성된 요약문을 터미널에 출력
                print(f"\n=== 🎯 {user_name} (ID: {user_id}) 요약문 ===")
                print(f"📊 Weekly Score: {weekly_score:.2f}")
                print(f"📝 요약문:")
                print("-" * 60)
                print(weekly_summary_text)
                print("-" * 60)
                
                processed_users.append(processed_user)
                successful_count += 1
                weekly_scores.append(weekly_score)
                
                print(f"✓ {user_name} (ID: {user_id}): weekly_score={weekly_score:.2f} → 처리 완료")
                print(f"  📋 팀 목표 기여: {len([g for g in user_data.get('team_goals', []) if g.get('contributionCount', 0) > 0])}/{len(user_data.get('team_goals', []))}개 활성화")
                
            except Exception as e:
                print(f"❌ 사용자 ID {user_data.get('user_id', 'unknown')} 처리 실패: {e}")
                failed_count += 1
        
        # 7. MongoDB에 저장
        if processed_users:
            save_success = self.mongodb_manager.save_quarter_combination_results(year, quarter, processed_users)
            if save_success:
                print(f"📊 MongoDB 저장 완료: weekly_combination_results.{year}Q{quarter}")
            else:
                print(f"❌ {year}년 {quarter}분기 MongoDB 저장 실패")
        
        # 8. MariaDB 업데이트 통계
        print(f"\n💾 MariaDB user_quarter_scores 업데이트:")
        print(f"  - {year}년 {quarter}분기 총 {successful_count}명의 weekly_score 업데이트 완료")
        if failed_count > 0:
            print(f"  - 실패: {failed_count}명")
        
        # 통계 계산 및 출력
        print(f"\n=== {quarter}분기 조합 처리 완료 ===")
        print(f"성공: {successful_count}명 → weekly_combination_results에 저장 완료")
        print(f"실패: {failed_count}명")
        
        avg_score = None
        if weekly_scores:
            avg_score = sum(weekly_scores) / len(weekly_scores)
            max_score = max(weekly_scores)
            min_score = min(weekly_scores)
            
            print(f"평균 weekly_score: {avg_score:.2f}")
            print(f"최고 weekly_score: {max_score:.2f}")
            print(f"최저 weekly_score: {min_score:.2f}")
        
        return {
            "quarter": quarter,
            "successful_count": successful_count,
            "failed_count": failed_count,
            "average_score": round(avg_score, 2) if avg_score else 0
        }

def main():
    print("🚀 주간 평가 조합 결과 생성 시작 (MongoDB 기반)")
    print("=" * 60)
    
    # 에이전트 초기화
    agent = WeeklyReportAgent()
    
    # MongoDB 연결 테스트
    print("🔌 MongoDB 연결 테스트...")
    if not agent.mongodb_manager.connect():
        print("❌ MongoDB 연결 실패. 프로그램을 종료합니다.")
        return
    
    print(f"\n=== 2024년 전체 분기 조합 처리 시작 ===")
    print(f"입력 소스: MongoDB - {os.getenv('MONGO_DB_NAME')}.weekly_evaluation_results")
    print(f"출력 대상: MongoDB - {os.getenv('MONGO_DB_NAME')}.weekly_combination_results")
    print(f"처리 방식: 분기별 문서에 모든 사용자 조합 결과 저장")
    print("=" * 60)
    
    # 전체 결과 저장용
    all_quarters_results = {}
    
    # 4개 분기 모두 처리
    for quarter in [1, 2, 3, 4]:
        quarter_result = agent.process_single_quarter_combination(2024, quarter)
        all_quarters_results[f"Q{quarter}"] = quarter_result
        
        # 분기 간 구분을 위한 여백
        print("\n" + "=" * 60)
    
    # 전체 분기 통합 결과 출력
    print(f"\n=== 2024년 전체 분기 조합 처리 완료 ===")
    
    total_processed = 0
    for quarter in [1, 2, 3, 4]:
        if f"Q{quarter}" in all_quarters_results:
            quarter_data = all_quarters_results[f"Q{quarter}"]
            successful = quarter_data["successful_count"]
            total_processed += successful
            avg_score = quarter_data["average_score"]
            print(f"Q{quarter}: 성공 {successful}명, 평균 점수 {avg_score}")
        else:
            print(f"Q{quarter}: 데이터 없음")
    
    print(f"\n🎉 처리 완료 요약:")
    print(f"  - 총 처리된 사용자: {total_processed}명")
    print(f"  - 입력: weekly_evaluation_results (data_type: personal-quarter)")
    print(f"  - 출력: weekly_combination_results (type: personal-quarter)")
    print(f"  - 저장 방식: 분기별 문서에 모든 사용자 조합 결과 저장")
    print(f"  - 데이터베이스: {os.getenv('MONGO_DB_NAME')}")
    print(f"  - 총 문서 수: 4개 (2024년 1,2,3,4분기)")
    print(f"  - 💾 MariaDB user_quarter_scores.weekly_score 업데이트: 총 {total_processed}명")
    
    # MariaDB 업데이트 결과 검증
    print(f"\n🔍 MariaDB 업데이트 검증:")
    try:
        conn = pymysql.connect(**agent.db_config)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    evaluation_quarter,
                    COUNT(*) as updated_count,
                    AVG(weekly_score) as avg_score,
                    MIN(weekly_score) as min_score,
                    MAX(weekly_score) as max_score
                FROM user_quarter_scores 
                WHERE evaluation_year = 2024 AND weekly_score IS NOT NULL
                GROUP BY evaluation_quarter
                ORDER BY evaluation_quarter
            """)
            results = cur.fetchall()
            
            for row in results:
                quarter, count, avg_score, min_score, max_score = row
                print(f"  Q{quarter}: {count}명 업데이트, 평균 {avg_score:.2f} (범위: {min_score:.2f}~{max_score:.2f})")
        conn.close()
    except Exception as e:
        print(f"  ❌ 검증 실패: {e}")
    
    # MongoDB 연결 종료
    agent.mongodb_manager.close()

if __name__ == "__main__":
    main()