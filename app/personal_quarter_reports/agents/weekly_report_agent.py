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
    calculate_final_score,
    calculate_enhanced_final_score  # 새로 추가된 함수
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
    
    def update_weekly_score_in_db(self, user_id: int, evaluation_year: int, evaluation_quarter: int, enhanced_final_score: float, score_details: dict):
        """user_quarter_scores 테이블의 weekly_score 컬럼을 향상된 점수로 업데이트"""
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
                        (enhanced_final_score, user_id, evaluation_year, evaluation_quarter)
                    )
                else:
                    # 새 데이터 추가
                    cur.execute(
                        """INSERT INTO user_quarter_scores (user_id, evaluation_year, evaluation_quarter, weekly_score, created_at, updated_at)
                           VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""",
                        (user_id, evaluation_year, evaluation_quarter, enhanced_final_score)
                    )
                    
        except Exception as e:
            print(f"❌ MariaDB 업데이트 오류 (user_id: {user_id}): {e}")
        finally:
            conn.close()

    def generate_basic_summary(self, user_name: str, year: int, quarter: int, enhanced_result: dict) -> str:
        """MongoDB 데이터가 없는 사용자를 위한 기본 요약문 생성"""
        try:
            existing_score = enhanced_result.get('existing_final_score', 0)
            performance_score = enhanced_result.get('performance_score', 0)
            
            prompt = f"""다음은 {user_name} 님의 {year}년 {quarter}분기 평가 정보입니다.

■ 평가 정보:
- 기존 업무 평가 점수: {existing_score:.2f}점
- 개인 실적 평가 점수: {performance_score:.2f}점
- MongoDB 활동 데이터: 없음

위 정보를 바탕으로 다음 구조와 요구사항에 맞는 업무 요약문을 작성해주세요:

**필수 구조 (정확히 이 문장 형식으로 작성):**
1. 도입문 (고정): "{user_name} 님의 {year}년 {quarter}분기 업무 수행 내역입니다."

2. 활동 현황: "상세한 활동 데이터는 추가 수집이 필요하나, 배정된 업무 수행을 통해 팀 운영에 참여하였습니다."

3. 마무리 문장: 없음 (활동 현황 후 바로 종료)

**작성 세부 요구사항:**
- 완전히 객관적이고 중립적인 어조만 사용
- 데이터 부족 상황을 자연스럽게 표현
- 전체 길이: 80-100자 내외

**엄격한 금지사항:**
- 점수, 등급, 순위 언급 완전 금지
- "우수한", "부족한", "미흡한" 등 평가적 표현 완전 금지
- 부정적이거나 비판적인 표현 완전 금지
- 개인 역량이나 특성 언급 금지
- 마무리 문장이나 종합 평가 작성 금지

**허용되는 표현 (이것만 사용):**
- "참여하였습니다"
- "배정된", "업무", "활동"

**출력 형식:**
- 단일 문단으로 작성
- 도입문 → 활동 현황 후 즉시 종료
- 활동 현황 문장의 마침표로 전체 종료
"""

            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "당신은 HR 전문가로서 절대적으로 객관적이고 일관성 있는 직원 평가 요약문을 작성해야 합니다. 모든 직원에게 완전히 동일한 구조, 형식, 어조를 적용하여 100% 공정한 평가를 보장해야 합니다. 주어진 형식을 한 글자도 벗어나지 말고 정확히 준수하세요."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=200
            )
            
            summary_text = response.choices[0].message.content.strip()
            return summary_text
            
        except Exception as e:
            return f"{user_name} 님의 {year}년 {quarter}분기 업무 수행 내역입니다. 상세한 활동 데이터는 추가 수집이 필요하나, 배정된 업무 수행을 통해 팀 운영에 참여하였습니다."
    
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
                    return f"User_{user_id}"
                    
        except Exception as e:
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
            
            # 프롬프트 구성
            prompt = f"""다음은 {user_name} 님의 {user_data.get('year', 2024)}년 {user_data.get('quarter', 1)}분기 업무 활동 데이터입니다.

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
위 데이터를 바탕으로 다음 구조와 요구사항에 맞는 업무 요약문을 작성해주세요:

**필수 구조 (정확히 이 문장 형식으로 작성):**
1. 도입문 (고정): "{user_name} 님의 {user_data.get('year', 2024)}년 {user_data.get('quarter', 1)}분기 업무 수행 내역입니다."

2. 목표별 성과 (각 목표마다 정확히 이 형식):
   - "[정확한 목표명]에 [숫자]건 기여하였으며, 이는 [구체적 활동 요약]을/를 통해 이루어졌습니다([출처 정보] weekly 보고서 기반)."

3. 마무리 문장: 없음 (목표별 성과 나열 후 바로 종료)

**목표명 표기 규칙 (정확히 이렇게 작성):**
- 원가 관련: "CSP 파트너쉽 강화를 통한 원가 개선"
- 업무 개선: "Cloud Professional 업무 진행을 통한 BR/UR 개선"
- 글로벌: "글로벌 사업 Tech-presales 진행"
- AI 관련: "AI 업무 적용"
- ESG 관련: "ESG 사업 수익 창출", "신규 ESG BM 발굴"
- 문화 관련: "조직문화 혁신"

**작성 세부 요구사항:**
- 각 목표별 기여 건수와 출처 정보 필수 포함
- 구체적 업무 내용을 25자 이내로 간략 요약
- 완전히 객관적이고 중립적인 어조만 사용
- 배정된 목표 수에 따라 자연스러운 길이 차이 허용 (무리한 길이 맞추기 금지)
- 모든 기여에 대해 반드시 "weekly 보고서 기반" 포함

**출처 표기 규칙 (정확히 이 형식):**
- 여러 주차: "1월 1주차, 2월 5주차, 3월 7주차 weekly 보고서 기반"
- 단일 주차: "2월 12주차 weekly 보고서 기반"
- 연속 주차 3개 이상: "1월 2주차, 3월 9주차, 3월 13주차 weekly 보고서 기반"

**엄격한 금지사항:**
- 점수, 등급, 순위, 성과 수준 언급 완전 금지
- "탁월한", "뛰어난", "우수한", "두드러진", "중요한", "성공적인", "효과적인", "전문적인" 등 모든 평가적 수식어 금지
- 개인 역량이나 특성 언급 금지 ("분석적 사고", "문제 해결 능력" 등)
- 출처 없는 기여도나 성과 언급 금지
- 다른 직원과의 비교 언급 금지
- "무리한 길이 맞추기나 내용 부풀리기" 금지
- "실제 배정된 목표와 기여도만 정확히 반영" 필수
- 마무리 문장이나 종합 평가 작성 금지

**허용되는 표현 (이것만 사용):**
- "기여하였으며", "이루어졌습니다"
- "수행", "진행", "완료", "처리", "담당"
- "활동", "업무", "과제"

**검증 체크리스트:**
1. 도입문이 정확한 고정 형식인가?
2. 모든 목표명이 정확한 표기법을 따르는가?
3. 모든 출처에 "weekly 보고서 기반"이 포함되는가?
4. 평가적 수식어가 전혀 없는가?
5. 개인 역량 언급이 전혀 없는가?
6. 실제 배정된 목표와 기여도만 반영했는가?
7. 마무리 문장이나 종합 평가가 없는가?

**출력 형식:**
- 단일 문단으로 작성
- 도입문 → 목표별 성과 나열 후 즉시 종료
- 마지막 목표별 성과 문장의 마침표로 전체 종료
"""

            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "당신은 HR 전문가로서 절대적으로 객관적이고 일관성 있는 직원 평가 요약문을 작성해야 합니다. 모든 직원에게 완전히 동일한 구조, 형식, 어조를 적용하여 100% 공정한 평가를 보장해야 합니다. 주어진 형식을 한 글자도 벗어나지 말고 정확히 준수하세요."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=400
            )
            
            summary_text = response.choices[0].message.content.strip()
            return summary_text
            
        except Exception as e:
            year = user_data.get('year', 2024)
            quarter = user_data.get('quarter', 1)
            return f"{user_name} 님의 {year}년 {quarter}분기 업무 수행 내역입니다. 상세한 활동 데이터는 추가 수집이 필요하나, 배정된 업무 수행을 통해 팀 운영에 참여하였습니다."

    def get_all_users_from_db(self) -> List[Dict]:
        """MariaDB users 테이블에서 모든 사용자 정보 조회"""
        conn = pymysql.connect(**self.db_config)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id, name FROM users WHERE id IS NOT NULL ORDER BY id")
                users = cur.fetchall()
                
                print(f"📋 MariaDB users 테이블에서 {len(users)}명 조회 완료")
                if users:
                    print(f"   사용자 ID 범위: {users[0]['id']} ~ {users[-1]['id']}")
                
                return [{"user_id": user['id'], "user_name": user['name']} for user in users]
                
        except Exception as e:
            print(f"❌ 사용자 목록 조회 실패: {e}")
            return []
        finally:
            conn.close()

    def process_single_quarter_combination(self, year: int, quarter: int) -> Dict:
        """단일 분기 조합 결과 처리 - 향상된 점수 계산 적용 (전체 사용자 대상)"""
        print(f"\n🚀 {year}년 {quarter}분기 주간 평가 조합 처리 시작")
        
        # 1. MariaDB에서 모든 사용자 목록 조회
        all_users = self.get_all_users_from_db()
        
        if not all_users:
            print(f"⚠️ users 테이블에서 사용자를 찾을 수 없습니다.")
            return {
                "quarter": quarter,
                "successful_count": 0,
                "failed_count": 0,
                "average_score": 0
            }
        
        # 2. MongoDB에서 활동 데이터 조회 (참고용)
        mongodb_users_data = self.mongodb_manager.get_quarter_evaluation_data(year, quarter)
        mongodb_user_dict = {data.get('user_id'): data for data in mongodb_users_data}
        
        print(f"📊 전체 사용자 수: {len(all_users)}명")
        print(f"📊 MongoDB 활동 데이터: {len(mongodb_users_data)}명")
        
        # 3. 각 사용자별로 처리 (users 테이블의 모든 사용자 대상)
        processed_users = []
        successful_count = 0
        failed_count = 0
        enhanced_weekly_scores = []
        
        for i, user_info in enumerate(all_users, 1):
            # 진행률 표시 (매 100명마다)
            if i % 100 == 0 or i == len(all_users):
                print(f"📈 처리 진행률: {i}/{len(all_users)} ({i/len(all_users)*100:.1f}%)")
            
            try:
                user_id = user_info.get('user_id')
                user_name = user_info.get('user_name')
                
                if not user_id:
                    failed_count += 1
                    continue
                
                # 4. MongoDB 활동 데이터 확인 (있으면 사용, 없으면 기본값)
                user_mongodb_data = mongodb_user_dict.get(user_id)
                
                # 5. 향상된 점수 계산
                try:
                    enhanced_result = calculate_enhanced_final_score(user_id, year, quarter)
                    enhanced_final_score = enhanced_result.get('enhanced_final_score', 0.0)
                except Exception as score_error:
                    enhanced_final_score = 0.0
                    enhanced_result = {}
                
                # 6. AI 강화된 요약문 생성
                if user_mongodb_data:
                    weekly_summary_text = self.generate_enhanced_activity_summary(user_mongodb_data, user_name)
                else:
                    weekly_summary_text = self.generate_basic_summary(user_name, year, quarter, enhanced_result)
                
                # 7. 결과 데이터 구성
                processed_user = {
                    "user_id": user_id,
                    "user_name": user_name,
                    "year": year,
                    "quarter": quarter,
                    "weekly_score": enhanced_final_score,
                    "weekly_summary_text": weekly_summary_text,
                    "score_breakdown": {
                        "existing_final_score": enhanced_result.get('existing_final_score', 0),
                        "performance_score": enhanced_result.get('performance_score', 0),
                        "avg_score": enhanced_result.get('avg_score', 0),
                        "workload_score": enhanced_result.get('workload_score', 0)
                    },
                    "has_mongodb_data": user_mongodb_data is not None
                }
                
                # 8. MariaDB user_quarter_scores 테이블에 향상된 점수 업데이트
                self.update_weekly_score_in_db(user_id, year, quarter, enhanced_final_score, enhanced_result)
                
                processed_users.append(processed_user)
                successful_count += 1
                enhanced_weekly_scores.append(enhanced_final_score)
                
            except Exception as e:
                failed_count += 1
        
        # 9. MongoDB에 저장
        if processed_users:
            save_success = self.mongodb_manager.save_quarter_combination_results(year, quarter, processed_users)
            if save_success:
                print(f"✅ MongoDB 저장 완료: weekly_combination_results.{year}Q{quarter}")
            else:
                print(f"❌ {year}년 {quarter}분기 MongoDB 저장 실패")
        
        # 10. 통계 계산 및 출력
        print(f"📊 {year}년 {quarter}분기 처리 완료: 성공 {successful_count}명, 실패 {failed_count}명")
        
        avg_score = 0
        if enhanced_weekly_scores:
            avg_score = sum(enhanced_weekly_scores) / len(enhanced_weekly_scores)
            print(f"🏆 평균 점수: {avg_score:.2f}")
        
        return {
            "quarter": quarter,
            "successful_count": successful_count,
            "failed_count": failed_count,
            "average_score": round(avg_score, 2) if enhanced_weekly_scores else 0
        }

def main():
    print("🚀 향상된 주간 평가 조합 결과 생성 시작")
    print("🔧 새로운 기능: 기존 점수(25%) + 개인 실적 점수(75%) 조합")
    print("="*70)
    
    # 에이전트 초기화
    agent = WeeklyReportAgent()
    
    # MongoDB 연결 테스트
    print("🔌 MongoDB 연결 테스트...")
    if not agent.mongodb_manager.connect():
        print("❌ MongoDB 연결 실패. 프로그램을 종료합니다.")
        return
    
    print(f"\n📋 처리 개요:")
    print(f"  입력: MongoDB.{os.getenv('MONGO_DB_NAME')}.weekly_evaluation_results")
    print(f"  출력: MongoDB.{os.getenv('MONGO_DB_NAME')}.weekly_combination_results")
    print(f"  업데이트: MariaDB.{os.getenv('DB_NAME')}.user_quarter_scores.weekly_score")
    print(f"  점수 계산: 향상된 조합 점수 (기존 25% + 실적 75%)")
    print("="*70)
    
    # 전체 결과 저장용
    all_quarters_results = {}
    
    # 4개 분기 모두 처리
    for quarter in [1, 2, 3, 4]:
        quarter_result = agent.process_single_quarter_combination(2024, quarter)
        all_quarters_results[f"Q{quarter}"] = quarter_result
    
    # 전체 분기 통합 결과 출력
    print(f"\n🎉 2024년 전체 분기 처리 완료 요약")
    print(f"="*70)
    
    total_processed = 0
    total_failed = 0
    for quarter in [1, 2, 3, 4]:
        if f"Q{quarter}" in all_quarters_results:
            quarter_data = all_quarters_results[f"Q{quarter}"]
            successful = quarter_data["successful_count"]
            failed = quarter_data["failed_count"]
            avg_score = quarter_data["average_score"]
            total_processed += successful
            total_failed += failed
            print(f"📈 Q{quarter}: 성공 {successful}명, 실패 {failed}명, 평균 점수 {avg_score}")
        else:
            print(f"⚠️ Q{quarter}: 데이터 없음")
    
    print(f"\n🏆 최종 처리 결과:")
    print(f"  ✅ 총 처리 성공: {total_processed}명")
    print(f"  ❌ 총 처리 실패: {total_failed}명")
    print(f"  📊 성공률: {total_processed/(total_processed+total_failed)*100:.1f}%" if (total_processed+total_failed) > 0 else "")
    print(f"  💾 MongoDB 문서: 4개 (2024년 1,2,3,4분기)")
    print(f"  💾 MariaDB 업데이트: {total_processed}개 레코드")
    
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
                quarter = row['evaluation_quarter']
                count = row['updated_count']
                avg_score = row['avg_score']
                min_score = row['min_score']
                max_score = row['max_score']
                print(f"  📊 Q{quarter}: {count}명 업데이트, 평균 {avg_score:.2f} (범위: {min_score:.2f}~{max_score:.2f})")
        conn.close()
    except Exception as e:
        print(f"  ❌ 검증 실패: {e}")
    
    print(f"\n🎯 핵심 개선사항:")
    print(f"  • 점수 계산: 기존 단순 점수 → 향상된 조합 점수 (기존 25% + 실적 75%)")
    print(f"  • 개인 실적: task_results 기반 가중평균 실적 점수 반영")
    print(f"  • AI 요약문: GPT-4o 기반 맞춤형 성과 요약문 생성")
    print(f"  • 프롬프트 개선: 간결한 도입문, 마무리 문장 제거, 완전한 구조 통일")
    
    # MongoDB 연결 종료
    agent.mongodb_manager.close()
    
    print(f"\n✨ 향상된 주간 평가 조합 처리 완료!")
    print("="*70)

if __name__ == "__main__":
    main()