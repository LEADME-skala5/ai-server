import mysql.connector
import json
from decimal import Decimal
from typing import Dict, List, Tuple
import logging
from datetime import datetime
import os

# .env 파일 로드
from dotenv import load_dotenv
load_dotenv()

# MongoDB 추가 import
from pymongo import MongoClient

# LangChain 최신 버전 호환
try:
    from langchain_openai import OpenAI
except ImportError:
    from langchain_community.llms import OpenAI

from langchain.prompts import PromptTemplate
try:
    from langchain.chains import LLMChain
except ImportError:
    # LangChain 최신 버전의 경우
    from langchain_core.runnables import RunnablePassthrough
    from langchain_core.output_parsers import StrOutputParser

class MongoDBManager:
    """MongoDB 연결 및 관리 클래스"""
    
    def __init__(self):
        # .env에서 MongoDB 설정 로드
        self.host = os.getenv("MONGO_HOST")
        self.port = int(os.getenv("MONGO_PORT"))
        self.username = os.getenv("MONGO_USER")
        self.password = os.getenv("MONGO_PASSWORD")
        self.database_name = os.getenv("MONGO_DB_NAME")
        
        self.mongodb_uri = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/"
        self.collection_name = "personal_quarter_reports"
        self.client = None
        
        print(f"📋 MongoDB 설정 로드 완료: {self.host}:{self.port}/{self.database_name}")
    
    def connect(self):
        """MongoDB 연결"""
        try:
            self.client = MongoClient(self.mongodb_uri)
            # 연결 테스트
            self.client.admin.command('ping')
            print("✅ MongoDB 연결 성공!")
            return True
        except Exception as e:
            print(f"❌ MongoDB 연결 실패: {e}")
            return False
    
    def add_user_to_quarter_document(self, user_data: Dict) -> bool:
        """분기별 문서에 사용자 데이터 추가"""
        try:
            if not self.client:
                if not self.connect():
                    return False
            
            db = self.client[self.database_name]
            collection = db[self.collection_name]
            
            quarter_key = f"{user_data['year']}Q{user_data['quarter']}"
            
            # 해당 분기 문서가 존재하는지 확인
            existing_doc = collection.find_one({
                "quarter": quarter_key,
                "data_type": "peer_evaluation_results"
            })
            
            if existing_doc:
                # 기존 문서에 사용자 데이터 추가
                collection.update_one(
                    {"quarter": quarter_key, "data_type": "peer_evaluation_results"},
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
                    "quarter": quarter_key,
                    "year": user_data['year'],
                    "quarter_num": user_data['quarter'],
                    "data_type": "peer_evaluation_results",
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

class PeerEvaluationSystem:
    def __init__(self, openai_api_key: str):
        # .env에서 MariaDB 설정 로드
        self.db_config = {
            'host': os.getenv("DB_HOST"),
            'port': int(os.getenv("DB_PORT")),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'database': os.getenv("DB_NAME")
        }
        
        print(f"📋 MariaDB 설정 로드 완료: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")
        
        self.llm = OpenAI(api_key=openai_api_key, temperature=0.7, max_tokens=1500)
        
    def get_db_connection(self):
        """DB 연결"""
        return mysql.connector.connect(**self.db_config)
    
    def fetch_peer_evaluation_data(self, evaluatee_user_id: int, year: int, quarter: int) -> List[Dict]:
        """특정 사용자가 받은 동료평가 키워드 데이터 조회"""
        query = """
        SELECT 
            ek.keyword,
            ek.is_positive,
            ek.passionate_weight,
            ek.professional_weight,
            ek.proactive_weight,
            ek.people_weight,
            ek.pessimistic_weight,
            ek.political_weight,
            ek.passive_weight,
            ek.personal_weight,
            COUNT(pke.keyword_id) as keyword_count
        FROM peer_keyword_evaluations pke
        JOIN evaluation_keywords ek ON pke.keyword_id = ek.id
        WHERE pke.evaluatee_user_id = %s 
        AND pke.evaluation_year = %s 
        AND pke.evaluation_quarter = %s
        GROUP BY pke.keyword_id
        """
        
        conn = self.get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 원래 쿼리 실행
        cursor.execute(query, (evaluatee_user_id, year, quarter))
        results = cursor.fetchall()
        
        conn.close()
        
        return results

    def update_peer_score_in_db(self, user_id: int, evaluation_year: int, evaluation_quarter: int, peer_score: float):
        """user_quarter_scores 테이블의 peer_score 컬럼 업데이트"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # 기존 데이터 확인
            check_query = """
            SELECT id FROM user_quarter_scores 
            WHERE user_id = %s AND evaluation_year = %s AND evaluation_quarter = %s
            """
            cursor.execute(check_query, (user_id, evaluation_year, evaluation_quarter))
            existing_record = cursor.fetchone()
            
            if existing_record:
                # 기존 데이터 업데이트
                update_query = """
                UPDATE user_quarter_scores 
                SET peer_score = %s, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s AND evaluation_year = %s AND evaluation_quarter = %s
                """
                cursor.execute(update_query, (peer_score, user_id, evaluation_year, evaluation_quarter))
                print(f"✅ user_quarter_scores peer_score 업데이트 완료: 사용자 ID {user_id}, 점수 {peer_score}")
            else:
                # 새 데이터 추가
                insert_query = """
                INSERT INTO user_quarter_scores (user_id, evaluation_year, evaluation_quarter, peer_score)
                VALUES (%s, %s, %s, %s)
                """
                cursor.execute(insert_query, (user_id, evaluation_year, evaluation_quarter, peer_score))
                print(f"✅ user_quarter_scores 새 데이터 추가 완료: 사용자 ID {user_id}, 점수 {peer_score}")
            
            conn.commit()
            
        except Exception as e:
            print(f"❌ DB 업데이트 오류: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

# 새로운 점수 계산 함수 (기존 함수들 대체)
def calculate_new_peer_score(keyword_data: List[Dict]) -> float:
    """
    새로운 점수 계산 방식: 기본 3점 + (GOOD 가중치 합 - BAD 가중치 합) / 키워드 수
    5점 만점으로 제한
    """
    base_score = 3.0
    total_score_diff = 0.0
    total_keywords = 0
    
    for item in keyword_data:
        keyword = item['keyword']
        count = int(item['keyword_count'])
        
        # GOOD 4개 차원 가중치 합
        good_weights = (
            float(item['passionate_weight'] or 0) +
            float(item['professional_weight'] or 0) +
            float(item['proactive_weight'] or 0) +
            float(item['people_weight'] or 0)
        )
        
        # BAD 4개 차원 가중치 합  
        bad_weights = (
            float(item['pessimistic_weight'] or 0) +
            float(item['political_weight'] or 0) +
            float(item['passive_weight'] or 0) +
            float(item['personal_weight'] or 0)
        )
        
        # 이 키워드의 점수 기여도 (선택된 횟수만큼 반영)
        keyword_contribution = (good_weights - bad_weights) * count
        total_score_diff += keyword_contribution
        total_keywords += count
    
    # 최종 점수 계산
    if total_keywords > 0:
        final_score = base_score + (total_score_diff / total_keywords)
    else:
        final_score = base_score
    
    # 5점 만점으로 제한
    final_score = max(0.0, min(5.0, final_score))
    
    return final_score

class PeerScoreAgent:
    """동료 점수 평가 Agent"""
    
    def __init__(self, db_system: PeerEvaluationSystem):
        self.db_system = db_system
    
    def save_score_to_db(self, user_id: int, year: int, quarter: int, score: float):
        """점수를 DB에 저장 - MariaDB user_quarter_scores 테이블 업데이트"""
        self.db_system.update_peer_score_in_db(user_id, year, quarter, score)
        return True

class FeedbackGenerationAgent:
    """평가 생성 Agent"""
    
    def __init__(self, db_system: PeerEvaluationSystem):
        self.db_system = db_system
        self.prompt_template = PromptTemplate(
            input_variables=["quarter", "positive_keywords", "negative_keywords", "score"],
            template="""
당신은 HR 전문가입니다. 동료평가 결과를 바탕으로 건설적이고 구체적인 피드백을 작성해주세요.
3,4줄로 간결하고 핵심위주로 작성하세요. 

{quarter}분기 동료평가 결과:
- 최종 점수: {score}점
- 긍정 키워드: {positive_keywords}
- 부정 키워드: {negative_keywords}

다음 형식으로 피드백을 작성해주세요:
1. 주요 강점 분석 (긍정 키워드 기반)
2. 개선 포인트 제시 (부정 키워드 기반)
3. 구체적인 성장 방향 제언

피드백은 격려하되 구체적이고 실행 가능한 조언이 포함되어야 합니다.
"""
        )
        
        # LangChain 버전 호환성 처리 - 경고 억제
        try:
            from langchain.chains import LLMChain
            self.use_legacy_chain = False  # 경고 방지를 위해 최신 방식 사용
            self.chain = self.prompt_template | self.db_system.llm | StrOutputParser()
        except:
            # fallback to legacy
            self.chain = LLMChain(llm=self.db_system.llm, prompt=self.prompt_template)
            self.use_legacy_chain = True
    
    def categorize_keywords(self, keyword_data: List[Dict]) -> Tuple[List[str], List[str]]:
        """키워드를 긍정/부정으로 분류"""
        positive_keywords = []
        negative_keywords = []
        
        for item in keyword_data:
            keyword = item['keyword']
            count = item['keyword_count']
            
            # 여러 번 선택된 키워드는 횟수 표시
            display_keyword = f"{keyword}({count}회)" if count > 1 else keyword
            
            if item['is_positive']:
                positive_keywords.append(display_keyword)
            else:
                negative_keywords.append(display_keyword)
        
        return positive_keywords, negative_keywords
    
    def generate_feedback(self, keyword_data: List[Dict], score: float, quarter: int) -> str:
        """피드백 생성"""
        positive_keywords, negative_keywords = self.categorize_keywords(keyword_data)
        
        positive_str = ", ".join(positive_keywords) if positive_keywords else "없음"
        negative_str = ", ".join(negative_keywords) if negative_keywords else "없음"
        
        # LangChain 버전 호환성 처리
        if self.use_legacy_chain:
            feedback = self.chain.run(
                quarter=quarter,
                positive_keywords=positive_str,
                negative_keywords=negative_str,
                score=round(score, 2)  # 5점 만점으로 변경
            )
        else:
            feedback = self.chain.invoke({
                "quarter": quarter,
                "positive_keywords": positive_str,
                "negative_keywords": negative_str,
                "score": round(score, 2)  # 5점 만점으로 변경
            })
        
        return feedback.strip()

# JSON 직렬화를 위한 헬퍼 함수
def convert_decimal_to_float(obj):
    """Decimal 객체를 float로 변환하는 함수"""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimal_to_float(v) for v in obj]
    return obj

class PeerEvaluationOrchestrator:
    """전체 동료평가 프로세스 오케스트레이터"""
    
    def __init__(self, openai_api_key: str):
        self.db_system = PeerEvaluationSystem(openai_api_key)
        self.score_agent = PeerScoreAgent(self.db_system)
        self.feedback_agent = FeedbackGenerationAgent(self.db_system)
        # MongoDB 매니저 추가
        self.mongodb_manager = MongoDBManager()
    
    def process_peer_evaluation(self, user_id: int, year: int, quarter: int, save_to_mongodb: bool = True) -> Dict:
        """전체 동료평가 프로세스 실행 - 분기별 MongoDB 저장"""
        try:
            # 1. 데이터 조회
            keyword_data = self.db_system.fetch_peer_evaluation_data(user_id, year, quarter)
            
            if not keyword_data:
                return {
                    "success": False,
                    "message": "해당 기간의 동료평가 데이터가 없습니다.",
                    "data": None 
                }
            
            # 2. 새로운 방식으로 점수 계산
            final_score = calculate_new_peer_score(keyword_data)
            
            # 3. 점수 DB 저장 (MariaDB user_quarter_scores 테이블) - 소수점 둘째자리로 반올림
            rounded_score = round(final_score, 2)
            self.score_agent.save_score_to_db(user_id, year, quarter, rounded_score)
            
            # 4. 피드백 생성
            feedback = self.feedback_agent.generate_feedback(keyword_data, final_score, quarter)
            
            # 5. 결과 구성 (5점 만점, 소수점 둘째자리)
            result_data = {
                "user_id": user_id,
                "year": year,
                "quarter": quarter,
                "peer_evaluation_score": round(float(final_score), 2),
                "calculation_method": "new_weighted_method_5point",
                "feedback": feedback,
                "keyword_summary": {
                    "positive": [item['keyword'] for item in keyword_data if item['is_positive']],
                    "negative": [item['keyword'] for item in keyword_data if not item['is_positive']]
                },
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # 6. 분기별 문서에 사용자 데이터 추가
            if save_to_mongodb:
                mongodb_save_success = self.mongodb_manager.add_user_to_quarter_document(result_data)
                
                if mongodb_save_success:
                    print(f"✅ 사용자 ID {user_id} 동료평가 분기별 문서에 추가 완료")
                else:
                    print(f"❌ 사용자 ID {user_id} 동료평가 MongoDB 저장 실패")
            
            result = {
                "success": True,
                "data": result_data
            }
            
            return result
            
        except Exception as e:
            logging.error(f"동료평가 처리 중 오류 발생: {str(e)}")
            return {
                "success": False,
                "message": f"처리 중 오류가 발생했습니다: {str(e)}",
                "data": None
            }
    
    def process_batch_peer_evaluation(self, user_ids: List[int], year: int, quarter: int) -> List[Dict]:
        """여러 사용자의 동료평가를 배치 처리 - 분기별 문서에 추가"""
        results = []
        total_users = len(user_ids)
        successful_count = 0
        failed_count = 0
        scores = []
        
        for i, user_id in enumerate(user_ids, 1):
            # 진행률 표시 (매 10명마다)
            if i % 10 == 0 or i == total_users:
                print(f"처리 진행률: {i}/{total_users} ({i/total_users*100:.1f}%)")
            
            # 개별 사용자 처리 (분기별 문서에 추가)
            result = self.process_peer_evaluation(user_id, year, quarter, save_to_mongodb=True)
            results.append(result)
            
            # 성공/실패 통계 집계
            if result["success"]:
                successful_count += 1
                score = result["data"]["peer_evaluation_score"]
                scores.append(score)
                print(f"✓ User {user_id}: {score:.2f}/5.0 → 분기별 문서에 추가 완료")
            else:
                failed_count += 1
                print(f"✗ User {user_id}: 데이터 없음")
        
        return results
    
    def get_all_users_with_data(self, year: int, quarter: int) -> List[int]:
        """해당 연도/분기에 데이터가 있는 모든 사용자 ID 조회"""
        query = """
        SELECT DISTINCT evaluatee_user_id 
        FROM peer_keyword_evaluations 
        WHERE evaluation_year = %s AND evaluation_quarter = %s
        ORDER BY evaluatee_user_id
        """
        
        conn = self.db_system.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, (year, quarter))
        results = cursor.fetchall()
        conn.close()
        
        return [row[0] for row in results]

def process_single_quarter(orchestrator, user_ids, year, quarter):
    """단일 분기 처리 함수 - 분기별 문서에 사용자 데이터 추가"""
    print(f"\n=== {year}년 {quarter}분기 동료평가 처리 시작 ===")
    print(f"처리할 사용자 수: {len(user_ids)}명")
    print(f"MongoDB 저장 방식: {year}Q{quarter} 문서에 사용자 데이터 추가")
    print("=" * 50)
    
    # 배치 처리 실행 (각 사용자를 분기별 문서에 추가)
    results = orchestrator.process_batch_peer_evaluation(
        user_ids=user_ids,
        year=year,
        quarter=quarter
    )
    
    # 결과 통계 계산
    successful_count = sum(1 for r in results if r["success"])
    failed_count = len(results) - successful_count
    
    print(f"\n=== {quarter}분기 동료평가 처리 완료 ===")
    print(f"성공: {successful_count}명 → {year}Q{quarter} 문서에 추가 완료")
    print(f"실패: {failed_count}명")
    
    avg_score = None
    # 통계 계산
    if successful_count > 0:
        scores = [r["data"]["peer_evaluation_score"] for r in results if r["success"]]
        avg_score = sum(scores) / len(scores)
        max_score = max(scores)
        min_score = min(scores)
        
        print(f"평균 점수: {avg_score:.2f}/5.0")
        print(f"최고 점수: {max_score:.2f}/5.0")
        print(f"최저 점수: {min_score:.2f}/5.0")
    
    # 실패한 사용자 개수만 출력
    if failed_count > 0:
        print(f"데이터가 없는 사용자: {failed_count}명")
    
    return {
        "quarter": quarter,
        "successful_count": successful_count,
        "failed_count": failed_count,
        "average_score": round(avg_score, 2) if avg_score else 0
    }

def main():
    # .env에서 OpenAI API 키 로드
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    if not OPENAI_API_KEY:
        print("❌ .env 파일에 OPENAI_API_KEY가 설정되지 않았습니다.")
        return
    
    print("✅ .env 파일에서 설정 로드 완료")
    print(f"🔑 OpenAI API 키: {OPENAI_API_KEY[:10]}{'*' * 20}{OPENAI_API_KEY[-5:]}")
    
    # 오케스트레이터 초기화
    orchestrator = PeerEvaluationOrchestrator(OPENAI_API_KEY)
    
    # MongoDB 연결 테스트
    print("🔌 MongoDB 연결 테스트...")
    if not orchestrator.mongodb_manager.connect():
        print("❌ MongoDB 연결 실패. 프로그램을 종료합니다.")
        return
    
    # 1~100 사용자 ID 리스트 생성
    user_ids = list(range(1, 101))
    
    print(f"\n=== 2024년 전체 분기 동료평가 배치 처리 시작 (분기별 문서 저장) ===")
    print(f"처리할 사용자 수: {len(user_ids)}명")
    print(f"처리할 분기: Q1, Q2, Q3, Q4")
    print(f"저장 방식: 분기별 문서에 사용자 데이터 누적 추가")
    print(f"저장 위치: MongoDB - {os.getenv('MONGO_DB_NAME')}.personal_quarter_reports")
    print(f"문서 구조:")
    print(f"  - 2024Q1 문서: Q1 모든 사용자 데이터")
    print(f"  - 2024Q2 문서: Q2 모든 사용자 데이터")
    print(f"  - 2024Q3 문서: Q3 모든 사용자 데이터")
    print(f"  - 2024Q4 문서: Q4 모든 사용자 데이터")
    print("=" * 60)
    
    # 전체 결과 저장용
    all_quarters_results = {}
    
    # 4개 분기 모두 처리
    for quarter in [1, 2, 3, 4]:
        quarter_result = process_single_quarter(orchestrator, user_ids, 2024, quarter)
        all_quarters_results[f"Q{quarter}"] = quarter_result
        
        # 분기 간 구분을 위한 여백
        print("\n" + "="*60)
    
    # 전체 분기 통합 결과 출력
    print(f"\n=== 2024년 전체 분기 동료평가 처리 완료 ===")
    
    total_processed = 0
    for quarter in [1, 2, 3, 4]:
        if f"Q{quarter}" in all_quarters_results:
            quarter_data = all_quarters_results[f"Q{quarter}"]
            successful = quarter_data["successful_count"]
            total_processed += successful
            print(f"Q{quarter}: 성공 {successful}명 → 2024Q{quarter} 문서에 저장 완료")
        else:
            print(f"Q{quarter}: 데이터 없음")
    
    print(f"\n🎉 처리 완료 요약:")
    print(f"  - 총 처리된 사용자: {total_processed}명")
    print(f"  - 저장 방식: 분기별 하나의 문서에 모든 사용자 데이터 저장")
    print(f"  - 데이터베이스: {os.getenv('MONGO_DB_NAME')}")
    print(f"  - 컬렉션: personal_quarter_reports")
    print(f"  - 총 문서 수: 4개 (2024Q1, 2024Q2, 2024Q3, 2024Q4)")
    print(f"  - 문서 구조: quarter/year/quarter_num/data_type/user_count/users[]")
    print(f"  - MariaDB user_quarter_scores.peer_score 업데이트 완료")
    
    # MongoDB 연결 종료
    orchestrator.mongodb_manager.close()
    
    return all_quarters_results

if __name__ == "__main__":
    main()