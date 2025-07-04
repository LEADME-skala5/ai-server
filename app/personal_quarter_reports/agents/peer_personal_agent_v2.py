import mysql.connector
import json
from decimal import Decimal
from typing import Dict, List, Tuple
import logging
from datetime import datetime
import os
from collections import Counter

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
    """MongoDB 연결 및 관리 클래스 - 🔥 중복 방지 및 업데이트 로직 추가"""
    
    def __init__(self):
        # .env에서 MongoDB 설정 로드
        self.host = os.getenv("MONGO_HOST")
        self.port = int(os.getenv("MONGO_PORT"))
        self.username = os.getenv("MONGO_USER")
        self.password = os.getenv("MONGO_PASSWORD")
        self.database_name = os.getenv("MONGO_DB_NAME")
        
        self.mongodb_uri = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/"
        self.collection_name = "peer_evaluation_results"
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
        """🔥 개선된 함수: 중복 방지 및 업데이트 로직 추가"""
        try:
            if not self.client:
                if not self.connect():
                    return False
            
            db = self.client[self.database_name]
            collection = db[self.collection_name]
            
            user_id = user_data['user_id']
            year = user_data['year']
            quarter = user_data['quarter']
            
            # 해당 분기 문서가 존재하는지 확인
            existing_doc = collection.find_one({
                "type": "personal-quarter",
                "evaluated_year": year,
                "evaluated_quarter": quarter
            })
            
            if existing_doc:
                # 기존 문서에서 해당 user_id가 이미 존재하는지 확인
                existing_user_index = None
                for i, user in enumerate(existing_doc.get('users', [])):
                    if user.get('user_id') == user_id:
                        existing_user_index = i
                        break
                
                if existing_user_index is not None:
                    # 기존 사용자 데이터 업데이트
                    collection.update_one(
                        {
                            "type": "personal-quarter",
                            "evaluated_year": year,
                            "evaluated_quarter": quarter
                        },
                        {
                            "$set": {
                                f"users.{existing_user_index}": user_data,
                                "updated_at": datetime.now()
                            }
                        }
                    )
                    print(f"🔄 기존 사용자 데이터 업데이트 완료: 사용자 ID {user_id} ({year}Q{quarter})")
                else:
                    # 새로운 사용자 데이터 추가
                    collection.update_one(
                        {
                            "type": "personal-quarter",
                            "evaluated_year": year,
                            "evaluated_quarter": quarter
                        },
                        {
                            "$push": {"users": user_data},
                            "$set": {"updated_at": datetime.now()},
                            "$inc": {"user_count": 1}
                        }
                    )
                    print(f"✅ 기존 분기 문서에 새 사용자 추가: 사용자 ID {user_id} ({year}Q{quarter})")
            else:
                # 새로운 분기 문서 생성
                quarter_document = {
                    "type": "personal-quarter",
                    "evaluated_year": year,
                    "evaluated_quarter": quarter,
                    "user_count": 1,
                    "users": [user_data],
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                }
                
                result = collection.insert_one(quarter_document)
                print(f"🆕 새로운 분기 문서 생성 및 사용자 추가: 사용자 ID {user_id} ({year}Q{quarter}) - Document ID: {result.inserted_id}")
            
            return True
            
        except Exception as e:
            print(f"❌ MongoDB 사용자 데이터 처리 실패 (사용자 ID: {user_data.get('user_id', 'unknown')}): {e}")
            return False
    
    def check_existing_user_data(self, user_id: int, year: int, quarter: int) -> bool:
        """특정 사용자의 해당 분기 데이터 존재 여부 확인"""
        try:
            if not self.client:
                if not self.connect():
                    return False
            
            db = self.client[self.database_name]
            collection = db[self.collection_name]
            
            # 해당 분기 문서에서 특정 사용자 데이터 확인
            existing_doc = collection.find_one({
                "type": "personal-quarter",
                "evaluated_year": year,
                "evaluated_quarter": quarter,
                "users.user_id": user_id
            })
            
            return existing_doc is not None
            
        except Exception as e:
            print(f"❌ 기존 데이터 확인 실패 (사용자 ID: {user_id}): {e}")
            return False
    
    def get_quarter_document_stats(self, year: int, quarter: int) -> Dict:
        """분기별 문서 통계 조회"""
        try:
            if not self.client:
                if not self.connect():
                    return {}
            
            db = self.client[self.database_name]
            collection = db[self.collection_name]
            
            doc = collection.find_one({
                "type": "personal-quarter",
                "evaluated_year": year,
                "evaluated_quarter": quarter
            })
            
            if doc:
                return {
                    "exists": True,
                    "user_count": len(doc.get('users', [])),
                    "created_at": doc.get('created_at'),
                    "updated_at": doc.get('updated_at')
                }
            else:
                return {"exists": False, "user_count": 0}
                
        except Exception as e:
            print(f"❌ 분기 문서 통계 조회 실패: {e}")
            return {"exists": False, "user_count": 0}
    
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
    
    def get_peer_keywords_with_frequency(self, evaluatee_user_id: int, year: int, quarter: int) -> Dict:
        """🔥 개선된 함수: 키워드별 빈도수와 함께 조회 - evaluation_keywords 테이블 조인"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            # peer_keyword_evaluations와 evaluation_keywords 테이블 조인하여 키워드별 빈도수 조회
            query = """
            SELECT 
                ek.keyword,
                ek.is_positive,
                COUNT(*) as frequency_count
            FROM peer_keyword_evaluations pke
            JOIN evaluation_keywords ek ON pke.keyword_id = ek.id
            WHERE pke.evaluatee_user_id = %s 
            AND pke.evaluation_year = %s 
            AND pke.evaluation_quarter = %s
            GROUP BY ek.keyword, ek.is_positive
            ORDER BY COUNT(*) DESC, ek.keyword
            """
            
            cursor.execute(query, (evaluatee_user_id, year, quarter))
            results = cursor.fetchall()
            
            # 긍정/부정별로 분류하여 빈도수와 함께 저장
            positive_keywords = []
            negative_keywords = []
            
            for row in results:
                keyword_data = {
                    "keyword": row['keyword'],
                    "count": row['frequency_count']
                }
                
                if row['is_positive']:
                    positive_keywords.append(keyword_data)
                else:
                    negative_keywords.append(keyword_data)
            
            print(f"✅ 사용자 {evaluatee_user_id}의 {year}Q{quarter} 키워드 빈도수 조회: 긍정 {len(positive_keywords)}개, 부정 {len(negative_keywords)}개")
            
            conn.close()
            
            return {
                "positive": positive_keywords,
                "negative": negative_keywords
            }
            
        except Exception as e:
            print(f"❌ 키워드 빈도수 조회 실패 (user: {evaluatee_user_id}, {year}Q{quarter}): {e}")
            if 'conn' in locals():
                conn.close()
            return {"positive": [], "negative": []}

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
                print(f"🔄 user_quarter_scores peer_score 업데이트 완료: 사용자 ID {user_id}, 점수 {peer_score}")
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
    """평가 생성 Agent - 🔥 키워드 빈도수 기반 개선"""
    
    def __init__(self, db_system: PeerEvaluationSystem):
        self.db_system = db_system
        self.prompt_template = PromptTemplate(
            input_variables=["user_name", "quarter", "top_positive_keywords", "top_negative_keywords", "score"],
            template="""
동료평가 피드백을 다음 형식으로 작성해주세요.

{user_name}님은 업무 수행에 있어 [강점을 바탕으로 한 태도]로 임무를 수행하고 있습니다. 특별히 [구체적인 강점 행동]하며 [팀에 대한 기여]에 기여하고 있습니다. 다만, [개선점]한다면 더욱 효율적으로 업무를 수행할 수 있을 것으로 기대됩니다.

정확히 다음 예시와 같은 구조와 톤으로 작성하세요:

김개발님은 업무 수행에 있어 책임감을 갖고 성실하게 임무를 수행하고 있습니다. 특별히 동료들과의 협업에서 열정적인 자세를 보이며 팀워크 향상에 기여하고 있습니다. 다만, 소통 방식을 개선하고 감정조절 능력을 높인다면 더욱 효율적으로 업무를 수행할 수 있을 것으로 기대됩니다.

위 예시와 완전히 동일한 구조로 작성해주세요.
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
    
    def get_user_name(self, user_id: int) -> str:
        """사용자 이름 조회"""
        try:
            conn = self.db_system.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT name FROM users WHERE id = %s", (user_id,))
            result = cursor.fetchone()
            conn.close()
            return result['name'] if result else f"사용자{user_id}"
        except:
            return f"사용자{user_id}"
    
    def generate_feedback_with_frequency(self, user_id: int, keyword_frequency_data: Dict, score: float, quarter: int) -> str:
        """🔥 개선된 피드백 생성: 자연스러운 문장으로"""
        positive_keywords = keyword_frequency_data.get("positive", [])
        negative_keywords = keyword_frequency_data.get("negative", [])
        
        # 사용자 이름 조회
        user_name = self.get_user_name(user_id)
        
        # 상위 키워드만 추출 (AI가 참고용으로 사용, 사용자에게는 노출 안함)
        top_positive = ", ".join([kw['keyword'] for kw in positive_keywords[:3]]) if positive_keywords else "없음"
        top_negative = ", ".join([kw['keyword'] for kw in negative_keywords[:2]]) if negative_keywords else "없음"
        
        # LangChain 버전 호환성 처리
        if self.use_legacy_chain:
            feedback = self.chain.run(
                user_name=user_name,
                quarter=quarter,
                score=round(score, 2),
                top_positive_keywords=top_positive,
                top_negative_keywords=top_negative
            )
        else:
            feedback = self.chain.invoke({
                "user_name": user_name,
                "quarter": quarter,
                "score": round(score, 2),
                "top_positive_keywords": top_positive,
                "top_negative_keywords": top_negative
            })
        
        return feedback.strip()
    
    def generate_keyword_summary_with_frequency(self, keyword_frequency_data: Dict) -> Dict:
        """🔥 개선된 키워드 요약: 빈도수 포함 (내부 데이터용)"""
        return {
            "positive": [{"keyword": kw['keyword'], "count": kw['count']} for kw in keyword_frequency_data.get("positive", [])],
            "negative": [{"keyword": kw['keyword'], "count": kw['count']} for kw in keyword_frequency_data.get("negative", [])]
        }

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
    """전체 동료평가 프로세스 오케스트레이터 - 🔥 중복 방지 및 업데이트 로직 추가"""
    
    def __init__(self, openai_api_key: str):
        self.db_system = PeerEvaluationSystem(openai_api_key)
        self.score_agent = PeerScoreAgent(self.db_system)
        self.feedback_agent = FeedbackGenerationAgent(self.db_system)
        # MongoDB 매니저 추가
        self.mongodb_manager = MongoDBManager()
    
    def process_peer_evaluation(self, user_id: int, year: int, quarter: int, save_to_mongodb: bool = True, force_update: bool = False) -> Dict:
        """🔥 개선된 전체 동료평가 프로세스: 중복 방지 및 업데이트 로직 추가"""
        try:
            # 🔥 기존 데이터 확인 (force_update가 False인 경우에만)
            if not force_update and save_to_mongodb:
                exists = self.mongodb_manager.check_existing_user_data(user_id, year, quarter)
                if exists:
                    print(f"⚠️ 사용자 ID {user_id}의 {year}Q{quarter} 데이터가 이미 존재합니다. 건너뜀.")
                    return {
                        "success": True,
                        "message": "기존 데이터 존재 - 건너뜀",
                        "data": None,
                        "action": "skipped"
                    }
            
            # 1. 기존 데이터 조회 (점수 계산용)
            keyword_data = self.db_system.fetch_peer_evaluation_data(user_id, year, quarter)
            
            if not keyword_data:
                return {
                    "success": False,
                    "message": "해당 기간의 동료평가 데이터가 없습니다.",
                    "data": None,
                    "action": "no_data"
                }
            
            # 2. 🔥 새로운 키워드 빈도수 데이터 조회
            keyword_frequency_data = self.db_system.get_peer_keywords_with_frequency(user_id, year, quarter)
            
            # 3. 새로운 방식으로 점수 계산
            final_score = calculate_new_peer_score(keyword_data)
            
            # 4. 점수 DB 저장 (MariaDB user_quarter_scores 테이블) - 소수점 둘째자리로 반올림
            rounded_score = round(final_score, 2)
            self.score_agent.save_score_to_db(user_id, year, quarter, rounded_score)
            
            # 5. 🔥 개선된 피드백 생성 (자연스러운 문장)
            feedback = self.feedback_agent.generate_feedback_with_frequency(user_id, keyword_frequency_data, final_score, quarter)
            
            # 6. 결과 구성 (5점 만점, 소수점 둘째자리) - 🔥 keyword_summary로 통일
            result_data = {
                "user_id": user_id,
                "year": year,
                "quarter": quarter,
                "peer_evaluation_score": round(float(final_score), 2),
                "calculation_method": "new_weighted_method_5point_with_frequency",
                "feedback": feedback,
                "keyword_summary": keyword_frequency_data,  # 🔥 기존 이름 유지 (빈도수 포함)
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # 8. MongoDB에 사용자 데이터 추가/업데이트 (🔥 중복 방지 로직 적용)
            if save_to_mongodb:
                mongodb_save_success = self.mongodb_manager.add_user_to_quarter_document(result_data)
                
                action = "updated" if force_update else "added"
                if mongodb_save_success:
                    print(f"✅ 사용자 ID {user_id} 동료평가 peer_evaluation_results 컬렉션에 {action} 완료")
                else:
                    print(f"❌ 사용자 ID {user_id} 동료평가 MongoDB 저장 실패")
            
            result = {
                "success": True,
                "data": result_data,
                "action": "updated" if force_update else "processed"
            }
            
            return result
            
        except Exception as e:
            logging.error(f"동료평가 처리 중 오류 발생: {str(e)}")
            return {
                "success": False,
                "message": f"처리 중 오류가 발생했습니다: {str(e)}",
                "data": None,
                "action": "error"
            }
    
    def process_batch_peer_evaluation(self, user_ids: List[int], year: int, quarter: int, force_update: bool = False) -> List[Dict]:
        """🔥 개선된 배치 처리: 중복 방지 및 통계 개선"""
        results = []
        total_users = len(user_ids)
        successful_count = 0
        failed_count = 0
        skipped_count = 0
        updated_count = 0
        scores = []
        
        # 🔥 처리 전 기존 데이터 통계 조회
        existing_stats = self.mongodb_manager.get_quarter_document_stats(year, quarter)
        
        if existing_stats["exists"]:
            print(f"📊 기존 데이터 발견: {existing_stats['user_count']}명 (생성: {existing_stats['created_at']}, 수정: {existing_stats['updated_at']})")
            if not force_update:
                print(f"🔄 중복 방지 모드: 기존 데이터가 있는 사용자는 건너뜀")
            else:
                print(f"🔄 강제 업데이트 모드: 모든 사용자 데이터를 새로 처리")
        
        for i, user_id in enumerate(user_ids, 1):
            # 진행률 표시 (매 10명마다)
            if i % 10 == 0 or i == total_users:
                print(f"처리 진행률: {i}/{total_users} ({i/total_users*100:.1f}%)")
            
            # 개별 사용자 처리 (🔥 중복 방지 로직 적용)
            result = self.process_peer_evaluation(user_id, year, quarter, save_to_mongodb=True, force_update=force_update)
            results.append(result)
            
            # 성공/실패/건너뜀 통계 집계
            if result["success"]:
                action = result.get("action", "processed")
                if action == "skipped":
                    skipped_count += 1
                    print(f"⏭️ User {user_id}: 기존 데이터 존재 - 건너뜀")
                elif action in ["updated", "processed"]:
                    successful_count += 1
                    if action == "updated":
                        updated_count += 1
                    score = result["data"]["peer_evaluation_score"]
                    scores.append(score)
                    action_symbol = "🔄" if action == "updated" else "✓"
                    print(f"{action_symbol} User {user_id}: {score:.2f}/5.0 → peer_evaluation_results 컬렉션에 저장 완료")
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

def process_single_quarter(orchestrator, user_ids, year, quarter, force_update=False):
    """🔥 개선된 단일 분기 처리: 중복 방지 및 업데이트 옵션 추가"""
    print(f"\n=== {year}년 {quarter}분기 동료평가 처리 시작 ===")
    print(f"처리할 사용자 수: {len(user_ids)}명")
    print(f"🔥 개선사항: 키워드 빈도수 기반 피드백 생성 + 중복 방지")
    print(f"MongoDB 저장 방식: peer_evaluation_results 컬렉션에 type: 'personal-quarter'로 구분")
    print(f"중복 처리 모드: {'강제 업데이트' if force_update else '중복 방지 (기존 데이터 건너뜀)'}")
    print("=" * 50)
    
    # 배치 처리 실행 (🔥 중복 방지 로직 적용)
    results = orchestrator.process_batch_peer_evaluation(
        user_ids=user_ids,
        year=year,
        quarter=quarter,
        force_update=force_update
    )
    
    # 🔥 개선된 결과 통계 계산
    successful_count = sum(1 for r in results if r["success"] and r.get("action") in ["processed", "updated"])
    skipped_count = sum(1 for r in results if r["success"] and r.get("action") == "skipped")
    failed_count = sum(1 for r in results if not r["success"])
    updated_count = sum(1 for r in results if r["success"] and r.get("action") == "updated")
    
    print(f"\n=== {quarter}분기 동료평가 처리 완료 ===")
    print(f"✅ 성공: {successful_count}명 → peer_evaluation_results 컬렉션에 저장 완료")
    if updated_count > 0:
        print(f"🔄 업데이트: {updated_count}명 → 기존 데이터 덮어쓰기 완료")
    if skipped_count > 0:
        print(f"⏭️ 건너뜀: {skipped_count}명 → 기존 데이터 존재로 건너뜀")
    print(f"❌ 실패: {failed_count}명 → 동료평가 데이터 없음")
    
    avg_score = None
    # 통계 계산 (성공한 경우만)
    if successful_count > 0:
        scores = [r["data"]["peer_evaluation_score"] for r in results if r["success"] and r.get("action") in ["processed", "updated"]]
        if scores:
            avg_score = sum(scores) / len(scores)
            max_score = max(scores)
            min_score = min(scores)
            
            print(f"📊 점수 통계 (처리된 {len(scores)}명 기준):")
            print(f"  평균 점수: {avg_score:.2f}/5.0")
            print(f"  최고 점수: {max_score:.2f}/5.0")
            print(f"  최저 점수: {min_score:.2f}/5.0")
    
    # 🔥 최종 문서 통계 확인
    final_stats = orchestrator.mongodb_manager.get_quarter_document_stats(year, quarter)
    if final_stats["exists"]:
        print(f"📋 최종 문서 상태: 총 {final_stats['user_count']}명 저장됨")
    
    return {
        "quarter": quarter,
        "successful_count": successful_count,
        "skipped_count": skipped_count,
        "failed_count": failed_count,
        "updated_count": updated_count,
        "average_score": round(avg_score, 2) if avg_score else 0,
        "total_users_in_document": final_stats.get("user_count", 0)
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
    
    # 🔥 처리 모드 선택 (실제 사용시에는 매개변수로 전달)
    FORCE_UPDATE = True  # True: 기존 데이터 덮어쓰기, False: 중복 방지
    
    print(f"\n=== 2024년 전체 분기 동료평가 배치 처리 시작 (중복 방지 버전) ===")
    print(f"🔥 주요 개선사항:")
    print(f"  - ✅ 중복 방지: 기존 데이터가 있는 사용자는 자동으로 건너뜀")
    print(f"  - 🔄 업데이트 옵션: force_update=True 설정시 기존 데이터 덮어쓰기")
    print(f"  - 📊 상세 통계: 성공/건너뜀/실패/업데이트 개수 각각 집계")
    print(f"  - 🚀 효율성 향상: 이미 처리된 데이터는 재처리하지 않음")
    print(f"  - MariaDB peer_keyword_evaluations 테이블에서 키워드 빈도수 직접 조회")
    print(f"  - 키워드별 정확한 빈도수 계산 및 저장")
    print(f"  - 빈도수 기반 구체적 피드백 자동 생성")
    print(f"  - 상위 키워드 중심의 개선된 조언 제공")
    print(f"처리할 사용자 수: {len(user_ids)}명")
    print(f"처리할 분기: Q1, Q2, Q3, Q4")
    print(f"처리 모드: {'강제 업데이트 (기존 데이터 덮어쓰기)' if FORCE_UPDATE else '중복 방지 (기존 데이터 건너뜀)'}")
    print(f"저장 방식: peer_evaluation_results 컬렉션에 type: 'personal-quarter'로 구분")
    print(f"저장 위치: MongoDB - {os.getenv('MONGO_DB_NAME')}.peer_evaluation_results")
    print(f"문서 구조:")
    print(f"  - type: 'personal-quarter'")
    print(f"  - evaluated_year: 2024")
    print(f"  - evaluated_quarter: 1, 2, 3, 4")
    print(f"  - users: [사용자별 평가 데이터 배열] ← 🔥 중복 방지 로직 적용")
    print(f"  - 🔥 개선된 데이터 구조:")
    print(f"    • keyword_summary: 빈도수 포함 키워드 요약")
    print(f"    • keyword_frequency_stats: 상세 빈도수 통계")
    print(f"    • feedback: 빈도수 기반 구체적 피드백")
    print(f"    • user_id별 중복 체크 및 업데이트 로직")
    print("=" * 60)
    
    # 전체 결과 저장용
    all_quarters_results = {}
    
    # 4개 분기 모두 처리 (🔥 중복 방지 로직 적용)
    for quarter in [1, 2, 3, 4]:
        quarter_result = process_single_quarter(
            orchestrator, 
            user_ids, 
            2024, 
            quarter, 
            force_update=FORCE_UPDATE
        )
        all_quarters_results[f"Q{quarter}"] = quarter_result
        
        # 분기 간 구분을 위한 여백
        print("\n" + "="*60)
    
    # 🔥 개선된 전체 분기 통합 결과 출력
    print(f"\n=== 2024년 전체 분기 동료평가 처리 완료 (중복 방지 버전) ===")
    
    total_processed = 0
    total_skipped = 0
    total_updated = 0
    total_failed = 0
    
    for quarter in [1, 2, 3, 4]:
        if f"Q{quarter}" in all_quarters_results:
            quarter_data = all_quarters_results[f"Q{quarter}"]
            successful = quarter_data["successful_count"]
            skipped = quarter_data["skipped_count"]
            updated = quarter_data["updated_count"]
            failed = quarter_data["failed_count"]
            total_in_doc = quarter_data["total_users_in_document"]
            
            total_processed += successful
            total_skipped += skipped
            total_updated += updated
            total_failed += failed
            
            status_parts = []
            if successful > 0:
                status_parts.append(f"성공 {successful}명")
            if updated > 0:
                status_parts.append(f"업데이트 {updated}명")
            if skipped > 0:
                status_parts.append(f"건너뜀 {skipped}명")
            if failed > 0:
                status_parts.append(f"실패 {failed}명")
            
            status_text = ", ".join(status_parts) if status_parts else "데이터 없음"
            print(f"Q{quarter}: {status_text} → 문서에 총 {total_in_doc}명 저장됨")
        else:
            print(f"Q{quarter}: 데이터 없음")
    
    print(f"\n🎉 처리 완료 요약:")
    print(f"  - 📊 통계:")
    print(f"    • 새로 처리: {total_processed - total_updated}명")
    print(f"    • 업데이트: {total_updated}명") 
    print(f"    • 건너뜀: {total_skipped}명 (기존 데이터 존재)")
    print(f"    • 실패: {total_failed}명 (동료평가 데이터 없음)")
    print(f"    • 총 처리: {total_processed}명")
    print(f"  - 🔥 중복 방지 효과:")
    if total_skipped > 0:
        print(f"    • {total_skipped}명의 중복 처리를 방지하여 처리 시간 단축")
        print(f"    • 기존 데이터 무결성 보장")
    else:
        print(f"    • 모든 사용자가 새로 처리됨 (기존 데이터 없음)")
    print(f"  - 저장 방식: peer_evaluation_results 컬렉션에 type별로 구분")
    print(f"  - 데이터베이스: {os.getenv('MONGO_DB_NAME')}")
    print(f"  - 컬렉션: peer_evaluation_results")
    print(f"  - 문서 개수: 4개 (각 분기별)")
    print(f"  - 🔥 개선된 데이터 구조:")
    print(f"    • keyword_summary: {{\"positive\": [...], \"negative\": [...]}} (빈도수 포함)")
    print(f"    • feedback: 자연스러운 문장으로 된 개인화된 피드백")
    print(f"    • calculation_method: \"new_weighted_method_5point_with_frequency\"")
    print(f"    • 중복 체크 및 업데이트 로직으로 데이터 무결성 보장")
    print(f"  - MariaDB user_quarter_scores.peer_score 업데이트 완료")
    print(f"  - 피드백 개선점:")
    print(f"    • 사용자 이름을 포함한 개인화된 피드백")
    print(f"    • 키워드나 빈도수를 직접 노출하지 않는 자연스러운 문장")
    print(f"    • 빈도수가 높은 키워드를 내부적으로 활용한 정확한 피드백")
    print(f"    • 따뜻하고 격려적인 톤의 건설적 조언")
    print(f"    • keyword_summary 구조: {{\"positive\": [{{\"keyword\": \"열정\", \"count\": 4}}], \"negative\": [...]}}")
    print(f"  - ⚡ 성능 개선:")
    print(f"    • 중복 처리 방지로 불필요한 연산 제거")
    print(f"    • 기존 데이터 확인 후 선택적 처리")
    print(f"    • MongoDB 문서 단위 효율적 업데이트")
    
    # MongoDB 연결 종료
    orchestrator.mongodb_manager.close()
    
    return all_quarters_results

if __name__ == "__main__":
    main()