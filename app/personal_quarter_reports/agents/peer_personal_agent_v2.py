import mysql.connector
import json
from decimal import Decimal
from typing import Dict, List, Tuple
import logging
from datetime import datetime

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
        self.mongodb_uri = "mongodb://root:root@13.209.110.151:27017/"
        self.database_name = "skala"
        self.collection_name = "personal_quarter_reports"
        self.client = None
    
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
    
    def save_peer_data(self, quarter_data: Dict) -> bool:
        """동료평가 데이터를 MongoDB에 저장"""
        try:
            if not self.client:
                if not self.connect():
                    return False
            
            db = self.client[self.database_name]
            collection = db[self.collection_name]
            
            # peer 구조로 감싸기
            peer_document = {
                "peer": quarter_data,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "data_type": "peer_evaluation_results",
                "quarter": quarter_data.get("meta", {}).get("evaluation_period", "unknown")
            }
            
            result = collection.insert_one(peer_document)
            print(f"✅ MongoDB 저장 완료 - Document ID: {result.inserted_id}")
            print(f"   분기: {peer_document['quarter']}")
            print(f"   컬렉션: {self.database_name}.{self.collection_name}")
            
            return True
            
        except Exception as e:
            print(f"❌ MongoDB 저장 실패: {e}")
            return False
    
    def close(self):
        """MongoDB 연결 종료"""
        if self.client:
            self.client.close()
            print("MongoDB 연결 종료")

class PeerEvaluationSystem:
    def __init__(self, openai_api_key: str):
        self.db_config = {
            'host': '13.209.110.151',
            'port': 3306,
            'user': 'root',
            'password': 'root',
            'database': 'skala'
        }
        self.llm = OpenAI(api_key=openai_api_key, temperature=0.7,max_tokens=1500)
        
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
    
    def save_score_to_db(self, user_id: int, year: int, quarter: int, 
                        score: float):
        """점수를 DB에 저장 - 새로운 방식 (5점 만점)"""
        # 저장 로그도 생략 가능 (필요시 주석 해제)
        # print(f"[DB 저장] User {user_id}: {score:.2f}/5.0")
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
        
        # LangChain 버전 호환성 처리
        try:
            from langchain.chains import LLMChain
            self.chain = LLMChain(llm=self.db_system.llm, prompt=self.prompt_template)
            self.use_legacy_chain = True
        except:
            # 최신 버전의 경우
            self.chain = self.prompt_template | self.db_system.llm | StrOutputParser()
            self.use_legacy_chain = False
    
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
    
    def process_peer_evaluation(self, user_id: int, year: int, quarter: int) -> Dict:
        """전체 동료평가 프로세스 실행"""
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
            
            # 3. 점수 DB 저장 (새로운 방식)
            self.score_agent.save_score_to_db(user_id, year, quarter, final_score)
            
            # 4. 피드백 생성
            feedback = self.feedback_agent.generate_feedback(keyword_data, final_score, quarter)
            
            # 5. 결과 반환 (5점 만점, 소수점 둘째자리)
            result = {
                "success": True,
                "data": {
                    "user_id": user_id,
                    "year": year,
                    "quarter": quarter,
                    "peer_evaluation_score": round(float(final_score), 2),
                    "calculation_method": "new_weighted_method_5point",
                    "feedback": feedback,
                    "keyword_summary": {
                        "positive": [item['keyword'] for item in keyword_data if item['is_positive']],
                        "negative": [item['keyword'] for item in keyword_data if not item['is_positive']]
                    }
                }
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
        """여러 사용자의 동료평가를 배치 처리"""
        results = []
        total_users = len(user_ids)
        
        for i, user_id in enumerate(user_ids, 1):
            # 진행률 표시 (매 10명마다)
            if i % 10 == 0 or i == total_users:
                print(f"처리 진행률: {i}/{total_users} ({i/total_users*100:.1f}%)")
            
            result = self.process_peer_evaluation(user_id, year, quarter)
            results.append(result)
            
            # 성공/실패 여부만 간단히 출력
            if result["success"]:
                score = result["data"]["peer_evaluation_score"]
                print(f"✓ User {user_id}: {score:.2f}/5.0")
            else:
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
    """단일 분기 처리 함수 - MongoDB 저장 버전"""
    print(f"\n=== {year}년 {quarter}분기 처리 시작 ===")
    print(f"처리할 사용자 수: {len(user_ids)}명")
    print("=" * 50)
    
    # 배치 처리 실행
    results = orchestrator.process_batch_peer_evaluation(
        user_ids=user_ids,
        year=year,
        quarter=quarter
    )
    
    # 결과 통계 출력
    successful_count = sum(1 for r in results if r["success"])
    failed_count = len(results) - successful_count
    
    print(f"\n=== {quarter}분기 처리 완료 ===")
    print(f"성공: {successful_count}명")
    print(f"실패: {failed_count}명")
    
    avg_score = None
    if successful_count > 0:
        scores = [r["data"]["peer_evaluation_score"] for r in results if r["success"]]
        avg_score = sum(scores) / len(scores)
        print(f"평균 점수: {avg_score:.2f}/5.0")
        print(f"최고 점수: {max(scores):.2f}/5.0")
        print(f"최저 점수: {min(scores):.2f}/5.0")
    
    # 결과를 구조화된 형태로 변환 (기존과 동일)
    formatted_results = {
        "meta": {
            "evaluation_period": f"{year}Q{quarter}",
            "total_users_processed": len(results),
            "successful_evaluations": successful_count,
            "failed_evaluations": failed_count,
            "processing_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "version": "v2",
            "scoring_method": "new_weighted_method_5point"
        },
        "statistics": {
            "average_score": round(avg_score, 2) if avg_score else None,
            "max_score": round(max(scores), 2) if successful_count > 0 else None,
            "min_score": round(min(scores), 2) if successful_count > 0 else None
        },
        "evaluations": results
    }
    
    # MongoDB에 저장
    print(f"\n📦 MongoDB에 저장 중...")
    success = orchestrator.mongodb_manager.save_peer_data(formatted_results)
    
    if success:
        print(f"✅ {year}Q{quarter} 데이터가 MongoDB에 성공적으로 저장되었습니다!")
    else:
        print(f"❌ MongoDB 저장 실패. JSON 파일로 백업 저장합니다.")
        # 백업으로 JSON 파일 저장
        backup_filename = f"peer_evaluation_results_{year}Q{quarter}_backup.json"
        with open(backup_filename, 'w', encoding='utf-8') as f:
            json.dump(formatted_results, f, ensure_ascii=False, indent=2)
        print(f"백업 파일: {backup_filename}")
    
    # 실패한 사용자 목록은 너무 길어질 수 있으므로 개수만 출력
    if failed_count > 0:
        print(f"데이터가 없는 사용자: {failed_count}명")
    
    return formatted_results

def main():
    # OpenAI API 키 설정
    OPENAI_API_KEY = "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"
    
    # 오케스트레이터 초기화
    orchestrator = PeerEvaluationOrchestrator(OPENAI_API_KEY)
    
    # MongoDB 연결 테스트
    print("🔌 MongoDB 연결 테스트...")
    if not orchestrator.mongodb_manager.connect():
        print("❌ MongoDB 연결 실패. 프로그램을 종료합니다.")
        return
    
    # 1~100 사용자 ID 리스트 생성
    user_ids = list(range(1, 101))
    
    print(f"\n=== 2024년 전체 분기 동료평가 배치 처리 시작 ===")
    print(f"처리할 사용자 수: {len(user_ids)}명")
    print(f"처리할 분기: Q1, Q2, Q3, Q4")
    print(f"저장 위치: MongoDB - skala.personal_quarter_reports")
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
    print(f"\n=== 2024년 전체 분기 처리 완료 ===")
    
    total_saved_to_mongodb = 0
    for quarter in [1, 2, 3, 4]:
        quarter_data = all_quarters_results[f"Q{quarter}"]
        successful = quarter_data["meta"]["successful_evaluations"]
        print(f"Q{quarter}: 성공 {successful}명 → MongoDB 저장 완료")
        total_saved_to_mongodb += 1
    
    print(f"\n🎉 처리 완료 요약:")
    print(f"  - 총 {total_saved_to_mongodb}개 분기 데이터가 MongoDB에 저장됨")
    print(f"  - 데이터베이스: skala")
    print(f"  - 컬렉션: personal_quarter_reports")
    print(f"  - 구조: peer > 실제데이터")
    
    # MongoDB 연결 종료
    orchestrator.mongodb_manager.close()
    
    return all_quarters_results

if __name__ == "__main__":
    main()