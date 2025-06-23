import mysql.connector
import json
from decimal import Decimal
from typing import Dict, List, Tuple
import logging

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

class PeerEvaluationSystem:
    def __init__(self, openai_api_key: str):
        self.db_config = {
            'host': '13.209.110.151',
            'port': 3306,
            'user': 'root',
            'password': 'root',
            'database': 'skala'
        }
        self.llm = OpenAI(api_key=openai_api_key, temperature=0.7)
        
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
        
        # 디버깅을 위한 로그 추가
        print(f"[DB 조회] User ID: {evaluatee_user_id}, Year: {year}, Quarter: {quarter}")
        
        conn = self.get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 먼저 해당 사용자의 모든 데이터 확인
        debug_query = """
        SELECT DISTINCT evaluatee_user_id, evaluation_year, evaluation_quarter 
        FROM peer_keyword_evaluations 
        WHERE evaluatee_user_id = %s
        """
        cursor.execute(debug_query, (evaluatee_user_id,))
        debug_results = cursor.fetchall()
        print(f"[DB 디버그] User {evaluatee_user_id}의 모든 평가 데이터: {debug_results}")
        
        # 원래 쿼리 실행
        cursor.execute(query, (evaluatee_user_id, year, quarter))
        results = cursor.fetchall()
        print(f"[DB 결과] 조회된 키워드 수: {len(results)}")
        
        conn.close()
        
        return results

# 점수 계산 함수들
def calculate_dimension_scores(keyword_data: List[Dict]) -> Dict[str, float]:
    """8개 차원별 점수 계산"""
    dimensions = {
        'passionate': 0.0, 'professional': 0.0, 'proactive': 0.0, 'people': 0.0,
        'pessimistic': 0.0, 'political': 0.0, 'passive': 0.0, 'personal': 0.0
    }
    
    total_keywords = sum(int(item['keyword_count']) for item in keyword_data)
    
    if total_keywords == 0:
        return dimensions
        
    for item in keyword_data:
        count = int(item['keyword_count'])
        for dim in dimensions.keys():
            weight_key = f"{dim}_weight"
            # Decimal을 float로 변환
            weight_value = float(item[weight_key]) if item[weight_key] else 0.0
            dimensions[dim] += count * weight_value
    
    # 정규화 및 float 변환
    for dim in dimensions:
        dimensions[dim] = float(dimensions[dim] / total_keywords)
        
    return dimensions

def calculate_final_score(dimensions: Dict[str, float]) -> float:
    """최종 동료평가 점수 계산 (5점 만점)"""
    positive_avg = (dimensions['passionate'] + dimensions['professional'] + 
                   dimensions['proactive'] + dimensions['people']) / 4
    
    negative_avg = (dimensions['pessimistic'] + dimensions['political'] + 
                   dimensions['passive'] + dimensions['personal']) / 4
    
    # 5점 스케일로 정규화: (긍정평균 - 부정평균) * 2.5 + 2.5
    final_score = (positive_avg - negative_avg) * 2.5 + 2.5
    return max(0.0, min(5.0, final_score))  # 0-5점 범위 제한

class PeerScoreAgent:
    """동료 점수 평가 Agent"""
    
    def __init__(self, db_system: PeerEvaluationSystem):
        self.db_system = db_system
    
    def save_score_to_db(self, user_id: int, year: int, quarter: int, 
                        score: float, dimensions: Dict[str, float]):
        """점수를 DB에 저장 - 우선 로그로만 출력"""
        # DB 테이블 구조를 확인한 후 실제 저장 로직 구현
        print(f"[DB 저장 시뮬레이션]")
        print(f"User ID: {user_id}, Year: {year}, Quarter: {quarter}")
        print(f"Final Score: {score:.2f}/5.0")
        print(f"Dimension Scores: {dimensions}")
        
        # 실제 테이블이 있다면 아래와 같이 구현
        # conn = self.db_system.get_db_connection()
        # cursor = conn.cursor()
        # 
        # # 실제 테이블 구조에 맞게 수정 필요
        # insert_query = """
        # INSERT INTO [실제_테이블명] 
        # ([실제_컬럼명들])
        # VALUES (%s, %s, %s, %s, ...)
        # """
        # 
        # cursor.execute(insert_query, (user_id, year, quarter, score, ...))
        # conn.commit()
        # conn.close()
        
        return True

class FeedbackGenerationAgent:
    """평가 생성 Agent"""
    
    def __init__(self, db_system: PeerEvaluationSystem):
        self.db_system = db_system
        self.prompt_template = PromptTemplate(
            input_variables=["quarter", "positive_keywords", "negative_keywords", "score"],
            template="""
당신은 HR 전문가입니다. 동료평가 결과를 바탕으로 건설적이고 구체적인 피드백을 작성해주세요.

{quarter}분기 동료평가 결과:
- 최종 점수: {score}점
- 긍정 키워드: {positive_keywords}
- 부정 키워드: {negative_keywords}

다음 형식으로 피드백을 작성해주세요:
1. 동료들이 선택한 키워드 요약
2. 주요 강점 분석 (긍정 키워드 기반)
3. 개선 포인트 제시 (부정 키워드 기반)
4. 구체적인 성장 방향 제언

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
            
            # 2. 점수 계산 (별도 함수 사용)
            dimensions = calculate_dimension_scores(keyword_data)
            final_score = calculate_final_score(dimensions)
            
            # 3. 점수 DB 저장
            self.score_agent.save_score_to_db(user_id, year, quarter, final_score, dimensions)
            
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
                    "dimension_scores": {k: round(float(v), 3) for k, v in dimensions.items()},
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
        
        for user_id in user_ids:
            print(f"\n=== Processing User ID: {user_id} ===")
            result = self.process_peer_evaluation(user_id, year, quarter)
            results.append(result)
            
            # 성공/실패 여부 출력
            if result["success"]:
                score = result["data"]["peer_evaluation_score"]
                print(f"✓ User {user_id}: {score}/5.0 처리 완료")
            else:
                print(f"✗ User {user_id}: {result['message']}")
        
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

# 사용 예시
def main():
    # OpenAI API 키 설정
    OPENAI_API_KEY = "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"
    
    # 오케스트레이터 초기화
    orchestrator = PeerEvaluationOrchestrator(OPENAI_API_KEY)
    
    year, quarter = 2024, 1
    
    # 1~100번 사용자 모두 처리
    print(f"=== {year}년 {quarter}분기 동료평가 배치 처리 시작 ===")
    user_ids = list(range(1, 101))  # 1~100
    batch_results = orchestrator.process_batch_peer_evaluation(user_ids, year, quarter)
    
    # 성공한 결과만 필터링
    successful_results = [r for r in batch_results if r["success"]]
    failed_results = [r for r in batch_results if not r["success"]]
    
    print(f"\n=== 처리 결과 요약 ===")
    print(f"✓ 성공: {len(successful_results)}명")
    print(f"✗ 실패: {len(failed_results)}명")
    print(f"총 처리: {len(batch_results)}명")
    
    # 성공한 사용자들의 점수 분포 출력
    if successful_results:
        scores = [r["data"]["peer_evaluation_score"] for r in successful_results]
        print(f"\n점수 분포:")
        print(f"평균: {sum(scores)/len(scores):.2f}")
        print(f"최고: {max(scores):.2f}")
        print(f"최저: {min(scores):.2f}")
    
    # 결과를 파일로 저장
    with open(f'peer_evaluation_results_{year}Q{quarter}.json', 'w', encoding='utf-8') as f:
        json.dump(batch_results, f, ensure_ascii=False, indent=2)
    
    print(f"\n결과가 peer_evaluation_results_{year}Q{quarter}.json 파일로 저장되었습니다.")

if __name__ == "__main__":
    main()


# import mysql.connector

# def check_database_users():
#     """데이터베이스에 있는 사용자 데이터 확인"""
#     db_config = {
#         'host': '13.209.110.151',
#         'port': 3306,
#         'user': 'root',
#         'password': 'root',
#         'database': 'skala'
#     }
    
#     conn = mysql.connector.connect(**db_config)
#     cursor = conn.cursor()
    
#     # 1. 모든 evaluatee_user_id 조회
#     print("=== 동료평가 데이터가 있는 모든 사용자 ===")
#     cursor.execute("""
#         SELECT DISTINCT evaluatee_user_id 
#         FROM peer_keyword_evaluations 
#         ORDER BY evaluatee_user_id
#     """)
#     all_users = [row[0] for row in cursor.fetchall()]
#     print(f"총 사용자 수: {len(all_users)}")
#     print(f"사용자 ID 범위: {min(all_users)} ~ {max(all_users)}")
#     print(f"사용자 목록: {all_users}")
    
#     # 2. 2024년 1분기 데이터가 있는 사용자
#     print(f"\n=== 2024년 1분기 데이터가 있는 사용자 ===")
#     cursor.execute("""
#         SELECT DISTINCT evaluatee_user_id 
#         FROM peer_keyword_evaluations 
#         WHERE evaluation_year = 2024 AND evaluation_quarter = 1
#         ORDER BY evaluatee_user_id
#     """)
#     q1_users = [row[0] for row in cursor.fetchall()]
#     print(f"2024Q1 사용자 수: {len(q1_users)}")
#     print(f"사용자 목록: {q1_users}")
    
#     # 3. 연도/분기별 사용자 수
#     print(f"\n=== 연도/분기별 사용자 수 ===")
#     cursor.execute("""
#         SELECT evaluation_year, evaluation_quarter, COUNT(DISTINCT evaluatee_user_id) as user_count
#         FROM peer_keyword_evaluations 
#         GROUP BY evaluation_year, evaluation_quarter
#         ORDER BY evaluation_year, evaluation_quarter
#     """)
#     for row in cursor.fetchall():
#         year, quarter, count = row
#         print(f"{year}년 {quarter}분기: {count}명")
    
#     # 4. 1~100 범위에서 빠진 사용자 확인
#     print(f"\n=== 1~100 범위에서 빠진 사용자 ===")
#     full_range = set(range(1, 101))
#     existing_users = set(all_users)
#     missing_users = sorted(full_range - existing_users)
#     print(f"빠진 사용자 수: {len(missing_users)}")
#     if len(missing_users) <= 20:  # 너무 많으면 일부만 출력
#         print(f"빠진 사용자: {missing_users}")
#     else:
#         print(f"빠진 사용자 (일부): {missing_users[:20]}...")
    
#     conn.close()

# if __name__ == "__main__":
#     check_database_users()