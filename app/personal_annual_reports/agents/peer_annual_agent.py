import json
from collections import Counter, defaultdict
from typing import Dict, List, Any, Optional
import openai
from langchain_openai import OpenAI
from langchain.prompts import PromptTemplate
import pymongo
import mysql.connector
from datetime import datetime

class AnnualPeerEvaluationSystem:
    def __init__(self, openai_api_key: str, mysql_config: Dict[str, str] = None):
        """
        연간 동료평가 종합 시스템 초기화
        
        Args:
            openai_api_key: OpenAI API 키
            mysql_config: MySQL 연결 정보 (선택사항)
        """
        # MongoDB 연결 설정
        mongodb_connection_string = "mongodb://root:root@13.209.110.151:27017/"
        self.client = pymongo.MongoClient(mongodb_connection_string)
        self.db = self.client.skala
        
        # 분기별 데이터가 저장된 컬렉션 사용
        self.collection = self.db.personal_quarter_reports
        
        # MySQL 연결 설정 (기본값 또는 전달받은 설정 사용)
        self.mysql_config = mysql_config or {
            'host': '13.209.110.151',
            'port': 3306,
            'database': 'skala',
            'user': 'root',
            'password': 'root',
            'charset': 'utf8mb4'
        }
        
        openai.api_key = openai_api_key
        self.llm = OpenAI(temperature=0.7, openai_api_key=openai_api_key)
        
        # 컬렉션 정보 출력
        self._analyze_collection_structure()
    
    def get_user_info_from_rdb(self, user_id: int) -> Dict[str, Any]:
        """
        RDB에서 사용자 정보 조회
        
        Args:
            user_id: 사용자 ID
            
        Returns:
            사용자 정보 딕셔너리
        """
        try:
            # MySQL 연결
            connection = mysql.connector.connect(**self.mysql_config)
            cursor = connection.cursor(dictionary=True)
            
            # 사용자 기본 정보와 부서, 직책 정보를 JOIN으로 조회
            query = """
            SELECT 
                u.id as user_id,
                u.name,
                u.created_at as start_date,
                d.name as department,
                j.name as job
            FROM users u
            LEFT JOIN departments d ON u.department_id = d.id
            LEFT JOIN jobs j ON u.job_id = j.id
            WHERE u.id = %s
            """
            
            cursor.execute(query, (user_id,))
            result = cursor.fetchone()
            
            if result:
                # 날짜 형식 변환
                start_date = result['start_date'].strftime('%Y-%m-%d') if result['start_date'] else "2024-01-01"
                
                user_info = {
                    "name": result['name'] or f"사용자{user_id}",
                    "department": result['department'] or "미지정",
                    "job": result['job'] or "미지정",
                    "startDate": start_date,
                    "endDate": "2024-12-27"  # 연말 평가 기준 종료일
                }
                
                print(f"✅ 사용자 {user_id} 정보 조회 성공: {user_info['name']} ({user_info['department']})")
                return user_info
            else:
                print(f"⚠️ 사용자 {user_id} 정보를 찾을 수 없습니다.")
                return self._get_default_user_info(user_id)
                
        except mysql.connector.Error as e:
            print(f"MySQL 연결 오류: {e}")
            return self._get_default_user_info(user_id)
        except Exception as e:
            print(f"사용자 정보 조회 중 오류: {e}")
            return self._get_default_user_info(user_id)
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()
    
    def _get_default_user_info(self, user_id: int) -> Dict[str, Any]:
        """
        기본 사용자 정보 반환 (RDB 조회 실패시)
        
        Args:
            user_id: 사용자 ID
            
        Returns:
            기본 사용자 정보
        """
        return {
            "name": f"사용자{user_id}",
            "department": "미지정",
            "job": "미지정", 
            "startDate": "2024-01-01",
            "endDate": "2024-12-27"
        }
    
    def _analyze_collection_structure(self):
        """
        컬렉션 구조 분석 (간단 버전)
        """
        try:
            doc_count = self.collection.count_documents({})
            print(f"📊 personal_quarter_reports 컬렉션: {doc_count}개 문서")
        except Exception as e:
            print(f"구조 분석 중 오류: {e}")
    
    def get_quarterly_data(self, user_id: int, year: int) -> List[Dict]:
        """
        특정 사용자의 연간 분기별 평가 데이터 조회
        
        Args:
            user_id: 사용자 ID
            year: 평가 연도
            
        Returns:
            분기별 평가 데이터 리스트
        """
        quarterly_data = []
        
        try:
            # 모든 분기 문서 조회 (quarter 필드가 "2024Q1" 형태)
            for quarter_num in [1, 2, 3, 4]:
                quarter_str = f"{year}Q{quarter_num}"
                query = {"quarter": quarter_str}
                document = self.collection.find_one(query)
                
                if document and "peer" in document:
                    peer_data = document["peer"]
                    
                    if "evaluations" in peer_data and isinstance(peer_data["evaluations"], list):
                        evaluations = peer_data["evaluations"]
                        
                        for eval_item in evaluations:
                            if (eval_item.get("success") and 
                                "data" in eval_item and
                                eval_item["data"].get("user_id") == user_id):
                                
                                # 분기 정보 추가
                                eval_data = eval_item["data"].copy()
                                if "quarter" not in eval_data:
                                    eval_data["quarter"] = quarter_num
                                if "year" not in eval_data:
                                    eval_data["year"] = year
                                    
                                quarterly_data.append(eval_data)
                                break
                    
        except Exception as e:
            print(f"사용자 {user_id} 데이터 조회 중 오류: {e}")
        
        quarterly_data.sort(key=lambda x: x.get("quarter", 0))
        return quarterly_data
    
    def find_available_users(self, year: int = 2024) -> List[int]:
        """
        지정된 연도에 평가 데이터가 있는 사용자 ID 목록 반환
        
        Args:
            year: 조회할 연도
            
        Returns:
            사용자 ID 리스트
        """
        user_ids = set()
        
        try:
            # 해당 연도의 모든 분기 문서 조회
            for quarter_num in [1, 2, 3, 4]:
                quarter_str = f"{year}Q{quarter_num}"
                query = {"quarter": quarter_str}
                document = self.collection.find_one(query)
                
                if document and "peer" in document:
                    peer_data = document["peer"]
                    
                    if isinstance(peer_data, dict) and "evaluations" in peer_data:
                        evaluations = peer_data["evaluations"]
                        
                        if isinstance(evaluations, list):
                            for eval_item in evaluations:
                                if (eval_item.get("success") and 
                                    "data" in eval_item):
                                    
                                    user_id = eval_item["data"].get("user_id")
                                    if user_id:
                                        user_ids.add(user_id)
            
            user_list = sorted(list(user_ids))
            print(f"🔍 {year}년 평가 데이터가 있는 사용자: {len(user_list)}명")
            return user_list
            
        except Exception as e:
            print(f"사용자 목록 조회 중 오류: {e}")
            return []
    
    def count_keywords_across_quarters(self, quarterly_data: List[Dict]) -> Dict[str, Dict[str, int]]:
        """
        분기별 키워드 빈도 계산
        
        Args:
            quarterly_data: 분기별 평가 데이터
            
        Returns:
            키워드 타입별 빈도 딕셔너리
        """
        positive_counter = Counter()
        negative_counter = Counter()
        
        for data in quarterly_data:
            keyword_summary = data.get("keyword_summary", {})
            
            # 긍정 키워드 카운트
            for keyword in keyword_summary.get("positive", []):
                positive_counter[keyword] += 1
                
            # 부정 키워드 카운트  
            for keyword in keyword_summary.get("negative", []):
                negative_counter[keyword] += 1
        
        return {
            "positive": dict(positive_counter),
            "negative": dict(negative_counter)
        }
    
    def get_top_keywords(self, keyword_counts: Dict[str, int], top_n: int = 5) -> List[str]:
        """
        상위 N개 키워드 추출
        
        Args:
            keyword_counts: 키워드별 빈도
            top_n: 상위 개수
            
        Returns:
            상위 키워드 리스트
        """
        return [keyword for keyword, count in 
                Counter(keyword_counts).most_common(top_n)]
    
    def calculate_annual_score(self, quarterly_data: List[Dict]) -> float:
        """
        연간 종합 점수 계산 (가중평균)
        
        Args:
            quarterly_data: 분기별 평가 데이터
            
        Returns:
            연간 종합 점수
        """
        if not quarterly_data:
            return 0.0
            
        # 가중치: Q1(20%), Q2(25%), Q3(25%), Q4(30%)
        weights = {1: 0.2, 2: 0.25, 3: 0.25, 4: 0.3}
        
        weighted_sum = 0.0
        total_weight = 0.0
        
        for data in quarterly_data:
            quarter = data["quarter"]
            score = data["peer_evaluation_score"]
            weight = weights.get(quarter, 0.25)
            
            weighted_sum += score * weight
            total_weight += weight
        
        return round(weighted_sum / total_weight if total_weight > 0 else 0.0, 2)
    
    def analyze_growth_trend(self, quarterly_data: List[Dict]) -> Dict[str, Any]:
        """
        성장 트렌드 분석
        
        Args:
            quarterly_data: 분기별 평가 데이터
            
        Returns:
            성장 트렌드 분석 결과
        """
        if len(quarterly_data) < 2:
            return {"trend": "insufficient_data", "growth_rate": 0.0}
            
        scores = [data["peer_evaluation_score"] for data in quarterly_data]
        
        # 전체 성장률
        growth_rate = round(scores[-1] - scores[0], 2)
        
        # LLM을 이용한 트렌드 분석
        trend = self._analyze_trend_with_llm(scores, growth_rate)
        
        return {
            "trend": trend,
            "growth_rate": growth_rate,
            "quarterly_scores": scores,
            "best_quarter": quarterly_data[scores.index(max(scores))]["quarter"],
            "improvement_periods": self._identify_improvement_periods(scores)
        }
    
    def _analyze_trend_with_llm(self, scores: List[float], growth_rate: float) -> str:
        """
        LLM을 이용한 성장 트렌드 분석
        
        Args:
            scores: 분기별 점수 리스트
            growth_rate: 전체 성장률
            
        Returns:
            트렌드 분석 결과
        """
        quarterly_progression = " → ".join([f"Q{i+1}({score})" for i, score in enumerate(scores)])
        
        prompt_template = PromptTemplate(
            input_variables=["quarterly_progression", "growth_rate", "score_count"],
            template="""
당신은 데이터 분석 전문가입니다. 다음 분기별 점수 변화를 분석하여 성장 트렌드를 판단해주세요.

분기별 점수 변화: {quarterly_progression}
전체 성장률: {growth_rate}점
데이터 기간: {score_count}개 분기

다음 중 하나의 트렌드를 선택해주세요:
1. "strong_growth" - 뚜렷하고 강한 성장 패턴
2. "steady_growth" - 꾸준하고 안정적인 성장 패턴  
3. "stable" - 안정적이고 일관된 성과 유지
4. "slight_decline" - 소폭의 하락 또는 부진
5. "significant_decline" - 뚜렷한 하락 패턴
6. "fluctuating" - 기복이 있는 변동 패턴

분석 기준:
- 전체 성장률뿐만 아니라 분기별 변화의 일관성도 고려
- 점수의 절대값과 상대적 변화량 모두 고려
- 중간 분기의 변동성도 반영

트렌드만 답변해주세요 (예: "steady_growth")
            """
        )
        
        formatted_prompt = prompt_template.format(
            quarterly_progression=quarterly_progression,
            growth_rate=growth_rate,
            score_count=len(scores)
        )
        
        try:
            response = self.llm.invoke(formatted_prompt)
            trend = response.strip().replace('"', '').replace("'", "")
            
            # 유효한 트렌드인지 확인
            valid_trends = [
                "strong_growth", "steady_growth", "stable", 
                "slight_decline", "significant_decline", "fluctuating"
            ]
            
            if trend in valid_trends:
                return trend
            else:
                # LLM 응답이 유효하지 않으면 기본 로직 사용
                return self._fallback_trend_analysis(growth_rate)
                
        except Exception as e:
            print(f"LLM 트렌드 분석 중 오류: {e}")
            # 오류 시 기본 로직 사용
            return self._fallback_trend_analysis(growth_rate)
    
    def _fallback_trend_analysis(self, growth_rate: float) -> str:
        """
        LLM 실패 시 사용할 기본 트렌드 분석
        
        Args:
            growth_rate: 성장률
            
        Returns:
            트렌드 분석 결과
        """
        if growth_rate > 0.3:
            return "strong_growth"
        elif growth_rate > 0.1:
            return "steady_growth"
        elif growth_rate > -0.1:
            return "stable"
        elif growth_rate > -0.3:
            return "slight_decline"
        else:
            return "significant_decline"
    
    def _identify_improvement_periods(self, scores: List[float]) -> List[str]:
        """성장 구간 식별"""
        periods = []
        
        for i in range(1, len(scores)):
            if scores[i] > scores[i-1] + 0.1:
                periods.append(f"Q{i}에서 Q{i+1} 성장")
                
        return periods
    
    def _generate_final_comment(self, user_id: int, quarterly_data: List[Dict], 
                              growth_analysis: Dict, keyword_analysis: Dict, user_name: str = None) -> str:
        """
        1년 흐름이 보이는 최종 코멘트 생성 (feedback과 통합)
        
        Args:
            user_id: 사용자 ID
            quarterly_data: 분기별 데이터
            growth_analysis: 성장 분석 결과
            keyword_analysis: 키워드 분석 결과
            user_name: 사용자 이름 (선택사항)
            
        Returns:
            통합된 최종 코멘트
        """
        scores = [data["peer_evaluation_score"] for data in quarterly_data]
        growth_rate = growth_analysis["growth_rate"]
        
        # 분기별 점수 변화 문자열 생성
        quarters_desc = " → ".join([f"Q{i+1}({score})" for i, score in enumerate(scores)])
        
        # 상위 키워드 추출
        top_positive = self.get_top_keywords(keyword_analysis["positive"], 3)
        top_negative = self.get_top_keywords(keyword_analysis["negative"], 2)
        
        # 사용자 이름 설정 (제공되지 않으면 기본값)
        name = user_name if user_name else f"사용자{user_id}"
        
        # 성장 트렌드 설명
        trend_descriptions = {
            "strong_growth": "눈에 띄는 성장",
            "steady_growth": "꾸준한 발전", 
            "stable": "안정적인 성과",
            "slight_decline": "일시적 부진",
            "significant_decline": "개선 필요",
            "fluctuating": "변동성 있는 발전"
        }
        
        trend_desc = trend_descriptions.get(growth_analysis["trend"], "변화 관찰")
        
        # 간결한 LLM 프롬프트로 실제 피드백 생성
        prompt_template = PromptTemplate(
            input_variables=[
                "name", "quarters_desc", "growth_rate", "trend_desc", 
                "top_positive", "top_negative"
            ],
            template="""
{name}님의 2024년 동료평가 결과를 바탕으로 간결한 피드백을 작성해주세요.

- 분기별 점수: {quarters_desc}
- 성장률: {growth_rate}점 ({trend_desc})
- 주요 강점: {top_positive}
- 개선 포인트: {top_negative}

다음 조건으로 2-3문장 작성:
1. 실제 이름 사용
2. 구체적인 강점과 개선점 언급
3. 200자 이내로 작성
4. 격려하는 톤으로 마무리

예: "{name}님은 2024년 Q1(3.8)에서 Q4(4.1)로 꾸준히 성장하며 0.3점 향상을 보였습니다. 협업역량과 문제해결력에서 특히 뛰어났으나, 소통 부분에서 개선이 필요합니다. 지속적인 발전으로 더 큰 성과를 기대합니다."

한국어로 자연스럽게 작성해주세요.
            """
        )
        
        formatted_prompt = prompt_template.format(
            name=name,
            quarters_desc=quarters_desc,
            growth_rate=growth_rate,
            trend_desc=trend_desc,
            top_positive=", ".join(top_positive),
            top_negative=", ".join(top_negative)
        )
        
        try:
            response = self.llm.invoke(formatted_prompt)
            final_comment = response.strip()
            
            # 불필요한 따옴표나 템플릿 형태 제거
            final_comment = final_comment.replace('"', '').replace("'", "")
            
            # 길이 제한 확인 및 조정 (최대 200자)
            if len(final_comment) > 200:
                final_comment = final_comment[:200] + "..."
            
            return final_comment
            
        except Exception as e:
            # LLM 호출 실패 시 간결한 기본 코멘트 반환
            fallback = f"{name}님은 2024년 {quarters_desc}의 성과를 달성하며 {abs(growth_rate):.2f}점 {'성장' if growth_rate >= 0 else '변화'}했습니다. {', '.join(top_positive[:2])} 등의 강점을 보였으나 {', '.join(top_negative[:1])} 부분에서 개선이 필요합니다."
            return fallback[:200]
    
    def process_annual_evaluation(self, user_id: int, year: int = 2024) -> Dict[str, Any]:
        """
        연간 종합 평가 처리
        
        Args:
            user_id: 사용자 ID
            year: 평가 연도
            
        Returns:
            연간 종합 평가 결과
        """
        try:
            print(f"\n🔄 사용자 {user_id}의 {year}년 연간 평가를 처리합니다...")
            
            # 1. 사용자 정보 조회
            user_info = self.get_user_info_from_rdb(user_id)
            
            # 2. 분기별 데이터 조회
            quarterly_data = self.get_quarterly_data(user_id, year)
            
            if not quarterly_data:
                return {
                    "success": False,
                    "error": f"사용자 {user_id}의 {year}년 평가 데이터를 찾을 수 없습니다."
                }
            
            # 3. 키워드 분석
            keyword_counts = self.count_keywords_across_quarters(quarterly_data)
            
            # 4. 연간 점수 계산
            annual_score = self.calculate_annual_score(quarterly_data)
            
            # 5. 성장 트렌드 분석
            growth_analysis = self.analyze_growth_trend(quarterly_data)
            
            # 6. Top 5 키워드 추출
            top_positive_keywords = self.get_top_keywords(keyword_counts["positive"], 5)
            top_negative_keywords = self.get_top_keywords(keyword_counts["negative"], 5)
            
            # 7. 통합된 최종 코멘트 생성 (사용자 이름 포함)
            final_comment = self._generate_final_comment(
                user_id, quarterly_data, growth_analysis, keyword_counts, user_info["name"]
            )
            
            # 8. 결과 구성 (메타데이터 포함)
            result = {
                "success": True,
                "data": {
                    "type": "individual-year-end",
                    "title": "2024 연말 성과 리포트",
                    "employee": user_info,
                    "evaluation": {
                        "user_id": user_id,
                        "year": year,
                        "quarter": "Annual",
                        "peer_evaluation_score": annual_score,
                        "calculation_method": "annual_weighted_average",
                        "peerFeedback": [
                            {
                                "type": "positive",
                                "keywords": top_positive_keywords
                            },
                            {
                                "type": "negative", 
                                "keywords": top_negative_keywords
                            }
                        ],
                        "finalComment": final_comment
                    }
                }
            }
            
            return result
            
        except Exception as e:
            return {
                "success": False,
                "error": f"연간 평가 처리 중 오류 발생: {str(e)}"
            }
    
    def save_batch_evaluations_to_mongo(self, batch_results: Dict[str, Any]) -> bool:
        """
        일괄 처리된 연간 평가 결과를 MongoDB에 저장
        
        Args:
            batch_results: 일괄 처리 결과
            
        Returns:
            저장 성공 여부
        """
        try:
            # 연간 평가용 컬렉션에 저장
            annual_collection = self.db.personal_annual_reports
            
            save_data = {
                "evaluation_type": "annual_batch",
                "year": 2024,
                "processed_date": datetime.now(),
                "batch_id": f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "results": batch_results
            }
            
            # 새로운 일괄 처리 결과로 저장
            result = annual_collection.insert_one(save_data)
            
            print(f"💾 일괄 연간 평가 결과를 MongoDB에 저장했습니다. (Document ID: {result.inserted_id})")
            print(f"📊 총 {batch_results['meta']['total_users_processed']}명의 결과가 하나의 문서로 저장되었습니다.")
            return True
            
        except Exception as e:
            print(f"MongoDB 저장 중 오류 발생: {str(e)}")
            return False
    
    def process_multiple_users(self, user_ids: List[int], year: int = 2024) -> Dict[str, Any]:
        """
        여러 사용자의 연간 평가 일괄 처리 (로컬 파일 저장 없이 MongoDB만 사용)
        
        Args:
            user_ids: 사용자 ID 리스트
            year: 평가 연도
            
        Returns:
            일괄 처리 결과
        """
        results = {
            "meta": {
                "evaluation_period": f"{year}-Annual",
                "total_users_processed": len(user_ids),
                "successful_evaluations": 0,
                "failed_evaluations": 0,
                "processing_date": datetime.now().isoformat(),
                "version": "v1",
                "scoring_method": "annual_weighted_average"
            },
            "statistics": {
                "average_score": 0.0,
                "max_score": 0.0,
                "min_score": 0.0
            },
            "evaluations": []
        }
        
        scores = []
        
        for user_id in user_ids:
            print(f"\n🔄 Processing user {user_id}...")
            
            result = self.process_annual_evaluation(user_id, year)
            
            if result["success"]:
                results["evaluations"].append(result)
                results["meta"]["successful_evaluations"] += 1
                scores.append(result["data"]["evaluation"]["peer_evaluation_score"])
                
            else:
                results["meta"]["failed_evaluations"] += 1
                results["evaluations"].append(result)
        
        # 통계 계산
        if scores:
            results["statistics"] = {
                "average_score": round(sum(scores) / len(scores), 2),
                "max_score": round(max(scores), 2),
                "min_score": round(min(scores), 2)
            }
        
        # MongoDB에 일괄 저장
        self.save_batch_evaluations_to_mongo(results)
        
        return results


# 사용 예시
if __name__ == "__main__":
    # OpenAI API 키 설정 (실제 키로 변경 필요)
    OPENAI_API_KEY = "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"
    
    # MySQL 설정 (선택사항 - 기본값 사용 가능)
    mysql_config = {
        'host': '13.209.110.151',
        'port': 3306,
        'database': 'skala',
        'user': 'root',
        'password': 'root',
        'charset': 'utf8mb4'
    }
    
    # 시스템 초기화
    system = AnnualPeerEvaluationSystem(
        openai_api_key=OPENAI_API_KEY,
        mysql_config=mysql_config
    )
    
    # 사용 가능한 사용자 ID 확인
    available_users = system.find_available_users(year=2024)
    
    if not available_users:
        print("❌ 2024년 평가 데이터가 있는 사용자를 찾을 수 없습니다.")
        print("데이터베이스 연결 및 컬렉션을 확인해주세요.")
        exit(1)
    
    # 모든 사용자 일괄 처리 (로컬 파일 저장 없이 MongoDB만 사용)
    print(f"\n🔄 모든 사용자({len(available_users)}명) 일괄 처리를 시작합니다...")
    print("📝 결과는 로컬 파일에 저장되지 않고 MongoDB에만 저장됩니다.")
    
    batch_results = system.process_multiple_users(available_users, year=2024)
    
    # 결과 요약 출력
    print(f"\n📊 처리 완료 요약:")
    print(f"  - 총 처리 사용자: {batch_results['meta']['total_users_processed']}명")
    print(f"  - 성공: {batch_results['meta']['successful_evaluations']}명")
    print(f"  - 실패: {batch_results['meta']['failed_evaluations']}명")
    if batch_results['statistics']['average_score'] > 0:
        print(f"  - 평균 점수: {batch_results['statistics']['average_score']}")
        print(f"  - 최고 점수: {batch_results['statistics']['max_score']}")
        print(f"  - 최저 점수: {batch_results['statistics']['min_score']}")
    
    print(f"\n✅ 모든 결과가 MongoDB 'personal_annual_reports_batch' 컬렉션에 저장되었습니다.")
    print(f"🗂️ 컬렉션에서 batch_id로 검색하여 결과를 확인할 수 있습니다.")