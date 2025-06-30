import os
import json
import mysql.connector
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv
from pymongo import MongoClient
from collections import Counter

# LangChain 최신 버전 호환
try:
    from langchain_openai import ChatOpenAI
except ImportError:
    from langchain_community.chat_models import ChatOpenAI

from langchain.prompts import PromptTemplate
try:
    from langchain.chains import LLMChain
except ImportError:
    from langchain_core.runnables import RunnablePassthrough
    from langchain_core.output_parsers import StrOutputParser

# .env 파일 로드
load_dotenv()

class FinalPerformanceReviewGenerator:
    """최종 성과 리뷰 생성기 v2 (weekly_evaluation_results 연동)"""
    
    def __init__(self):
        # MariaDB 설정
        self.db_config = {
            'host': os.getenv("DB_HOST"),
            'port': int(os.getenv("DB_PORT")),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'database': os.getenv("DB_NAME")
        }
        
        # MongoDB 설정
        self.mongo_host = os.getenv("MONGO_HOST")
        self.mongo_port = int(os.getenv("MONGO_PORT"))
        self.mongo_user = os.getenv("MONGO_USER")
        self.mongo_password = os.getenv("MONGO_PASSWORD")
        self.mongo_db_name = os.getenv("MONGO_DB_NAME")
        
        self.mongodb_uri = f"mongodb://{self.mongo_user}:{self.mongo_password}@{self.mongo_host}:{self.mongo_port}/"
        self.mongo_client = None
        
        # OpenAI GPT-4o 설정
        openai_api_key = os.getenv("OPENAI_API_KEY")
        self.llm = ChatOpenAI(
            model="gpt-4o",
            api_key=openai_api_key,
            temperature=0,  # 일관성을 위해 0으로 설정
            max_tokens=4000
        )
        
        # 성장 방향 제언 프롬프트 템플릿
        self.growth_advice_prompt = PromptTemplate(
            input_variables=["user_name", "weekly_summaries", "qualitative_evaluations", "peer_feedbacks", "year"],
            template="""
당신은 경험이 풍부한 HR 전문가입니다. 
한 직원의 {year}년 전체 평가 데이터를 종합적으로 분석하여 성장 방향을 제언해주세요.

평가 대상: {user_name}님
분석 기간: {year}년 전체 (1~4분기)

【주간 업무 요약 (4개 분기)】
{weekly_summaries}

【정성 평가 결과 (4개 분기)】
{qualitative_evaluations}

【동료 평가 피드백 (4개 분기)】
{peer_feedbacks}

다음 구조로 성장 방향을 제언해주세요:

1. **연간 성과 종합 분석** (2-3줄)
   - 1년간 일관되게 나타난 강점과 성장 영역을 종합적으로 분석

2. **피드백 키워드 변화 추이 분석** (2-3줄)
   - 분기별로 받은 피드백의 변화 패턴 분석
   - 일관된 강점과 지속적 개선점 파악

3. **핵심 성장 포인트** (3-4개 항목)
   - 가장 중요한 성장 영역을 우선순위별로 제시

4. **장기적 커리어 방향성** (2-3줄)
   - 현재 역량을 바탕으로 한 중장기 발전 방향
   - 전문성 강화 및 리더십 개발 방향

따뜻하고 격려적인 톤을 유지하되, 데이터에 기반한 분석을 제공해주세요.
분기별 데이터의 변화 패턴을 차근차근 분석한 후 체계적으로 답변해주세요.
"""
        )
        
        # 종합 Comment 프롬프트 템플릿 (개선된 버전)
        self.comprehensive_comment_prompt = PromptTemplate(
            input_variables=["user_name", "weekly_scores", "quarterly_ratings", "key_achievements", "strengths", "improvement_areas", "qualitative_evaluations", "peer_keywords", "year"],
            template="""
당신은 경험이 풍부한 HR 전문가입니다.
{user_name}님의 {year}년 전체 성과 데이터를 바탕으로 간결하고 확정적인 총평을 작성해주세요.

【정량 데이터】
분기별 점수: {weekly_scores}
분기별 등급: {quarterly_ratings}

【주요 성취 (연간)】
{key_achievements}

【강점 및 개선점 (연간 분석 결과)】
강점: {strengths}
개선점: {improvement_areas}

【정성 평가 데이터 (4개 분기)】
{qualitative_evaluations}

【동료 피드백 키워드 (연간 집계)】
{peer_keywords}

다음 구조로 총 4줄의 간결한 총평을 작성해주세요:

1. **1년간 성과 요약** (1줄)
   - 분기별 등급 변화와 점수 추세를 바탕으로 "꾸준한 성장세" 또는 "안정적 성과 유지" 등

2. **핵심 특징 한 줄 정리** (1줄)
   - 강점 데이터를 바탕으로 "팀 내 든든한 협업 파트너", "창의적 문제 해결사" 등 핵심 역할 정의

3. **올해의 가장 큰 성취** (1줄)
   - 주요 성취 데이터에서 가장 임팩트 있는 성과 선택하여 언급

4. **내년 기대 포인트** (1줄)
   - 현재 강점과 성장 궤도를 바탕으로 한 기대감 표현

확정적이고 총평다운 톤으로 작성해주세요.
모든 데이터를 종합적으로 분석한 후 체계적으로 답변해주세요.
"""
        )
        
        # LangChain 체인 설정
        try:
            self.growth_advice_chain = self.growth_advice_prompt | self.llm | StrOutputParser()
            self.comprehensive_comment_chain = self.comprehensive_comment_prompt | self.llm | StrOutputParser()
            self.use_legacy_chain = False
        except:
            self.growth_advice_chain = LLMChain(llm=self.llm, prompt=self.growth_advice_prompt)
            self.comprehensive_comment_chain = LLMChain(llm=self.llm, prompt=self.comprehensive_comment_prompt)
            self.use_legacy_chain = True
        
        print(f"📋 최종 성과 리뷰 생성 시스템 v2 초기화 완료")
        print(f"MariaDB: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")
        print(f"MongoDB: {self.mongo_host}:{self.mongo_port}/{self.mongo_db_name}")
        print(f"AI 모델: GPT-4o (Temperature: 0)")
        print(f"🔥 개선사항: weekly_evaluation_results 데이터 연동")
    
    def get_db_connection(self):
        """MariaDB 연결"""
        return mysql.connector.connect(**self.db_config)
    
    def connect_mongodb(self):
        """MongoDB 연결"""
        try:
            self.mongo_client = MongoClient(self.mongodb_uri)
            self.mongo_client.admin.command('ping')
            print("✅ MongoDB 연결 성공!")
            return True
        except Exception as e:
            print(f"❌ MongoDB 연결 실패: {e}")
            return False
    
    def get_all_users(self) -> List[Dict]:
        """MariaDB users 테이블에서 모든 사용자 조회"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT id, name FROM users ORDER BY id")
            users = cursor.fetchall()
            conn.close()
            
            print(f"✅ 총 {len(users)}명의 사용자 조회 완료")
            return users
        except Exception as e:
            print(f"❌ 사용자 조회 실패: {e}")
            return []
    
    def get_quarterly_data_from_collection(self, collection_name: str, user_id: int, year: int, field_name: str) -> List:
        """특정 컬렉션에서 사용자의 분기별 데이터 조회"""
        try:
            if not self.mongo_client:
                if not self.connect_mongodb():
                    return []
            
            db = self.mongo_client[self.mongo_db_name]
            collection = db[collection_name]
            
            quarterly_data = []
            
            # 1~4분기 각각 조회
            for quarter in [1, 2, 3, 4]:
                document = collection.find_one({
                    "type": "personal-quarter",
                    "evaluated_year": year,
                    "evaluated_quarter": quarter
                })
                
                if document and "users" in document:
                    # 해당 사용자 데이터 찾기
                    for user_data in document["users"]:
                        if user_data.get("user_id") == user_id:
                            field_value = user_data.get(field_name)
                            if field_value is not None:
                                if field_name == "weekly_score":
                                    quarterly_data.append({"quarter": quarter, "score": field_value})
                                elif field_name == "keyword_summary":
                                    quarterly_data.append({"quarter": quarter, "keywords": field_value})
                                else:
                                    if field_value and field_value.strip():
                                        quarterly_data.append(f"[{quarter}분기] {field_value.strip()}")
                            break
            
            return quarterly_data
            
        except Exception as e:
            print(f"❌ {collection_name} 컬렉션에서 사용자 {user_id} 데이터 조회 실패: {e}")
            return []
    
    def get_weekly_evaluation_summary(self, user_id: int, year: int) -> Dict:
        """🔥 새로운 함수: weekly_evaluation_results에서 연간 요약 데이터 조회"""
        try:
            if not self.mongo_client:
                if not self.connect_mongodb():
                    return {}
            
            db = self.mongo_client[self.mongo_db_name]
            collection = db["weekly_evaluation_results"]
            
            # data_type: "personal-annual"로 문서 조회
            document = collection.find_one({
                "data_type": "personal-annual"
            })
            
            if not document or "users" not in document:
                print(f"❌ weekly_evaluation_results 연간 문서 구조 오류")
                return {}
            
            # 사용자 ID를 문자열로 변환하여 검색
            user_id_str = str(user_id)
            
            # users 객체에서 해당 사용자 찾기
            if user_id_str not in document["users"]:
                print(f"❌ 사용자 {user_id} 데이터가 weekly_evaluation_results에 없음")
                return {}
            
            user_data = document["users"][user_id_str]
            annual_report = user_data.get("annual_report", {})
            
            # 분기별 등급 추출
            quarterly_ratings = []
            quarterly_performance = annual_report.get("quarterlyPerformance", [])
            for perf in quarterly_performance:
                quarter = perf.get("quarter", "")
                rating = perf.get("rating", "")
                if quarter and rating:
                    quarterly_ratings.append(f"{quarter}: {rating}")
            
            # 주요 성취 추출
            key_achievements = annual_report.get("keyAchievements", [])
            
            # 강점 및 개선점 추출
            overall_assessment = annual_report.get("overall_assessment", {})
            strengths = overall_assessment.get("strengths", [])
            improvement_areas = overall_assessment.get("improvement_areas", [])
            
            print(f"✅ 사용자 {user_id}의 weekly_evaluation_results 요약 데이터 조회 완료")
            
            return {
                "quarterly_ratings": quarterly_ratings,
                "key_achievements": key_achievements,
                "strengths": strengths,
                "improvement_areas": improvement_areas
            }
            
        except Exception as e:
            print(f"❌ weekly_evaluation_results 요약 데이터 조회 실패 (user: {user_id}): {e}")
            return {}
    
    def aggregate_peer_keywords(self, quarterly_keyword_data: List[Dict]) -> Dict:
        """연간 동료 피드백 키워드 집계"""
        positive_counter = Counter()
        negative_counter = Counter()
        
        for quarter_data in quarterly_keyword_data:
            keywords = quarter_data.get("keywords", {})
            
            # 긍정 키워드 집계
            positive_keywords = keywords.get("positive", [])
            for keyword_data in positive_keywords:
                if isinstance(keyword_data, dict):
                    keyword = keyword_data.get("keyword", "")
                    count = keyword_data.get("count", 1)
                    positive_counter[keyword] += count
                else:
                    positive_counter[keyword_data] += 1
            
            # 부정 키워드 집계
            negative_keywords = keywords.get("negative", [])
            for keyword_data in negative_keywords:
                if isinstance(keyword_data, dict):
                    keyword = keyword_data.get("keyword", "")
                    count = keyword_data.get("count", 1)
                    negative_counter[keyword] += count
                else:
                    negative_counter[keyword_data] += 1
        
        # 상위 5개 추출
        top_positive = positive_counter.most_common(5)
        top_negative = negative_counter.most_common(5)
        
        return {
            "positive": [f"{kw}({count}회)" for kw, count in top_positive],
            "negative": [f"{kw}({count}회)" for kw, count in top_negative]
        }
    
    def collect_user_annual_data(self, user_id: int, year: int) -> Dict:
        """사용자의 연간 모든 평가 데이터 수집 (개선된 버전)"""
        try:
            # 1. 주간 업무 요약 데이터 수집
            weekly_summaries = self.get_quarterly_data_from_collection(
                "weekly_combination_results", user_id, year, "weekly_summary_text"
            )
            
            # 2. 정성 평가 데이터 수집
            qualitative_evaluations = self.get_quarterly_data_from_collection(
                "qualitative_evaluation_results", user_id, year, "evaluation_text"
            )
            
            # 3. 동료 평가 피드백 데이터 수집
            peer_feedbacks = self.get_quarterly_data_from_collection(
                "peer_evaluation_results", user_id, year, "feedback"
            )
            
            # 4. 정량 데이터 (주간 점수) 수집
            weekly_scores = self.get_quarterly_data_from_collection(
                "weekly_combination_results", user_id, year, "weekly_score"
            )
            
            # 5. 동료 피드백 키워드 수집
            peer_keyword_data = self.get_quarterly_data_from_collection(
                "peer_evaluation_results", user_id, year, "keyword_summary"
            )
            
            # 6. 🔥 weekly_evaluation_results 연간 요약 데이터 수집
            weekly_evaluation_summary = self.get_weekly_evaluation_summary(user_id, year)
            
            # 동료 키워드 집계
            peer_keywords_aggregated = self.aggregate_peer_keywords(peer_keyword_data)
            
            total_data_count = len(weekly_summaries) + len(qualitative_evaluations) + len(peer_feedbacks)
            
            print(f"✅ 사용자 {user_id} 데이터 수집 완료: 주간요약 {len(weekly_summaries)}개, 정성평가 {len(qualitative_evaluations)}개, 동료피드백 {len(peer_feedbacks)}개, 주간점수 {len(weekly_scores)}개")
            
            return {
                "weekly_summaries": weekly_summaries,
                "qualitative_evaluations": qualitative_evaluations,
                "peer_feedbacks": peer_feedbacks,
                "weekly_scores": weekly_scores,
                "peer_keywords_aggregated": peer_keywords_aggregated,
                "weekly_evaluation_summary": weekly_evaluation_summary,  # 🔥 새로 추가
                "total_data_count": total_data_count
            }
            
        except Exception as e:
            print(f"❌ 사용자 {user_id} 연간 데이터 수집 실패: {e}")
            return {
                "weekly_summaries": [],
                "qualitative_evaluations": [],
                "peer_feedbacks": [],
                "weekly_scores": [],
                "peer_keywords_aggregated": {"positive": [], "negative": []},
                "weekly_evaluation_summary": {},
                "total_data_count": 0
            }
    
    def generate_growth_advice(self, user_name: str, annual_data: Dict, year: int) -> str:
        """AI 기반 성장 방향 제언 생성"""
        try:
            # 데이터를 텍스트로 포맷팅
            weekly_text = "\n".join(annual_data["weekly_summaries"]) if annual_data["weekly_summaries"] else "주간 업무 요약 데이터 없음"
            qualitative_text = "\n".join(annual_data["qualitative_evaluations"]) if annual_data["qualitative_evaluations"] else "정성 평가 데이터 없음"
            peer_text = "\n".join(annual_data["peer_feedbacks"]) if annual_data["peer_feedbacks"] else "동료 평가 피드백 없음"
            
            # AI 성장 방향 제언 생성
            if self.use_legacy_chain:
                growth_advice = self.growth_advice_chain.run(
                    user_name=user_name,
                    weekly_summaries=weekly_text,
                    qualitative_evaluations=qualitative_text,
                    peer_feedbacks=peer_text,
                    year=year
                )
            else:
                growth_advice = self.growth_advice_chain.invoke({
                    "user_name": user_name,
                    "weekly_summaries": weekly_text,
                    "qualitative_evaluations": qualitative_text,
                    "peer_feedbacks": peer_text,
                    "year": year
                })
            
            return growth_advice.strip()
            
        except Exception as e:
            print(f"❌ AI 성장 방향 제언 생성 실패: {e}")
            return f"{user_name}님을 위한 맞춤형 성장 방향을 제언드립니다."
    
    def generate_comprehensive_comment(self, user_name: str, annual_data: Dict, year: int) -> str:
        """🔥 개선된 AI 기반 종합 Comment 생성 (weekly_evaluation_results 활용)"""
        try:
            # 정량 데이터 포맷팅
            weekly_scores_text = ""
            if annual_data["weekly_scores"]:
                scores_list = [f"{data['quarter']}분기: {data['score']}점" for data in annual_data["weekly_scores"]]
                weekly_scores_text = ", ".join(scores_list)
            else:
                weekly_scores_text = "정량 데이터 없음"
            
            # 🔥 weekly_evaluation_results 데이터 활용
            weekly_summary = annual_data["weekly_evaluation_summary"]
            
            # 분기별 등급 포맷팅
            quarterly_ratings_text = ", ".join(weekly_summary.get("quarterly_ratings", [])) if weekly_summary.get("quarterly_ratings") else "등급 데이터 없음"
            
            # 주요 성취 포맷팅
            key_achievements_text = "\n".join([f"- {achievement}" for achievement in weekly_summary.get("key_achievements", [])]) if weekly_summary.get("key_achievements") else "주요 성취 데이터 없음"
            
            # 강점 및 개선점 포맷팅
            strengths_text = ", ".join(weekly_summary.get("strengths", [])) if weekly_summary.get("strengths") else "강점 데이터 없음"
            improvement_areas_text = ", ".join(weekly_summary.get("improvement_areas", [])) if weekly_summary.get("improvement_areas") else "개선점 데이터 없음"
            
            # 정성 평가 데이터 포맷팅
            qualitative_text = "\n".join(annual_data["qualitative_evaluations"]) if annual_data["qualitative_evaluations"] else "정성 평가 데이터 없음"
            
            # 동료 키워드 포맷팅
            peer_keywords = annual_data["peer_keywords_aggregated"]
            positive_keywords = ", ".join(peer_keywords["positive"]) if peer_keywords["positive"] else "없음"
            negative_keywords = ", ".join(peer_keywords["negative"]) if peer_keywords["negative"] else "없음"
            peer_keywords_text = f"긍정: {positive_keywords} / 부정: {negative_keywords}"
            
            # AI 종합 Comment 생성
            if self.use_legacy_chain:
                comprehensive_comment = self.comprehensive_comment_chain.run(
                    user_name=user_name,
                    weekly_scores=weekly_scores_text,
                    quarterly_ratings=quarterly_ratings_text,
                    key_achievements=key_achievements_text,
                    strengths=strengths_text,
                    improvement_areas=improvement_areas_text,
                    qualitative_evaluations=qualitative_text,
                    peer_keywords=peer_keywords_text,
                    year=year
                )
            else:
                comprehensive_comment = self.comprehensive_comment_chain.invoke({
                    "user_name": user_name,
                    "weekly_scores": weekly_scores_text,
                    "quarterly_ratings": quarterly_ratings_text,
                    "key_achievements": key_achievements_text,
                    "strengths": strengths_text,
                    "improvement_areas": improvement_areas_text,
                    "qualitative_evaluations": qualitative_text,
                    "peer_keywords": peer_keywords_text,
                    "year": year
                })
            
            return comprehensive_comment.strip()
            
        except Exception as e:
            print(f"❌ AI 종합 Comment 생성 실패: {e}")
            return f"{user_name}님의 {year}년 성과에 대한 종합 평가입니다."
    
    def save_final_review_to_mongodb(self, user_data: Dict, year: int) -> bool:
        """최종 성과 리뷰를 MongoDB final_performance_reviews 컬렉션에 저장"""
        try:
            if not self.mongo_client:
                if not self.connect_mongodb():
                    return False
            
            db = self.mongo_client[self.mongo_db_name]
            collection = db["final_performance_reviews"]
            
            # 기존 연간 문서 찾기
            existing_doc = collection.find_one({
                "type": "personal-annual",
                "evaluated_year": year
            })
            
            if existing_doc:
                # 기존 문서에 사용자 데이터 추가
                collection.update_one(
                    {
                        "type": "personal-annual",
                        "evaluated_year": year
                    },
                    {
                        "$push": {"users": user_data},
                        "$set": {"updated_at": datetime.now()},
                        "$inc": {"user_count": 1}
                    }
                )
                print(f"✅ 기존 최종리뷰 문서에 사용자 ID {user_data['user_id']} 추가 완료")
            else:
                # 새로운 연간 문서 생성
                annual_document = {
                    "type": "personal-annual",
                    "evaluated_year": year,
                    "user_count": 1,
                    "users": [user_data],
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                }
                
                result = collection.insert_one(annual_document)
                print(f"✅ 새로운 최종리뷰 문서 생성 및 사용자 ID {user_data['user_id']} 추가 완료 - Document ID: {result.inserted_id}")
            
            return True
            
        except Exception as e:
            print(f"❌ MongoDB 최종리뷰 저장 실패 (사용자 ID: {user_data.get('user_id', 'unknown')}): {e}")
            return False
    
    def process_user_final_review(self, user_id: int, user_name: str, year: int) -> Dict:
        """개별 사용자의 최종 성과 리뷰 처리 (개선된 버전)"""
        try:
            # 1. 연간 모든 평가 데이터 수집 (weekly_evaluation_results 포함)
            annual_data = self.collect_user_annual_data(user_id, year)
            
            if annual_data["total_data_count"] == 0:
                return {
                    "success": False,
                    "message": "평가 데이터가 없습니다.",
                    "data": None
                }
            
            # 2. AI 기반 성장 방향 제언 생성
            growth_advice = self.generate_growth_advice(user_name, annual_data, year)
            
            # 3. 🔥 개선된 AI 기반 종합 Comment 생성
            comprehensive_comment = self.generate_comprehensive_comment(user_name, annual_data, year)
            
            # 4. 결과 구성
            result_data = {
                "user_id": user_id,
                "user_name": user_name,
                "year": year,
                "data_sources": {
                    "weekly_summaries_count": len(annual_data["weekly_summaries"]),
                    "qualitative_evaluations_count": len(annual_data["qualitative_evaluations"]),
                    "peer_feedbacks_count": len(annual_data["peer_feedbacks"]),
                    "weekly_scores_count": len(annual_data["weekly_scores"]),
                    "weekly_evaluation_summary_available": bool(annual_data["weekly_evaluation_summary"]),
                    "total_data_points": annual_data["total_data_count"]
                },
                "growth_advice": growth_advice,
                "comprehensive_comment": comprehensive_comment,
                "quarterly_scores": annual_data["weekly_scores"],
                "peer_keywords_summary": annual_data["peer_keywords_aggregated"],
                "weekly_evaluation_summary": annual_data["weekly_evaluation_summary"],  # 🔥 새로 추가
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return {
                "success": True,
                "data": result_data
            }
            
        except Exception as e:
            print(f"❌ 사용자 {user_id} 최종 성과 리뷰 처리 실패: {e}")
            return {
                "success": False,
                "message": f"처리 중 오류: {str(e)}",
                "data": None
            }
    
    def process_all_users_final_review(self, year: int) -> List[Dict]:
        """모든 사용자의 최종 성과 리뷰 처리"""
        # 1. 모든 사용자 조회
        users = self.get_all_users()
        if not users:
            print("❌ 사용자 데이터가 없습니다.")
            return []
        
        results = []
        successful_count = 0
        failed_count = 0
        
        print(f"\n=== {year}년 최종 성과 리뷰 생성 시작 (v2) ===")
        print(f"처리할 사용자 수: {len(users)}명")
        print(f"AI 모델: GPT-4o (Temperature: 0)")
        print(f"🔥 개선사항: weekly_evaluation_results 데이터 연동")
        print(f"생성 내용: 성장 방향 제언 + 종합 Comment (강화)")
        print("=" * 60)
        
        for i, user in enumerate(users, 1):
            user_id = user['id']
            user_name = user['name']
            
            # 진행률 표시
            if i % 5 == 0 or i == len(users) or i == 1:
                print(f"처리 진행률: {i}/{len(users)} ({i/len(users)*100:.1f}%)")
            
            # 개별 사용자 처리
            result = self.process_user_final_review(user_id, user_name, year)
            results.append(result)
            
            if result["success"]:
                # MongoDB에 저장
                save_success = self.save_final_review_to_mongodb(result["data"], year)
                
                if save_success:
                    successful_count += 1
                    data_count = result["data"]["data_sources"]["total_data_points"]
                    scores_count = result["data"]["data_sources"]["weekly_scores_count"]
                    weekly_summary_available = result["data"]["data_sources"]["weekly_evaluation_summary_available"]
                    weekly_indicator = "✅" if weekly_summary_available else "❌"
                    print(f"✓ User {user_id} ({user_name}): {data_count}개 데이터, {scores_count}개 점수, Weekly요약 {weekly_indicator} → 최종리뷰 완료")
                else:
                    failed_count += 1
                    print(f"✗ User {user_id} ({user_name}): 리뷰 생성 성공, MongoDB 저장 실패")
            else:
                failed_count += 1
                print(f"✗ User {user_id} ({user_name}): {result['message']}")
        
        print(f"\n=== {year}년 최종 성과 리뷰 완료 (v2) ===")
        print(f"성공: {successful_count}명")
        print(f"실패: {failed_count}명")
        print(f"저장 위치: {self.mongo_db_name}.final_performance_reviews")
        print(f"문서 타입: type='personal-annual', evaluated_year={year}")
        print(f"🔥 개선사항: weekly_evaluation_results 연동으로 더욱 풍부한 종합 Comment")
        
        return results
    
    def close(self):
        """연결 종료"""
        if self.mongo_client:
            self.mongo_client.close()
            print("MongoDB 연결 종료")

def main():
    print("🚀 최종 성과 리뷰 생성 시스템 v2 시작")
    print("=" * 60)
    
    # 생성기 초기화
    generator = FinalPerformanceReviewGenerator()
    
    # MongoDB 연결 테스트
    if not generator.connect_mongodb():
        print("❌ MongoDB 연결 실패. 프로그램을 종료합니다.")
        return
    
    # 2024년 최종 성과 리뷰 생성
    evaluation_year = 2024
    
    print(f"\n🎯 {evaluation_year}년 최종 성과 리뷰 생성 (v2)")
    print(f"생성 내용:")
    print(f"  1. 성장 방향 제언:")
    print(f"     - weekly_combination_results: weekly_summary_text")
    print(f"     - qualitative_evaluation_results: evaluation_text") 
    print(f"     - peer_evaluation_results: feedback")
    print(f"  2. 종합 Comment (🔥 강화):")
    print(f"     - weekly_combination_results: weekly_score (정량 데이터)")
    print(f"     - weekly_evaluation_results: quarterly_ratings, key_achievements, strengths, improvement_areas")
    print(f"     - qualitative_evaluation_results: evaluation_text (정성 평가)")
    print(f"     - peer_evaluation_results: keyword_summary (동료 피드백)")
    print(f"  3. MongoDB final_performance_reviews 컬렉션에 저장")
    print(f"     - type: 'personal-annual'")
    print(f"     - evaluated_year: {evaluation_year}")
    
    # 전체 사용자 처리
    results = generator.process_all_users_final_review(evaluation_year)
    
    # 통계 출력
    successful_results = [r for r in results if r["success"]]
    
    if successful_results:
        total_data_points = sum([r["data"]["data_sources"]["total_data_points"] for r in successful_results])
        total_scores = sum([r["data"]["data_sources"]["weekly_scores_count"] for r in successful_results])
        weekly_summary_count = sum([1 for r in successful_results if r["data"]["data_sources"]["weekly_evaluation_summary_available"]])
        avg_data_points = total_data_points / len(successful_results)
        
        print(f"\n📊 처리 통계:")
        print(f"  - 총 분석된 데이터 포인트: {total_data_points}개")
        print(f"  - 총 정량 점수 데이터: {total_scores}개")
        print(f"  - weekly_evaluation_results 연동 성공: {weekly_summary_count}명")
        print(f"  - 사용자당 평균 데이터: {avg_data_points:.1f}개")
        print(f"  - AI 모델: GPT-4o (Temperature: 0)")
        print(f"  - 🔥 개선된 종합 Comment: 분기별 등급, 주요 성취, 강점/개선점 모두 활용")
    
    print(f"\n🎉 최종 성과 리뷰 생성 시스템 v2 완료!")
    print(f"📄 결과 확인: MongoDB > {generator.mongo_db_name} > final_performance_reviews 컬렉션")
    print(f"📋 문서 구조:")
    print(f"   - type: 'personal-annual', evaluated_year: {evaluation_year}")
    print(f"   - 각 사용자별 성장 방향 제언 + 강화된 종합 Comment 포함")
    print(f"   - 🔥 weekly_evaluation_results 데이터로 더욱 정확하고 풍부한 총평")
    print(f"   - 정량/정성/동료/연간요약 평가 데이터 모두 활용")
    print(f"💾 저장 방식: 연도별 단일 문서에 모든 사용자 데이터 집적")
    
    # 연결 종료
    generator.close()
    
    return results

if __name__ == "__main__":
    main()