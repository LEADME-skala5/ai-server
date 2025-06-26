import os
import json
import mysql.connector
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv
from pymongo import MongoClient

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

class AnnualGrowthAdvisor:
    """연간 성장 방향 제언 생성기"""
    
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
        
        # OpenAI GPT-4o-turbo 설정
        openai_api_key = os.getenv("OPENAI_API_KEY")
        self.llm = ChatOpenAI(
            model="gpt-4o",  # GPT-4o-turbo 모델
            api_key=openai_api_key,
            temperature=0,
            max_tokens=3000
        )
        
        # 성장 방향 제언 프롬프트 템플릿
        self.growth_prompt = PromptTemplate(
            input_variables=["user_name", "weekly_summaries", "qualitative_evaluations", "peer_feedbacks", "year"],
            template="""
Let's think step by step

당신은 경험이 풍부한 HR 전문가입니다. 
한 직원의 {year}년 전체 평가 데이터를 종합적으로 분석하여 성장 방향을 제언해주세요.

평가 대상: {user_name}님
분석 기간: {year}년 전체 (1~4분기)

【주간 업무 요약】{weekly_summaries}
【정성 평가 결과】{qualitative_evaluations}  
【동료 평가 피드백】{peer_feedbacks}

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
"""
        )
        
        # LangChain 체인 설정
        try:
            self.growth_chain = self.growth_prompt | self.llm | StrOutputParser()
            self.use_legacy_chain = False
        except:
            self.growth_chain = LLMChain(llm=self.llm, prompt=self.growth_prompt)
            self.use_legacy_chain = True
        
        print(f"📋 연간 성장 방향 제언 시스템 초기화 완료")
        print(f"MariaDB: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")
        print(f"MongoDB: {self.mongo_host}:{self.mongo_port}/{self.mongo_db_name}")
        print(f"AI 모델: GPT-4o")
    
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
    
    def get_quarterly_data_from_collection(self, collection_name: str, user_id: int, year: int, field_name: str) -> List[str]:
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
                            field_value = user_data.get(field_name, "")
                            if field_value and field_value.strip():
                                quarterly_data.append(f"[{quarter}분기] {field_value.strip()}")
                            break
            
            return quarterly_data
            
        except Exception as e:
            print(f"❌ {collection_name} 컬렉션에서 사용자 {user_id} 데이터 조회 실패: {e}")
            return []
    
    def collect_user_annual_data(self, user_id: int, year: int) -> Dict:
        """사용자의 연간 모든 평가 데이터 수집"""
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
            
            total_data_count = len(weekly_summaries) + len(qualitative_evaluations) + len(peer_feedbacks)
            
            print(f"✅ 사용자 {user_id} 데이터 수집 완료: 주간요약 {len(weekly_summaries)}개, 정성평가 {len(qualitative_evaluations)}개, 동료피드백 {len(peer_feedbacks)}개 (총 {total_data_count}개)")
            
            return {
                "weekly_summaries": weekly_summaries,
                "qualitative_evaluations": qualitative_evaluations,
                "peer_feedbacks": peer_feedbacks,
                "total_data_count": total_data_count
            }
            
        except Exception as e:
            print(f"❌ 사용자 {user_id} 연간 데이터 수집 실패: {e}")
            return {
                "weekly_summaries": [],
                "qualitative_evaluations": [],
                "peer_feedbacks": [],
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
                growth_advice = self.growth_chain.run(
                    user_name=user_name,
                    weekly_summaries=weekly_text,
                    qualitative_evaluations=qualitative_text,
                    peer_feedbacks=peer_text,
                    year=year
                )
            else:
                growth_advice = self.growth_chain.invoke({
                    "user_name": user_name,
                    "weekly_summaries": weekly_text,
                    "qualitative_evaluations": qualitative_text,
                    "peer_feedbacks": peer_text,
                    "year": year
                })
            
            return growth_advice.strip()
            
        except Exception as e:
            print(f"❌ AI 성장 방향 제언 생성 실패: {e}")
            return f"{user_name}님을 위한 맞춤형 성장 방향을 제언드립니다. 수집된 평가 데이터를 바탕으로 지속적인 성장을 위한 구체적인 계획을 수립해보시기 바랍니다."
    
    def save_growth_advice_to_mongodb(self, user_data: Dict, year: int) -> bool:
        """성장 방향 제언을 MongoDB annual_growth_suggestion 컬렉션에 저장"""
        try:
            if not self.mongo_client:
                if not self.connect_mongodb():
                    return False
            
            db = self.mongo_client[self.mongo_db_name]
            collection = db["annual_growth_suggestion"]  # 지정된 컬렉션명
            
            # 기존 연간 문서 찾기 (evaluated_year 기준)
            existing_doc = collection.find_one({
                "evaluated_year": year
            })
            
            if existing_doc:
                # 기존 문서에 사용자 데이터 추가
                collection.update_one(
                    {"evaluated_year": year},
                    {
                        "$push": {"users": user_data},
                        "$set": {"updated_at": datetime.now()},
                        "$inc": {"user_count": 1}
                    }
                )
                print(f"✅ 기존 성장방향 문서에 사용자 ID {user_data['user_id']} 추가 완료")
            else:
                # 새로운 연간 문서 생성
                annual_document = {
                    "evaluated_year": year,
                    "user_count": 1,
                    "users": [user_data],
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                }
                
                result = collection.insert_one(annual_document)
                print(f"✅ 새로운 성장방향 문서 생성 및 사용자 ID {user_data['user_id']} 추가 완료 - Document ID: {result.inserted_id}")
            
            return True
            
        except Exception as e:
            print(f"❌ MongoDB 성장방향 저장 실패 (사용자 ID: {user_data.get('user_id', 'unknown')}): {e}")
            return False
    
    def process_user_growth_advice(self, user_id: int, user_name: str, year: int) -> Dict:
        """개별 사용자의 성장 방향 제언 처리"""
        try:
            # 1. 연간 모든 평가 데이터 수집
            annual_data = self.collect_user_annual_data(user_id, year)
            
            if annual_data["total_data_count"] == 0:
                return {
                    "success": False,
                    "message": "평가 데이터가 없습니다.",
                    "data": None
                }
            
            # 2. AI 기반 성장 방향 제언 생성
            growth_advice = self.generate_growth_advice(user_name, annual_data, year)
            
            # 3. 결과 구성
            result_data = {
                "user_id": user_id,
                "user_name": user_name,
                "year": year,
                "data_sources": {
                    "weekly_summaries_count": len(annual_data["weekly_summaries"]),
                    "qualitative_evaluations_count": len(annual_data["qualitative_evaluations"]),
                    "peer_feedbacks_count": len(annual_data["peer_feedbacks"]),
                    "total_data_points": annual_data["total_data_count"]
                },
                "growth_advice": growth_advice,
                "source_data": {
                    "weekly_summaries": annual_data["weekly_summaries"],
                    "qualitative_evaluations": annual_data["qualitative_evaluations"],
                    "peer_feedbacks": annual_data["peer_feedbacks"]
                },
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return {
                "success": True,
                "data": result_data
            }
            
        except Exception as e:
            print(f"❌ 사용자 {user_id} 성장 방향 제언 처리 실패: {e}")
            return {
                "success": False,
                "message": f"처리 중 오류: {str(e)}",
                "data": None
            }
    
    def process_all_users_growth_advice(self, year: int) -> List[Dict]:
        """모든 사용자의 성장 방향 제언 처리"""
        # 1. 모든 사용자 조회
        users = self.get_all_users()
        if not users:
            print("❌ 사용자 데이터가 없습니다.")
            return []
        
        results = []
        successful_count = 0
        failed_count = 0
        
        print(f"\n=== {year}년 연간 성장 방향 제언 생성 시작 ===")
        print(f"처리할 사용자 수: {len(users)}명")
        print(f"AI 모델: GPT-4o")
        print(f"데이터 소스: 주간요약 + 정성평가 + 동료피드백 (최대 12개 문장)")
        print("=" * 60)
        
        for i, user in enumerate(users, 1):
            user_id = user['id']
            user_name = user['name']
            
            # 진행률 표시
            if i % 5 == 0 or i == len(users) or i == 1:
                print(f"처리 진행률: {i}/{len(users)} ({i/len(users)*100:.1f}%)")
            
            # 개별 사용자 처리
            result = self.process_user_growth_advice(user_id, user_name, year)
            results.append(result)
            
            if result["success"]:
                # MongoDB에 저장
                save_success = self.save_growth_advice_to_mongodb(result["data"], year)
                
                if save_success:
                    successful_count += 1
                    data_count = result["data"]["data_sources"]["total_data_points"]
                    print(f"✓ User {user_id} ({user_name}): {data_count}개 데이터 기반 성장방향 제언 완료")
                else:
                    failed_count += 1
                    print(f"✗ User {user_id} ({user_name}): 제언 생성 성공, MongoDB 저장 실패")
            else:
                failed_count += 1
                print(f"✗ User {user_id} ({user_name}): {result['message']}")
        
        print(f"\n=== {year}년 연간 성장 방향 제언 완료 ===")
        print(f"성공: {successful_count}명")
        print(f"실패: {failed_count}명")
        print(f"저장 위치: {self.mongo_db_name}.annual_growth_suggestion")
        print(f"문서 구조: evaluated_year={year} 기준으로 사용자 데이터 집적")
        print(f"AI 모델: GPT-4o 기반 맞춤형 성장 방향 제언")
        
        return results
    
    def close(self):
        """연결 종료"""
        if self.mongo_client:
            self.mongo_client.close()
            print("MongoDB 연결 종료")

def main():
    print("🚀 연간 성장 방향 제언 생성 시스템 시작")
    print("=" * 60)
    
    # 생성기 초기화
    advisor = AnnualGrowthAdvisor()
    
    # MongoDB 연결 테스트
    if not advisor.connect_mongodb():
        print("❌ MongoDB 연결 실패. 프로그램을 종료합니다.")
        return
    
    # 2024년 성장 방향 제언 생성
    evaluation_year = 2024
    
    print(f"\n🎯 {evaluation_year}년 연간 성장 방향 제언 생성")
    print(f"작업 내용:")
    print(f"  1. 각 사용자의 1~4분기 평가 데이터 수집")
    print(f"     - weekly_combination_results: weekly_summary_text")
    print(f"     - qualitative_evaluation_results: evaluation_text") 
    print(f"     - peer_evaluation_results: feedback")
    print(f"  2. GPT-4o 기반 종합 분석 및 성장 방향 제언")
    print(f"  3. MongoDB annual_growth_suggestion 컬렉션에 저장")
    print(f"     - evaluated_year: {evaluation_year} 기준으로 문서 생성")
    print(f"     - 동일 연도의 모든 사용자 데이터가 하나의 문서에 집적")
    
    # 전체 사용자 처리
    results = advisor.process_all_users_growth_advice(evaluation_year)
    
    # 통계 출력
    successful_results = [r for r in results if r["success"]]
    
    if successful_results:
        total_data_points = sum([r["data"]["data_sources"]["total_data_points"] for r in successful_results])
        avg_data_points = total_data_points / len(successful_results)
        
        print(f"\n📊 처리 통계:")
        print(f"  - 총 분석된 데이터 포인트: {total_data_points}개")
        print(f"  - 사용자당 평균 데이터: {avg_data_points:.1f}개")
        print(f"  - AI 모델: GPT-4o (고품질 성장 방향 제언)")
    
    print(f"\n🎉 연간 성장 방향 제언 시스템 완료!")
    print(f"📄 결과 확인: MongoDB > {advisor.mongo_db_name} > annual_growth_suggestion 컬렉션")
    print(f"📋 문서 구조: evaluated_year={evaluation_year} 문서 내 사용자별 맞춤형 성장 로드맵")
    print(f"💾 저장 방식: 연도별 단일 문서에 모든 사용자 데이터 집적")
    
    # 연결 종료
    advisor.close()
    
    return results

if __name__ == "__main__":
    main()