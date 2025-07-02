import os
import json
import mysql.connector
from datetime import datetime
from typing import Dict, List, Optional
from collections import Counter
from dotenv import load_dotenv
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
    from langchain_core.runnables import RunnablePassthrough
    from langchain_core.output_parsers import StrOutputParser

# .env 파일 로드
load_dotenv()

class AnnualPeerEvaluationSummaryGenerator:
    """연간 동료평가 요약 생성기"""
    
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
        
        # OpenAI 설정
        openai_api_key = os.getenv("OPENAI_API_KEY")
        self.llm = OpenAI(api_key=openai_api_key, temperature=0.7, max_tokens=2000)
        
        # 프롬프트 템플릿 설정
        self.summary_prompt = PromptTemplate(
            input_variables=["user_name", "top_positive_keywords", "top_negative_keywords", "total_quarters", "avg_score"],
            template="""
당신은 HR 전문가입니다. 1년간의 동료평가 결과를 바탕으로 연간 종합 요약을 작성해주세요.

평가 대상: {user_name}님
평가 기간: 2024년 전체 (총 {total_quarters}개 분기)
연간 평균 점수: {avg_score}점

1년간 가장 많이 받은 긍정적 평가 (상위 5개):
{top_positive_keywords}

1년간 가장 많이 받은 개선점 (상위 5개):
{top_negative_keywords}

다음과 같은 형식으로 연간 종합 요약을 작성해주세요:
1. 1년간 일관되게 나타난 주요 강점 분석
2. 지속적으로 개선이 필요한 영역 파악
3. 내년도 성장 방향 제언

자연스럽고 따뜻한 톤으로 4-5줄 정도로 작성해주세요.
키워드나 빈도수를 직접 언급하지 말고, 자연스러운 문장으로 표현해주세요.

예시 톤:
"{user_name}님은 1년 내내 뛰어난 협업 능력과 적극적인 자세로 팀에 기여해주셨습니다. 특히 책임감 있는 업무 수행과 동료들과의 원활한 소통 능력이 지속적으로 높은 평가를 받았습니다. 다만, 때로는 세심한 부분에서의 꼼꼼함을 더해주시면 더욱 완성도 높은 성과를 만들어낼 수 있을 것입니다. 내년에도 현재의 긍정적인 에너지를 유지하시면서, 전문성 향상에도 지속적으로 관심을 가져주시기 바랍니다."
"""
        )
        
        # LangChain 체인 설정
        try:
            self.summary_chain = self.summary_prompt | self.llm | StrOutputParser()
            self.use_legacy_chain = False
        except:
            self.summary_chain = LLMChain(llm=self.llm, prompt=self.summary_prompt)
            self.use_legacy_chain = True
        
        print(f"📋 설정 로드 완료")
        print(f"MariaDB: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")
        print(f"MongoDB: {self.mongo_host}:{self.mongo_port}/{self.mongo_db_name}")
    
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
    
    def get_user_quarterly_data(self, user_id: int, year: int) -> List[Dict]:
        """MongoDB에서 해당 사용자의 분기별 동료평가 데이터 조회"""
        try:
            if not self.mongo_client:
                if not self.connect_mongodb():
                    return []
            
            db = self.mongo_client[self.mongo_db_name]
            collection = db["peer_evaluation_results"]
            
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
                            quarterly_data.append({
                                "quarter": quarter,
                                "score": user_data.get("peer_evaluation_score", 0),
                                "keyword_summary": user_data.get("keyword_summary", {})
                            })
                            break
            
            print(f"✅ 사용자 {user_id}의 분기별 데이터 {len(quarterly_data)}개 조회 완료")
            return quarterly_data
            
        except Exception as e:
            print(f"❌ 사용자 {user_id} 분기별 데이터 조회 실패: {e}")
            return []
    
    def aggregate_annual_keywords(self, quarterly_data: List[Dict]) -> Dict:
        """1년간 키워드 집계"""
        positive_counter = Counter()
        negative_counter = Counter()
        
        for quarter_data in quarterly_data:
            keyword_summary = quarter_data.get("keyword_summary", {})
            
            # 긍정 키워드 집계
            positive_keywords = keyword_summary.get("positive", [])
            for keyword_data in positive_keywords:
                if isinstance(keyword_data, dict):
                    keyword = keyword_data.get("keyword", "")
                    count = keyword_data.get("count", 1)
                    positive_counter[keyword] += count
                else:
                    # 기존 형식 호환성
                    positive_counter[keyword_data] += 1
            
            # 부정 키워드 집계
            negative_keywords = keyword_summary.get("negative", [])
            for keyword_data in negative_keywords:
                if isinstance(keyword_data, dict):
                    keyword = keyword_data.get("keyword", "")
                    count = keyword_data.get("count", 1)
                    negative_counter[keyword] += count
                else:
                    # 기존 형식 호환성
                    negative_counter[keyword_data] += 1
        
        # 상위 5개 추출
        top_positive = positive_counter.most_common(5)
        top_negative = negative_counter.most_common(5)
        
        return {
            "top_positive": [{"keyword": kw, "count": count} for kw, count in top_positive],
            "top_negative": [{"keyword": kw, "count": count} for kw, count in top_negative],
            "total_positive_count": sum(positive_counter.values()),
            "total_negative_count": sum(negative_counter.values())
        }
    
    def generate_annual_summary(self, user_name: str, quarterly_data: List[Dict], keyword_stats: Dict) -> str:
        """연간 요약문 생성"""
        # 평균 점수 계산
        scores = [data["score"] for data in quarterly_data if data["score"] > 0]
        avg_score = sum(scores) / len(scores) if scores else 0
        
        # 상위 키워드 텍스트 생성
        top_positive_text = ", ".join([f"{item['keyword']}({item['count']}회)" for item in keyword_stats["top_positive"]])
        top_negative_text = ", ".join([f"{item['keyword']}({item['count']}회)" for item in keyword_stats["top_negative"]])
        
        if not top_positive_text:
            top_positive_text = "없음"
        if not top_negative_text:
            top_negative_text = "없음"
        
        # AI 요약 생성
        try:
            if self.use_legacy_chain:
                summary = self.summary_chain.run(
                    user_name=user_name,
                    top_positive_keywords=top_positive_text,
                    top_negative_keywords=top_negative_text,
                    total_quarters=len(quarterly_data),
                    avg_score=round(avg_score, 2)
                )
            else:
                summary = self.summary_chain.invoke({
                    "user_name": user_name,
                    "top_positive_keywords": top_positive_text,
                    "top_negative_keywords": top_negative_text,
                    "total_quarters": len(quarterly_data),
                    "avg_score": round(avg_score, 2)
                })
            
            return summary.strip()
        except Exception as e:
            print(f"❌ AI 요약 생성 실패: {e}")
            return f"{user_name}님의 1년간 동료평가 결과를 종합한 요약입니다."
    
    def save_annual_summary_to_mongodb(self, user_data: Dict, year: int) -> bool:
        """연간 요약을 MongoDB에 저장"""
        try:
            if not self.mongo_client:
                if not self.connect_mongodb():
                    return False
            
            db = self.mongo_client[self.mongo_db_name]
            collection = db["peer_evaluation_results"]
            
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
                print(f"✅ 기존 연간 문서에 사용자 ID {user_data['user_id']} 추가 완료")
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
                print(f"✅ 새로운 연간 문서 생성 및 사용자 ID {user_data['user_id']} 추가 완료 - Document ID: {result.inserted_id}")
            
            return True
            
        except Exception as e:
            print(f"❌ MongoDB 연간 요약 저장 실패 (사용자 ID: {user_data.get('user_id', 'unknown')}): {e}")
            return False
    
    def process_user_annual_summary(self, user_id: int, user_name: str, year: int) -> Dict:
        """개별 사용자의 연간 요약 처리"""
        try:
            # 1. 분기별 데이터 조회
            quarterly_data = self.get_user_quarterly_data(user_id, year)
            
            if not quarterly_data:
                return {
                    "success": False,
                    "message": "분기별 데이터가 없습니다.",
                    "data": None
                }
            
            # 2. 연간 키워드 집계
            keyword_stats = self.aggregate_annual_keywords(quarterly_data)
            
            # 3. 연간 요약문 생성
            annual_summary = self.generate_annual_summary(user_name, quarterly_data, keyword_stats)
            
            # 4. 평균 점수 계산
            scores = [data["score"] for data in quarterly_data if data["score"] > 0]
            avg_score = sum(scores) / len(scores) if scores else 0
            
            # 5. 결과 구성
            result_data = {
                "user_id": user_id,
                "user_name": user_name,
                "year": year,
                "quarters_evaluated": len(quarterly_data),
                "annual_average_score": round(avg_score, 2),
                "top_positive_keywords": keyword_stats["top_positive"],
                "top_negative_keywords": keyword_stats["top_negative"],
                "total_positive_mentions": keyword_stats["total_positive_count"],
                "total_negative_mentions": keyword_stats["total_negative_count"],
                "annual_summary": annual_summary,
                "quarterly_scores": [{"quarter": data["quarter"], "score": data["score"]} for data in quarterly_data],
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return {
                "success": True,
                "data": result_data
            }
            
        except Exception as e:
            print(f"❌ 사용자 {user_id} 연간 요약 처리 실패: {e}")
            return {
                "success": False,
                "message": f"처리 중 오류: {str(e)}",
                "data": None
            }
    
    def process_all_users_annual_summary(self, year: int) -> List[Dict]:
        """모든 사용자의 연간 요약 처리"""
        # 1. 모든 사용자 조회
        users = self.get_all_users()
        if not users:
            print("❌ 사용자 데이터가 없습니다.")
            return []
        
        results = []
        successful_count = 0
        failed_count = 0
        
        print(f"\n=== {year}년 연간 동료평가 요약 생성 시작 ===")
        print(f"처리할 사용자 수: {len(users)}명")
        print("=" * 50)
        
        for i, user in enumerate(users, 1):
            user_id = user['id']
            user_name = user['name']
            
            # 진행률 표시
            if i % 10 == 0 or i == len(users):
                print(f"처리 진행률: {i}/{len(users)} ({i/len(users)*100:.1f}%)")
            
            # 개별 사용자 처리
            result = self.process_user_annual_summary(user_id, user_name, year)
            results.append(result)
            
            if result["success"]:
                # MongoDB에 저장
                save_success = self.save_annual_summary_to_mongodb(result["data"], year)
                
                if save_success:
                    successful_count += 1
                    avg_score = result["data"]["annual_average_score"]
                    quarters = result["data"]["quarters_evaluated"]
                    print(f"✓ User {user_id} ({user_name}): {quarters}분기 평균 {avg_score:.2f}점 → 연간 요약 완료")
                else:
                    failed_count += 1
                    print(f"✗ User {user_id} ({user_name}): 요약 생성 성공, MongoDB 저장 실패")
            else:
                failed_count += 1
                print(f"✗ User {user_id} ({user_name}): {result['message']}")
        
        print(f"\n=== {year}년 연간 동료평가 요약 완료 ===")
        print(f"성공: {successful_count}명")
        print(f"실패: {failed_count}명")
        print(f"저장 위치: {self.mongo_db_name}.peer_evaluation_results")
        print(f"문서 타입: type='personal-annual', evaluated_year={year}")
        
        return results
    
    def close(self):
        """연결 종료"""
        if self.mongo_client:
            self.mongo_client.close()
            print("MongoDB 연결 종료")

def main():
    print("🚀 연간 동료평가 요약 생성 시스템 시작")
    print("=" * 60)
    
    # 생성기 초기화
    generator = AnnualPeerEvaluationSummaryGenerator()
    
    # MongoDB 연결 테스트
    if not generator.connect_mongodb():
        print("❌ MongoDB 연결 실패. 프로그램을 종료합니다.")
        return
    
    # 2024년 연간 요약 생성
    evaluation_year = 2024
    
    print(f"\n🎯 {evaluation_year}년 연간 동료평가 요약 생성")
    print(f"작업 내용:")
    print(f"  1. 각 사용자의 1~4분기 동료평가 데이터 수집")
    print(f"  2. 연간 키워드 집계 (긍정/부정 상위 5개씩)")
    print(f"  3. AI 기반 연간 종합 요약문 생성")
    print(f"  4. MongoDB peer_evaluation_results 컬렉션에 저장")
    print(f"     - type: 'personal-annual'")
    print(f"     - evaluated_year: {evaluation_year}")
    
    # 전체 사용자 처리
    results = generator.process_all_users_annual_summary(evaluation_year)
    
    # 통계 출력
    successful_results = [r for r in results if r["success"]]
    
    if successful_results:
        avg_scores = [r["data"]["annual_average_score"] for r in successful_results]
        overall_avg = sum(avg_scores) / len(avg_scores)
        
        print(f"\n📊 통계 요약:")
        print(f"  - 전체 평균 점수: {overall_avg:.2f}점")
        print(f"  - 최고 점수: {max(avg_scores):.2f}점")
        print(f"  - 최저 점수: {min(avg_scores):.2f}점")
    
    # 연결 종료
    generator.close()
    
    print(f"\n🎉 {evaluation_year}년 연간 동료평가 요약 생성 완료!")
    
    return results

if __name__ == "__main__":
    main()