import os
import json
import mysql.connector
import numpy as np
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
    """연간 동료평가 요약 생성기 (패턴 분석 강화)"""
    
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
        
        # 개선된 프롬프트 템플릿 설정
        self.summary_prompt = PromptTemplate(
            input_variables=[
                "user_name", "department", "job_title",
                "top_positive_keywords", "top_negative_keywords", 
                "total_quarters", "avg_score",
                "quarterly_scores", "score_trend_analysis", "pattern_insights"
            ],
            template="""
당신은 HR 전문가입니다. 1년간의 동료평가 결과와 분기별 성장 패턴을 바탕으로 연간 종합 요약을 작성해주세요.

=== 기본 정보 ===
평가 대상: {user_name}님 ({department} / {job_title})
평가 기간: 2024년 전체 (총 {total_quarters}개 분기)
연간 평균 점수: {avg_score}점

=== 연간 성장 패턴 ===
분기별 점수 변화: {quarterly_scores}
전체 트렌드: {score_trend_analysis}
성장 패턴 특성: {pattern_insights}

=== 키워드 분석 ===
주요 강점 영역: {top_positive_keywords}
개선 필요 영역: {top_negative_keywords}

다음과 같은 순서로 1문장씩 연간 종합 요약을 작성해주세요:

1. **1년간 일관되게 나타난 주요 강점 분석** - 첫 번째 문장
2. **1년간 드러난 점수 변화 추세** - 두 번째 문장  
3. **지속적으로 개선이 필요한 영역 파악** - 세 번째 문장
4. **내년도 성장 방향 제언** - 네 번째 문장

요구사항:
- 정확히 4문장으로 구성하세요 (각 항목당 1문장)
- 분기별 점수 변화 패턴을 두 번째 문장에 자연스럽게 반영하세요
- 따뜻하고 격려적인 톤을 유지하세요
- 구체적인 키워드나 수치를 직접 언급하지 말고 자연스럽게 표현하세요

예시 (4문장 구조):
"{user_name}님은 1년 내내 뛰어난 협업 능력과 책임감 있는 업무 수행으로 팀에 지속적으로 기여해주셨습니다. 특히 연초 대비 하반기로 갈수록 동료들과의 소통 능력과 업무 완성도가 눈에 띄게 향상되는 성장 곡선을 보여주셨습니다. 다만 때로는 세부적인 부분에서의 꼼꼼함과 일정 관리 측면에서 조금 더 신경 써주시면 더욱 완성도 높은 성과를 만들어낼 수 있을 것입니다. 내년에는 현재의 우수한 협업 역량을 바탕으로 팀 내 멘토링이나 프로젝트 리딩 역할로 한 단계 더 성장하실 것을 기대합니다."
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
    
    def get_all_users_with_info(self) -> List[Dict]:
        """MariaDB에서 사용자 정보 조회 (부서, 직급 포함)"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            # 사용자 정보와 조직 정보 조인
            query = """
            SELECT 
                u.id, u.name,
                o.name as department,
                j.name as job_title
            FROM users u
            LEFT JOIN organizations o ON u.organization_id = o.id
            LEFT JOIN jobs j ON u.job_id = j.id
            ORDER BY u.id
            """
            cursor.execute(query)
            users = cursor.fetchall()
            conn.close()
            
            # 누락된 정보 기본값 설정
            for user in users:
                user['department'] = user['department'] or '미분류'
                user['job_title'] = user['job_title'] or '직원'
            
            print(f"✅ 총 {len(users)}명의 사용자 정보 조회 완료")
            return users
        except Exception as e:
            print(f"❌ 사용자 정보 조회 실패: {e}")
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
    
    def analyze_annual_pattern(self, quarterly_data: List[Dict]) -> Dict:
        """연간 성장 패턴 분석"""
        if not quarterly_data:
            return {
                "quarterly_scores_text": "데이터 없음",
                "trend_analysis": "평가 데이터 부족",
                "pattern_insights": "분석 불가"
            }
        
        scores = [data["score"] for data in quarterly_data if data["score"] > 0]
        
        if len(scores) < 2:
            return {
                "quarterly_scores_text": "평가 데이터 부족",
                "trend_analysis": "분석을 위한 충분한 데이터가 없음",
                "pattern_insights": "추가 평가 필요"
            }
        
        # 분기별 점수 텍스트 생성
        quarter_texts = []
        for data in quarterly_data:
            quarter_texts.append(f"{data['quarter']}분기: {data['score']:.1f}점")
        quarterly_scores_text = " → ".join(quarter_texts)
        
        # 트렌드 분석
        score_change = scores[-1] - scores[0]
        if score_change > 0.3:
            trend = f"상승 추세 ({score_change:.1f}점 증가)"
        elif score_change < -0.3:
            trend = f"하락 추세 ({abs(score_change):.1f}점 감소)"
        else:
            trend = "안정적 유지"
        
        # 패턴 분류
        std_dev = np.std(scores)
        max_score = max(scores)
        min_score = min(scores)
        peak_quarter = quarterly_data[scores.index(max_score)]["quarter"]
        
        if std_dev < 0.2:
            pattern = "안정형"
            pattern_detail = "일관된 성과 유지"
        elif score_change > 0.2:
            if peak_quarter >= 3:
                pattern = "후반기 성장형"
                pattern_detail = "하반기 집중적 향상"
            else:
                pattern = "지속 성장형"
                pattern_detail = "꾸준한 상승 곡선"
        elif max_score - min_score > 0.5:
            pattern = "변동형"
            pattern_detail = "분기별 기복 존재"
        else:
            pattern = "균형형"
            pattern_detail = "전반적 안정감"
        
        # 성장 인사이트 생성
        insights = f"{pattern} - {pattern_detail}"
        if peak_quarter:
            insights += f", {peak_quarter}분기 최고 성과"
        
        return {
            "quarterly_scores_text": quarterly_scores_text,
            "trend_analysis": trend,
            "pattern_insights": insights,
            "consistency": "높음" if std_dev < 0.3 else "보통",
            "peak_quarter": peak_quarter,
            "pattern_type": pattern
        }
    
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
    
    def generate_annual_summary(self, user_info: Dict, quarterly_data: List[Dict], 
                              keyword_stats: Dict, pattern_analysis: Dict) -> str:
        """연간 요약문 생성 (패턴 분석 포함)"""
        # 평균 점수 계산
        scores = [data["score"] for data in quarterly_data if data["score"] > 0]
        avg_score = sum(scores) / len(scores) if scores else 0
        
        # 키워드 텍스트 생성
        top_positive_text = ", ".join([f"{item['keyword']}" for item in keyword_stats["top_positive"][:3]])
        top_negative_text = ", ".join([f"{item['keyword']}" for item in keyword_stats["top_negative"][:3]])
        
        if not top_positive_text:
            top_positive_text = "긍정적 평가 다수"
        if not top_negative_text:
            top_negative_text = "특별한 개선점 없음"
        
        # AI 요약 생성
        try:
            prompt_data = {
                "user_name": user_info['name'],
                "department": user_info['department'],
                "job_title": user_info['job_title'],
                "top_positive_keywords": top_positive_text,
                "top_negative_keywords": top_negative_text,
                "total_quarters": len(quarterly_data),
                "avg_score": round(avg_score, 2),
                "quarterly_scores": pattern_analysis.get("quarterly_scores_text", ""),
                "score_trend_analysis": pattern_analysis.get("trend_analysis", ""),
                "pattern_insights": pattern_analysis.get("pattern_insights", "")
            }
            
            if self.use_legacy_chain:
                summary = self.summary_chain.run(**prompt_data)
            else:
                summary = self.summary_chain.invoke(prompt_data)
            
            return summary.strip()
        except Exception as e:
            print(f"❌ AI 요약 생성 실패: {e}")
            return f"{user_info['name']}님의 1년간 동료평가 결과를 종합한 요약입니다."
    
    def save_annual_summary_to_mongodb(self, user_data: Dict, year: int) -> bool:
        """연간 요약을 MongoDB에 저장 (동일 사용자 덮어쓰기)"""
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
                # 기존 사용자 데이터가 있는지 확인
                existing_users = existing_doc.get("users", [])
                user_exists = any(user.get("user_id") == user_data["user_id"] for user in existing_users)
                
                if user_exists:
                    # 기존 사용자 데이터 덮어쓰기
                    collection.update_one(
                        {
                            "type": "personal-annual",
                            "evaluated_year": year,
                            "users.user_id": user_data["user_id"]
                        },
                        {
                            "$set": {
                                "users.$": user_data,
                                "updated_at": datetime.now()
                            }
                        }
                    )
                    print(f"✅ 사용자 ID {user_data['user_id']} 기존 데이터 덮어쓰기 완료")
                else:
                    # 새로운 사용자 데이터 추가
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
                    print(f"✅ 기존 연간 문서에 사용자 ID {user_data['user_id']} 신규 추가 완료")
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
    
    def process_user_annual_summary(self, user_info: Dict, year: int) -> Dict:
        """개별 사용자의 연간 요약 처리 (패턴 분석 포함)"""
        try:
            user_id = user_info['id']
            
            # 1. 분기별 데이터 조회
            quarterly_data = self.get_user_quarterly_data(user_id, year)
            
            if not quarterly_data:
                return {
                    "success": False,
                    "message": "분기별 데이터가 없습니다.",
                    "data": None
                }
            
            # 2. 연간 패턴 분석
            pattern_analysis = self.analyze_annual_pattern(quarterly_data)
            
            # 3. 연간 키워드 집계
            keyword_stats = self.aggregate_annual_keywords(quarterly_data)
            
            # 4. 연간 요약문 생성 (패턴 분석 포함)
            annual_summary = self.generate_annual_summary(user_info, quarterly_data, keyword_stats, pattern_analysis)
            
            # 5. 평균 점수 계산
            scores = [data["score"] for data in quarterly_data if data["score"] > 0]
            avg_score = sum(scores) / len(scores) if scores else 0
            
            # 6. 결과 구성
            result_data = {
                "user_id": user_id,
                "user_name": user_info['name'],
                "department": user_info['department'],
                "job_title": user_info['job_title'],
                "year": year,
                "quarters_evaluated": len(quarterly_data),
                "annual_average_score": round(avg_score, 2),
                "top_positive_keywords": keyword_stats["top_positive"],
                "top_negative_keywords": keyword_stats["top_negative"],
                "total_positive_mentions": keyword_stats["total_positive_count"],
                "total_negative_mentions": keyword_stats["total_negative_count"],
                "pattern_analysis": pattern_analysis,
                "annual_summary": annual_summary,
                "quarterly_scores": [{"quarter": data["quarter"], "score": data["score"]} for data in quarterly_data],
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return {
                "success": True,
                "data": result_data
            }
            
        except Exception as e:
            print(f"❌ 사용자 {user_info.get('id', 'unknown')} 연간 요약 처리 실패: {e}")
            return {
                "success": False,
                "message": f"처리 중 오류: {str(e)}",
                "data": None
            }
    
    def process_all_users_annual_summary(self, year: int) -> List[Dict]:
        """모든 사용자의 연간 요약 처리 (패턴 분석 포함)"""
        # 1. 모든 사용자 정보 조회
        users = self.get_all_users_with_info()
        if not users:
            print("❌ 사용자 데이터가 없습니다.")
            return []
        
        results = []
        successful_count = 0
        failed_count = 0
        
        print(f"\n=== {year}년 연간 동료평가 요약 생성 시작 (패턴 분석 포함) ===")
        print(f"처리할 사용자 수: {len(users)}명")
        print("=" * 70)
        
        for i, user_info in enumerate(users, 1):
            user_id = user_info['id']
            user_name = user_info['name']
            department = user_info['department']
            
            # 진행률 표시
            if i % 10 == 0 or i == len(users):
                print(f"처리 진행률: {i}/{len(users)} ({i/len(users)*100:.1f}%)")
            
            # 개별 사용자 처리
            result = self.process_user_annual_summary(user_info, year)
            results.append(result)
            
            if result["success"]:
                # MongoDB에 저장
                save_success = self.save_annual_summary_to_mongodb(result["data"], year)
                
                if save_success:
                    successful_count += 1
                    data = result["data"]
                    avg_score = data["annual_average_score"]
                    quarters = data["quarters_evaluated"]
                    pattern = data["pattern_analysis"]["pattern_insights"]
                    print(f"✓ User {user_id} ({user_name}/{department}): {quarters}분기 평균 {avg_score:.2f}점, {pattern} → 연간 요약 완료")
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
        print(f"주요 개선사항: 4문장 구조, 패턴 분석, 부서/직급 정보 포함")
        
        return results
    
    def close(self):
        """연결 종료"""
        if self.mongo_client:
            self.mongo_client.close()
            print("MongoDB 연결 종료")

    def process_specific_users_annual_summary(self, user_ids: List[int], year: int) -> List[Dict]:
        """특정 사용자들의 연간 요약 처리"""
        # 1. 특정 사용자 정보 조회
        users = self.get_specific_users_with_info(user_ids)
        if not users:
            print("❌ 지정된 사용자 데이터가 없습니다.")
            return []
        
        results = []
        successful_count = 0
        failed_count = 0
        
        print(f"\n=== {year}년 특정 사용자 연간 동료평가 요약 생성 (패턴 분석 포함) ===")
        print(f"대상 사용자 ID: {user_ids}")
        print(f"처리할 사용자 수: {len(users)}명")
        print("=" * 70)
        
        for i, user_info in enumerate(users, 1):
            user_id = user_info['id']
            user_name = user_info['name']
            department = user_info['department']
            
            print(f"[{i}/{len(users)}] 처리 중: User {user_id} ({user_name}/{department})")
            
            # 개별 사용자 처리
            result = self.process_user_annual_summary(user_info, year)
            results.append(result)
            
            if result["success"]:
                # MongoDB에 저장
                save_success = self.save_annual_summary_to_mongodb(result["data"], year)
                
                if save_success:
                    successful_count += 1
                    data = result["data"]
                    avg_score = data["annual_average_score"]
                    quarters = data["quarters_evaluated"]
                    pattern = data["pattern_analysis"]["pattern_insights"]
                    print(f"✓ User {user_id} ({user_name}/{department}): {quarters}분기 평균 {avg_score:.2f}점, {pattern} → 연간 요약 완료")
                else:
                    failed_count += 1
                    print(f"✗ User {user_id} ({user_name}): 요약 생성 성공, MongoDB 저장 실패")
            else:
                failed_count += 1
                print(f"✗ User {user_id} ({user_name}): {result['message']}")
        
        print(f"\n=== {year}년 특정 사용자 연간 동료평가 요약 완료 ===")
        print(f"성공: {successful_count}명")
        print(f"실패: {failed_count}명")
        print(f"저장 위치: {self.mongo_db_name}.peer_evaluation_results")
        print(f"문서 타입: type='personal-annual', evaluated_year={year}")
        
        return results
    
    def get_specific_users_with_info(self, user_ids: List[int]) -> List[Dict]:
        """특정 사용자들의 정보 조회 (부서, 직급 포함)"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            # IN절을 사용하여 특정 사용자들만 조회
            placeholders = ",".join(["%s"] * len(user_ids))
            query = f"""
            SELECT 
                u.id, u.name,
                o.name as department,
                j.name as job_title
            FROM users u
            LEFT JOIN organizations o ON u.organization_id = o.id
            LEFT JOIN jobs j ON u.job_id = j.id
            WHERE u.id IN ({placeholders})
            ORDER BY u.id
            """
            cursor.execute(query, user_ids)
            users = cursor.fetchall()
            conn.close()
            
            # 누락된 정보 기본값 설정
            for user in users:
                user['department'] = user['department'] or '미분류'
                user['job_title'] = user['job_title'] or '직원'
            
            print(f"✅ 지정된 사용자 {len(users)}명의 정보 조회 완료")
            
            # 조회되지 않은 사용자 ID 확인
            found_ids = [user['id'] for user in users]
            missing_ids = [uid for uid in user_ids if uid not in found_ids]
            if missing_ids:
                print(f"⚠️ 조회되지 않은 사용자 ID: {missing_ids}")
            
            return users
        except Exception as e:
            print(f"❌ 특정 사용자 정보 조회 실패: {e}")
            return []

def main():
    print("🚀 연간 동료평가 요약 생성 시스템 시작 (특정 사용자 대상)")
    print("=" * 70)
    
    # 생성기 초기화
    generator = AnnualPeerEvaluationSummaryGenerator()
    
    # MongoDB 연결 테스트
    if not generator.connect_mongodb():
        print("❌ MongoDB 연결 실패. 프로그램을 종료합니다.")
        return
    
    # 2024년 연간 요약 생성
    evaluation_year = 2024
    target_user_ids = [76, 79, 91]  # 처리할 특정 사용자 ID
    
    print(f"\n🎯 {evaluation_year}년 연간 동료평가 요약 생성")
    print(f"대상 사용자: {target_user_ids}")
    print(f"작업 내용:")
    print(f"  1. 지정된 사용자의 1~4분기 동료평가 데이터 수집")
    print(f"  2. 연간 성장 패턴 분석 (트렌드, 일관성, 피크 분기)")
    print(f"  3. 연간 키워드 집계 (긍정/부정 상위 5개씩)")
    print(f"  4. AI 기반 4문장 구조 연간 요약문 생성")
    print(f"     - 1문장: 일관된 주요 강점")
    print(f"     - 2문장: 점수 변화 추세")
    print(f"     - 3문장: 개선 필요 영역")
    print(f"     - 4문장: 내년도 성장 방향")
    print(f"  5. MongoDB peer_evaluation_results 컬렉션에 저장")
    print(f"     - type: 'personal-annual'")
    print(f"     - evaluated_year: {evaluation_year}")
    
    # 특정 사용자들 처리
    results = generator.process_specific_users_annual_summary(target_user_ids, evaluation_year)
    
    # 통계 출력
    successful_results = [r for r in results if r["success"]]
    
    if successful_results:
        avg_scores = [r["data"]["annual_average_score"] for r in successful_results]
        overall_avg = sum(avg_scores) / len(avg_scores)
        
        # 패턴 분포 분석
        pattern_types = [r["data"]["pattern_analysis"]["pattern_type"] for r in successful_results]
        pattern_counter = Counter(pattern_types)
        
        print(f"\n📊 통계 요약:")
        print(f"  - 대상 사용자 평균 점수: {overall_avg:.2f}점")
        if len(avg_scores) > 1:
            print(f"  - 최고 점수: {max(avg_scores):.2f}점")
            print(f"  - 최저 점수: {min(avg_scores):.2f}점")
        print(f"  - 성장 패턴 분포:")
        for pattern, count in pattern_counter.most_common():
            print(f"    * {pattern}: {count}명")
            
        # 개별 사용자 요약 출력
        print(f"\n📋 개별 사용자 요약:")
        for result in successful_results:
            data = result["data"]
            print(f"  - User {data['user_id']} ({data['user_name']}): {data['annual_average_score']:.2f}점, {data['pattern_analysis']['pattern_type']}")
    
    # 연결 종료
    generator.close()
    
    print(f"\n🎉 {evaluation_year}년 특정 사용자 연간 동료평가 요약 생성 완료!")
    print(f"처리 대상: {target_user_ids}")
    print(f"개선사항: 패턴 분석, 4문장 구조, 부서/직급 정보 반영")
    
    return results

if __name__ == "__main__":
    main()